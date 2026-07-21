"""Daily production ingestion for the persisted WhoScored catalog.

Catalog access happens only in runtime tasks.  DAG parsing therefore remains
safe while Trino is restarting or before the additive catalog migration has
run, and there is no static fallback to the historical six competitions.
"""

# ruff: noqa: E402 -- the trust anchor must run before every non-built-in import

from __future__ import annotations

import sys as _whoscored_bootstrap_sys

_whoscored_source = __file__
if not _whoscored_source.startswith("/"):
    raise RuntimeError("WhoScored entrypoint requires an absolute source path")
_whoscored_production = _whoscored_source.startswith("/opt/airflow/")
_whoscored_root = (
    "/opt/airflow"
    if _whoscored_production
    else _whoscored_source.rsplit("/dags/", 1)[0]
)
if _whoscored_production:
    if (
        getattr(_whoscored_bootstrap_sys, "_whoscored_runtime_startup_schema", None)
        != 2
    ):
        raise RuntimeError("image-baked WhoScored startup anchor is required")
elif (
    getattr(_whoscored_bootstrap_sys, "_whoscored_runtime_startup_root", None)
    != _whoscored_root
):
    _whoscored_anchor_path = (
        _whoscored_root + "/docker/images/airflow/whoscored_runtime_startup.py"
    )
    _whoscored_anchor_globals = {
        "__builtins__": __builtins__,
        "sys": _whoscored_bootstrap_sys,
        "_WHOSCORED_RUNTIME_ROOT": _whoscored_root,
        "_WHOSCORED_REQUIRE_FULL_ATTESTATION": False,
    }
    with open(_whoscored_anchor_path, "rb") as _whoscored_anchor_handle:
        _whoscored_anchor_source = _whoscored_anchor_handle.read()
    exec(
        compile(_whoscored_anchor_source, _whoscored_anchor_path, "exec"),
        _whoscored_anchor_globals,
    )
_WHOSCORED_RUNTIME_CONTRACT = _whoscored_bootstrap_sys._load_whoscored_runtime_contract(
    _whoscored_root
)

import hashlib
import json
import math
import os
import re
import secrets
import shlex
import stat
from datetime import date, datetime, timedelta
from pathlib import Path
from typing import Any, Dict, Mapping, Optional, Sequence
from urllib.parse import parse_qsl, urlencode, urlsplit, urlunsplit

from airflow import DAG
from airflow.exceptions import AirflowException
from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator

from dags.scripts.whoscored_identity import stable_safe_token
from dags.scripts.whoscored_proxy_runtime import (
    PaidRuntime,
    WhoScoredProxyRuntimeError,
    paid_campaign_gateway_call,
    paid_alert_source_guard_command,
    resolve_paid_runtime,
    stable_profiles_work_item,
    stable_scope_work_item,
    validate_transport_alert_delivery,
)

from utils.config import DAG_TAGS, SCHEDULES
from utils.default_args import SCRAPER_ARGS


RUN_ROOT = "/opt/airflow/logs/whoscored_runs"
_RUN_DIR_TEMPLATE = (
    RUN_ROOT + "/{{ dag.dag_id | stable_safe_token }}/{{ run_id | stable_safe_token }}"
)
PAID_LEDGER_PATH = os.environ.get(
    "PROXY_FILTER_LEDGER_PATH",
    "/opt/airflow/logs/proxy_filter/paid_requests.jsonl",
)
MAX_LEDGER_EVENT_BYTES = 256 * 1024
MAX_TRAFFIC_XCOM_BYTES = 256 * 1024
TRAFFIC_RECONCILIATION_CONTRACT = "whoscored-paid-reconciliation-v2"
EXPECTED_FEEDS_PER_STAGE = 68
SCOPE_PARITY_TABLES = (
    "whoscored_schedule",
    "whoscored_match_incidents",
    "whoscored_match_bets",
    "whoscored_stage_standings",
    "whoscored_stage_forms",
    "whoscored_stage_streaks",
    "whoscored_stage_performance",
    "whoscored_team_stage_stats",
    "whoscored_player_stage_stats",
    "whoscored_referee_stage_stats",
)
PRODUCER_COMMIT_PATTERNS = {
    "scope": re.compile(r"^wss2-[0-9a-f]{64}$"),
    "match": re.compile(r"^ws2-v3-[0-9a-f]{64}$"),
    "match_not_available": re.compile(r"^wsna2-v3-[0-9a-f]{64}$"),
    "preview": re.compile(r"^wsp2-v3-[0-9a-f]{64}$"),
    "preview_not_available": re.compile(r"^wspna2-v3-[0-9a-f]{64}$"),
    "profile": re.compile(r"^wspr2-v3-[0-9a-f]{64}$"),
    "profile_not_available": re.compile(r"^wsprna2-[0-9a-f]{64}$"),
}
PRODUCER_ATTEMPT_KINDS = ("match", "preview", "profile")

# Two slots bound direct-only scope fan-out without serialising hundreds of
# competitions.  Match/scope commit locks protect idempotency across task
# processes; the paid route still has its independent hard lease limit of one.
DIRECT_POOL = os.environ.get("WHOSCORED_DIRECT_POOL", "whoscored_direct_pool")
# Scope integrity expands several Iceberg current views.  Keep only two such
# queries active so Trino workers do not hit their remote-task idle timeout.
DQ_POOL = os.environ.get("WHOSCORED_DQ_POOL", "whoscored_dq_pool")
DAILY_P95_HARD_LIMIT_HOURS = 4.0
COLD_DAGRUN_HARD_LIMIT_HOURS = 6.0

WHOSCORED_ARGS = {
    **{
        key: value
        for key, value in SCRAPER_ARGS.items()
        if key
        not in {
            "pool",
            "retries",
            "retry_delay",
            "execution_timeout",
            "on_failure_callback",
        }
    },
    "retries": 0,
    "execution_timeout": timedelta(minutes=20),
}

_TASK_ENV = {
    "PATH": "/usr/local/bin:/usr/bin:/bin:/home/airflow/.local/bin",
    "HOME": "/home/airflow",
    "WHOSCORED_SCHEMA_READY": "1",
    # Airflow's context environment does not guarantee a dedicated map-index
    # variable. Render the complete identity so every request is attributed to
    # the exact mapped task attempt in both local and durable ledgers.
    "AIRFLOW_CTX_DAG_ID": "{{ dag.dag_id }}",
    "AIRFLOW_CTX_DAG_RUN_ID": "{{ run_id }}",
    "AIRFLOW_CTX_TASK_ID": "{{ task.task_id }}",
    "AIRFLOW_CTX_TRY_NUMBER": "{{ ti.try_number }}",
    "AIRFLOW_CTX_MAP_INDEX": "{{ ti.map_index }}",
    "WHOSCORED_REQUEST_LEDGER_PATH": (
        _RUN_DIR_TEMPLATE
        + "/requests_{{ task.task_id | replace('.', '_') }}_"
        + "{{ ti.map_index }}_try{{ ti.try_number }}.jsonl"
    ),
}


def _safe_token(value: str) -> str:
    return stable_safe_token(value)


def _transport_runtime(
    context: Mapping[str, Any],
    *,
    task_id: Optional[str] = None,
    work_item_id: Optional[str] = None,
    missing_ok: bool = False,
) -> PaidRuntime:
    try:
        return resolve_paid_runtime(
            context,
            task_id=task_id,
            work_item_id=work_item_id,
            missing_ok=missing_ok,
        )
    except WhoScoredProxyRuntimeError as exc:
        raise AirflowException(str(exc)) from exc


def _bind_transport_allocation(
    transport: PaidRuntime,
    *,
    task_id: str,
    work_item_id: str,
    missing_ok: bool = False,
) -> PaidRuntime:
    try:
        return transport.for_allocation(
            task_id=task_id,
            work_item_id=work_item_id,
            missing_ok=missing_ok,
        )
    except WhoScoredProxyRuntimeError as exc:
        raise AirflowException(str(exc)) from exc


def _run_dir_from_context(context: Mapping[str, Any]) -> Path:
    dag = context.get("dag")
    dag_id = getattr(dag, "dag_id", None) or context.get("dag_id") or "unknown"
    run_id = context.get("run_id")
    if not run_id and context.get("dag_run") is not None:
        run_id = context["dag_run"].run_id
    return Path(RUN_ROOT) / _safe_token(str(dag_id)) / _safe_token(str(run_id))


def _scope_result_path(run_dir: Path, scope_spec: str) -> Path:
    digest = hashlib.sha256(scope_spec.encode("utf-8")).hexdigest()[:12]
    return run_dir / f"scope_{_safe_token(scope_spec)[:70]}_{digest}.json"


def _remove_directory_at(parent_fd: int, name: str, *, expected_dev: int) -> int:
    """Remove one directory tree without ever following a link or mount."""

    flags = os.O_RDONLY | os.O_DIRECTORY | os.O_NOFOLLOW
    directory_fd = os.open(name, flags, dir_fd=parent_fd)
    removed_bytes = 0
    try:
        directory_stat = os.fstat(directory_fd)
        if (
            not stat.S_ISDIR(directory_stat.st_mode)
            or directory_stat.st_dev != expected_dev
        ):
            raise OSError("stale run directory crosses a filesystem boundary")
        for entry in os.listdir(directory_fd):
            entry_stat = os.stat(
                entry,
                dir_fd=directory_fd,
                follow_symlinks=False,
            )
            if stat.S_ISDIR(entry_stat.st_mode):
                if entry_stat.st_dev != expected_dev:
                    raise OSError("stale run child crosses a filesystem boundary")
                removed_bytes += _remove_directory_at(
                    directory_fd,
                    entry,
                    expected_dev=expected_dev,
                )
                continue
            if stat.S_ISREG(entry_stat.st_mode):
                removed_bytes += entry_stat.st_size
            # Symlinks and other non-directories are removed as directory
            # entries. Their targets are never opened or inspected.
            os.unlink(entry, dir_fd=directory_fd)
    finally:
        os.close(directory_fd)
    os.rmdir(name, dir_fd=parent_fd)
    return removed_bytes


def cleanup_stale_run_directories(
    *,
    current_run_dir: Optional[Path] = None,
    now: Optional[datetime] = None,
) -> Dict[str, int]:
    """Bound failed/interrupted local staging independently of task success."""
    try:
        retention_days = int(os.environ.get("WHOSCORED_RUN_RETENTION_DAYS", "90"))
    except ValueError as exc:
        raise AirflowException("invalid WHOSCORED_RUN_RETENTION_DAYS") from exc
    if not 1 <= retention_days <= 3650:
        raise AirflowException("WHOSCORED_RUN_RETENTION_DAYS must be in 1..3650")
    root = Path(os.path.abspath(RUN_ROOT))
    try:
        root_stat = root.lstat()
    except FileNotFoundError:
        return {"removed_directories": 0, "removed_bytes": 0}
    if not stat.S_ISDIR(root_stat.st_mode) or stat.S_ISLNK(root_stat.st_mode):
        raise AirflowException("WhoScored run root must be a real directory")
    cutoff = (now or datetime.now()).timestamp() - retention_days * 86400
    current_parts: Optional[tuple[str, str]] = None
    if current_run_dir is not None:
        current = Path(os.path.abspath(current_run_dir))
        try:
            relative = current.relative_to(root)
        except ValueError as exc:
            raise AirflowException(
                "current WhoScored run directory is outside RUN_ROOT"
            ) from exc
        if len(relative.parts) != 2:
            raise AirflowException(
                "current WhoScored run directory must be RUN_ROOT/dag/run"
            )
        current_parts = (relative.parts[0], relative.parts[1])
    removed_directories = 0
    removed_bytes = 0
    root_fd = os.open(root, os.O_RDONLY | os.O_DIRECTORY | os.O_NOFOLLOW)
    try:
        for dag_name in sorted(os.listdir(root_fd)):
            try:
                dag_stat = os.stat(
                    dag_name,
                    dir_fd=root_fd,
                    follow_symlinks=False,
                )
            except FileNotFoundError:
                continue
            if stat.S_ISLNK(dag_stat.st_mode):
                raise AirflowException(
                    f"WhoScored run root contains a symlinked DAG entry: {dag_name}"
                )
            if not stat.S_ISDIR(dag_stat.st_mode):
                continue
            try:
                dag_fd = os.open(
                    dag_name,
                    os.O_RDONLY | os.O_DIRECTORY | os.O_NOFOLLOW,
                    dir_fd=root_fd,
                )
            except FileNotFoundError:
                continue
            try:
                if os.fstat(dag_fd).st_dev != root_stat.st_dev:
                    raise AirflowException(
                        f"WhoScored DAG run directory is a mount: {dag_name}"
                    )
                for run_name in sorted(os.listdir(dag_fd)):
                    try:
                        run_stat = os.stat(
                            run_name,
                            dir_fd=dag_fd,
                            follow_symlinks=False,
                        )
                    except FileNotFoundError:
                        continue
                    if stat.S_ISLNK(run_stat.st_mode):
                        raise AirflowException(
                            "WhoScored run root contains a symlinked run entry: "
                            f"{dag_name}/{run_name}"
                        )
                    if not stat.S_ISDIR(run_stat.st_mode):
                        continue
                    if current_parts == (dag_name, run_name):
                        continue
                    if run_stat.st_mtime >= cutoff:
                        continue
                    quarantine = f".cleanup-{secrets.token_hex(16)}"
                    os.rename(
                        run_name,
                        quarantine,
                        src_dir_fd=dag_fd,
                        dst_dir_fd=dag_fd,
                    )
                    removed_bytes += _remove_directory_at(
                        dag_fd,
                        quarantine,
                        expected_dev=root_stat.st_dev,
                    )
                    removed_directories += 1
            except FileNotFoundError:
                continue
            except OSError as exc:
                raise AirflowException(
                    f"cannot remove stale WhoScored runs for {dag_name}: {exc}"
                ) from exc
            finally:
                os.close(dag_fd)
    except OSError as exc:
        raise AirflowException(
            f"cannot inspect WhoScored run root {root}: {exc}"
        ) from exc
    finally:
        os.close(root_fd)
    return {
        "removed_directories": removed_directories,
        "removed_bytes": removed_bytes,
    }


def _scheduled_daily_durations_hours() -> list[float]:
    """Read terminal scheduled durations without gate survivorship bias.

    The first scheduled DagRun is the one permitted cold/full-history crawl
    and is excluded from the normal rolling SLO. Failed later DagRuns remain
    in the metric at their actual elapsed duration, so an SLO-gate failure is
    represented without manufacturing a new six-hour latency observation.
    """
    try:
        window = int(os.environ.get("WHOSCORED_DAILY_SLO_WINDOW", "30"))
    except ValueError as exc:
        raise AirflowException("invalid WHOSCORED_DAILY_SLO_WINDOW") from exc
    if not 5 <= window <= 365:
        raise AirflowException("WHOSCORED_DAILY_SLO_WINDOW must be in 5..365")

    from airflow.models.dagrun import DagRun
    from airflow.utils.session import create_session

    with create_session() as session:
        bootstrap_rows = (
            session.query(DagRun.run_id)
            .filter(
                DagRun.dag_id == "dag_ingest_whoscored",
                DagRun.run_id.like("scheduled__%"),
                DagRun.start_date.isnot(None),
            )
            .order_by(DagRun.start_date.asc())
            .limit(1)
            .all()
        )
        bootstrap_run_id = str(bootstrap_rows[0][0]) if bootstrap_rows else None
        rows = (
            session.query(
                DagRun.run_id,
                DagRun.state,
                DagRun.start_date,
                DagRun.end_date,
            )
            .filter(
                DagRun.dag_id == "dag_ingest_whoscored",
                DagRun.state.in_(("success", "failed")),
                DagRun.run_id.like("scheduled__%"),
                DagRun.start_date.isnot(None),
                DagRun.end_date.isnot(None),
            )
            .order_by(DagRun.end_date.desc())
            .limit(window)
            .all()
        )
    durations: list[float] = []
    for run_id, _state, start_date, end_date in rows:
        if str(run_id) == bootstrap_run_id:
            continue
        elapsed = max(0.0, (end_date - start_date).total_seconds() / 3600)
        durations.append(elapsed)
    return durations


def validate_whoscored_daily_slo(
    *,
    durations_hours: Optional[Sequence[float]] = None,
    **_context: Any,
) -> Dict[str, Any]:
    """Gate normal rolling p95 while warming up after the first cold crawl."""
    try:
        configured_limit = float(
            os.environ.get(
                "WHOSCORED_DAILY_P95_LIMIT_HOURS",
                str(DAILY_P95_HARD_LIMIT_HOURS),
            )
        )
        minimum_samples = int(os.environ.get("WHOSCORED_DAILY_SLO_MIN_SAMPLES", "20"))
        window = int(os.environ.get("WHOSCORED_DAILY_SLO_WINDOW", "30"))
    except ValueError as exc:
        raise AirflowException("invalid WhoScored daily SLO settings") from exc
    if (
        not math.isfinite(configured_limit)
        or not 0 < configured_limit <= DAILY_P95_HARD_LIMIT_HOURS
        or not 20 <= minimum_samples <= window <= 365
    ):
        raise AirflowException(
            "WhoScored daily SLO must be <=4h with 20..window warm-up samples"
        )
    raw = (
        list(durations_hours)
        if durations_hours is not None
        else _scheduled_daily_durations_hours()
    )
    if any(
        isinstance(value, bool)
        or not isinstance(value, (int, float))
        or not math.isfinite(float(value))
        or float(value) < 0
        for value in raw
    ):
        raise AirflowException("invalid duration in WhoScored daily SLO history")
    durations = sorted(float(value) for value in raw)
    if len(durations) < minimum_samples:
        return {
            "status": "warming_up",
            "samples": len(durations),
            "minimum_samples": minimum_samples,
            "p95_hours": None,
            "p95_limit_hours": configured_limit,
            "cold_hard_limit_hours": COLD_DAGRUN_HARD_LIMIT_HOURS,
        }
    rank = max(0, math.ceil(0.95 * len(durations)) - 1)
    p95_hours = durations[rank]
    summary = {
        "status": "success",
        "samples": len(durations),
        "minimum_samples": minimum_samples,
        "p95_hours": round(p95_hours, 6),
        "p95_limit_hours": configured_limit,
        "cold_hard_limit_hours": COLD_DAGRUN_HARD_LIMIT_HOURS,
    }
    if p95_hours > configured_limit:
        raise AirflowException(f"WhoScored daily rolling p95 SLO failed: {summary}")
    return summary


def _active_scope_specs() -> list[str]:
    # Lazy import keeps DAG parsing free of Trino/PyArrow dependencies.
    from dags.scripts.run_whoscored_scraper import resolve_daily_scope_specs

    scopes = resolve_daily_scope_specs()
    if not scopes:
        raise AirflowException("persisted WhoScored catalog has no active scopes")
    if len(scopes) != len(set(scopes)):
        raise AirflowException("persisted WhoScored catalog returned duplicate scopes")
    return sorted(scopes)


_CATALOG_IDENTITY_KEYS = {
    "schema_version",
    "catalog_batch_id",
    "catalog_payload_sha256",
    "catalog_raw_provenance_sha256",
    "technical_exclusion_audit_sha256",
    "catalog_as_of_date",
    "parent_catalog_batch_id",
    "parent_catalog_payload_sha256",
    "parent_catalog_raw_provenance_sha256",
}


def _catalog_identity(value: Mapping[str, Any], *, exact: bool) -> Dict[str, Any]:
    """Normalize the immutable identity emitted by one discovery attempt."""

    if not isinstance(value, Mapping) or (
        exact and set(value) != _CATALOG_IDENTITY_KEYS
    ):
        raise AirflowException("invalid WhoScored catalog identity schema")
    batch_id = str(value.get("catalog_batch_id") or "")
    if not re.fullmatch(r"[A-Za-z0-9_.-]{1,120}", batch_id):
        raise AirflowException("invalid WhoScored catalog identity batch id")
    result: Dict[str, Any] = {
        "schema_version": 1,
        "catalog_batch_id": batch_id,
    }
    if exact and value.get("schema_version") != 1:
        raise AirflowException("invalid WhoScored catalog identity version")
    for key in (
        "catalog_payload_sha256",
        "catalog_raw_provenance_sha256",
        "technical_exclusion_audit_sha256",
    ):
        digest = str(value.get(key) or "")
        if re.fullmatch(r"[0-9a-f]{64}", digest) is None:
            raise AirflowException(f"invalid WhoScored catalog identity {key}")
        result[key] = digest
    raw_as_of = value.get("catalog_as_of_date")
    if isinstance(raw_as_of, datetime):
        raise AirflowException("WhoScored catalog as-of identity must be a date")
    if isinstance(raw_as_of, date):
        as_of_date = raw_as_of.isoformat()
    else:
        as_of_date = str(raw_as_of or "")
    try:
        parsed_as_of = date.fromisoformat(as_of_date)
    except ValueError as exc:
        raise AirflowException("invalid WhoScored catalog as-of identity") from exc
    if parsed_as_of.isoformat() != as_of_date:
        raise AirflowException("non-canonical WhoScored catalog as-of identity")
    result["catalog_as_of_date"] = as_of_date

    parent_batch = value.get("parent_catalog_batch_id")
    parent_payload = value.get("parent_catalog_payload_sha256")
    parent_raw = value.get("parent_catalog_raw_provenance_sha256")
    if all(item is None for item in (parent_batch, parent_payload, parent_raw)):
        result.update(
            {
                "parent_catalog_batch_id": None,
                "parent_catalog_payload_sha256": None,
                "parent_catalog_raw_provenance_sha256": None,
            }
        )
    else:
        parent_batch = str(parent_batch or "")
        parent_payload = str(parent_payload or "")
        parent_raw = str(parent_raw or "")
        if (
            re.fullmatch(r"[A-Za-z0-9_.-]{1,120}", parent_batch) is None
            or re.fullmatch(r"[0-9a-f]{64}", parent_payload) is None
            or re.fullmatch(r"[0-9a-f]{64}", parent_raw) is None
        ):
            raise AirflowException("invalid WhoScored parent catalog identity")
        result.update(
            {
                "parent_catalog_batch_id": parent_batch,
                "parent_catalog_payload_sha256": parent_payload,
                "parent_catalog_raw_provenance_sha256": parent_raw,
            }
        )
    return result


def _logical_as_of_date(context: Mapping[str, Any]) -> str:
    logical_date = context.get("logical_date")
    if not isinstance(logical_date, datetime) or logical_date.tzinfo is None:
        raise AirflowException(
            "WhoScored discovery requires a timezone-aware logical_date"
        )
    return logical_date.date().isoformat()


def freeze_daily_scope_plan(
    *, validated_catalog: Mapping[str, Any], **context: Any
) -> Dict[str, Any]:
    """Bind one exact catalog generation for every task in this DagRun."""

    from dags.scripts import run_whoscored_scraper as runner
    from scrapers.whoscored.repository import catalog_payload_sha256

    if (
        not isinstance(validated_catalog, Mapping)
        or validated_catalog.get("status") != "success"
    ):
        raise AirflowException(
            "WhoScored catalog DQ did not return a validated identity"
        )
    identity = _catalog_identity(
        validated_catalog.get("catalog_identity"),
        exact=True,
    )
    repository = runner._new_repository()
    batch_id = identity["catalog_batch_id"]
    generation, catalog = repository.load_catalog_generation_snapshot(batch_id=batch_id)
    manifest_identity = {
        key: generation.get(key)
        for key in (
            "catalog_batch_id",
            "catalog_payload_sha256",
            "catalog_raw_provenance_sha256",
            "catalog_as_of_date",
            "parent_catalog_batch_id",
            "parent_catalog_payload_sha256",
            "parent_catalog_raw_provenance_sha256",
        )
    }
    expected_manifest_identity = {key: identity[key] for key in manifest_identity}
    if manifest_identity != expected_manifest_identity:
        raise AirflowException(
            "frozen WhoScored catalog manifest differs from validated discovery"
        )
    physical_payload_sha256 = catalog_payload_sha256(catalog.to_rows())
    if physical_payload_sha256 != identity["catalog_payload_sha256"]:
        raise AirflowException(
            "frozen WhoScored catalog payload differs from validated discovery"
        )
    selected = runner._select_catalog_snapshot_scopes(
        catalog,
        [],
        active_only=True,
    )
    catalog_scopes = sorted(scope.spec for scope, _runtime in selected)
    if not catalog_scopes or len(catalog_scopes) != len(set(catalog_scopes)):
        raise AirflowException("frozen WhoScored catalog has invalid active scopes")
    transport = _transport_runtime(context)
    if transport.is_paid:
        approval = transport.approval
        authority = approval.scheduled_authority if approval is not None else None
        if authority is None:
            raise AirflowException(
                "scheduled paid scope plan requires schema-v3 authority"
            )
        if (
            authority.catalog_batch_id != identity["parent_catalog_batch_id"]
            or authority.catalog_payload_sha256
            != identity["parent_catalog_payload_sha256"]
        ):
            raise AirflowException(
                "signed paid scope plan differs from the candidate catalog parent"
            )
        scopes = [item.scope for item in authority.scope_workloads]
        missing = sorted(set(scopes) - set(catalog_scopes))
        if missing:
            audit = _write_quarantine_disappearance_audit(
                catalog=catalog,
                missing_scopes=missing,
                signed_parent_batch_id=authority.catalog_batch_id,
                candidate_identity=identity,
                context=context,
            )
            raise AirflowException(
                "WHOSCORED_QUARANTINE_DISAPPEARANCE: signed scopes disappeared "
                f"or became ineligible: {missing}; audit_sha256="
                f"{audit['audit_sha256']}"
            )
        selected_by_scope = {
            scope.spec: (scope, runtime) for scope, runtime in selected
        }
        _validate_scheduled_scope_workloads(
            repository,
            [selected_by_scope[scope] for scope in scopes],
            authority.scope_workloads,
        )
        deferred = sorted(set(catalog_scopes) - set(scopes))
    else:
        scopes = catalog_scopes
        deferred = []
    scopes_sha256 = hashlib.sha256(
        ("\n".join(scopes) + "\n").encode("utf-8")
    ).hexdigest()
    result: Dict[str, Any] = {
        "schema_version": 2,
        **{key: value for key, value in identity.items() if key != "schema_version"},
        "active_scopes": scopes,
        "active_scope_count": len(scopes),
        "active_scopes_sha256": scopes_sha256,
    }
    if transport.is_paid:
        assert transport.approval is not None
        assert transport.approval.scheduled_authority is not None
        authority = transport.approval.scheduled_authority
        result.update(
            {
                "schema_version": 3,
                "catalog_active_scope_count": len(catalog_scopes),
                "deferred_scopes": deferred,
                "deferred_scope_count": len(deferred),
                "deferred_scopes_sha256": hashlib.sha256(
                    ("\n".join(deferred) + ("\n" if deferred else "")).encode("utf-8")
                ).hexdigest(),
                "cohort_id": authority.cohort_id,
                "cohort_sha256": authority.cohort_sha256,
                "workload_sha256": authority.workload_sha256,
                "scope_workloads": [
                    item.to_dict() for item in authority.scope_workloads
                ],
                "discovery_parent_target_count": (
                    authority.discovery_parent_target_count
                ),
                "discovery_expansion_headroom": (
                    authority.discovery_expansion_headroom
                ),
                "discovery_target_limit": authority.discovery_target_limit,
                "profile_target_count": authority.profile_target_count,
                "profile_targets_sha256": authority.profile_targets_sha256,
            }
        )
    return result


def _write_quarantine_disappearance_audit(
    *,
    catalog: Any,
    missing_scopes: Sequence[str],
    signed_parent_batch_id: str,
    candidate_identity: Mapping[str, Any],
    context: Mapping[str, Any],
) -> Dict[str, Any]:
    """Persist explicit evidence before the DAG-level failure alert fires."""

    from scrapers.whoscored.proxy_campaign import canonical_json_bytes

    quarantined_records = []
    quarantined_scopes: set[str] = set()
    missing_parts = {scope: tuple(scope.split("=", 1)) for scope in missing_scopes}
    for raw in getattr(catalog, "quarantined", ()):
        if not isinstance(raw, Mapping):
            continue
        record = {
            key: raw.get(key)
            for key in (
                "record_type",
                "competition_id",
                "season_id",
                "source_season_id",
                "stage_id",
                "eligibility",
                "classification_reason",
            )
        }
        matches = []
        for scope, (competition_id, season_id) in missing_parts.items():
            if str(raw.get("competition_id") or "") != competition_id:
                continue
            if (
                raw.get("record_type") == "competition"
                or str(raw.get("season_id") or "") == season_id
            ):
                matches.append(scope)
                quarantined_scopes.add(scope)
        if matches:
            quarantined_records.append({**record, "affected_scopes": sorted(matches)})
    quarantined_records.sort(
        key=lambda item: (
            str(item.get("record_type") or ""),
            str(item.get("competition_id") or ""),
            str(item.get("season_id") or ""),
            str(item.get("stage_id") or ""),
        )
    )
    body = {
        "schema_version": 1,
        "audit_type": "whoscored_quarantine_disappearance",
        "dag_id": str(_dag_id_from_context(context)),
        "run_id": str(
            context.get("run_id") or getattr(context.get("dag_run"), "run_id", "") or ""
        ),
        "signed_parent_catalog_batch_id": signed_parent_batch_id,
        "candidate_catalog_batch_id": candidate_identity["catalog_batch_id"],
        "candidate_catalog_payload_sha256": candidate_identity[
            "catalog_payload_sha256"
        ],
        "missing_scopes": sorted(missing_scopes),
        "quarantined_scopes": sorted(quarantined_scopes),
        "disappeared_scopes": sorted(set(missing_scopes) - quarantined_scopes),
        "quarantine_records": quarantined_records,
        "alert_route": "dag_level_failure_callback",
    }
    payload = canonical_json_bytes(body) + b"\n"
    digest = hashlib.sha256(payload).hexdigest()
    run_dir = _run_dir_from_context(context)
    run_dir.mkdir(parents=True, exist_ok=True)
    path = run_dir / "quarantine-disappearance.audit"
    try:
        descriptor = os.open(
            path,
            os.O_WRONLY | os.O_CREAT | os.O_EXCL | os.O_CLOEXEC | os.O_NOFOLLOW,
            0o600,
        )
    except FileExistsError:
        try:
            existing_descriptor = os.open(
                path, os.O_RDONLY | os.O_CLOEXEC | os.O_NOFOLLOW
            )
            metadata = os.fstat(existing_descriptor)
            if (
                not stat.S_ISREG(metadata.st_mode)
                or metadata.st_uid != os.geteuid()
                or metadata.st_nlink != 1
                or stat.S_IMODE(metadata.st_mode) != 0o600
                or metadata.st_size != len(payload)
            ):
                raise AirflowException(
                    "existing WhoScored quarantine audit metadata is unsafe"
                )
            existing = b""
            while len(existing) < metadata.st_size:
                chunk = os.read(existing_descriptor, metadata.st_size - len(existing))
                if not chunk:
                    break
                existing += chunk
        except OSError as exc:
            raise AirflowException(
                "cannot reopen WhoScored quarantine-disappearance audit"
            ) from exc
        finally:
            if "existing_descriptor" in locals():
                os.close(existing_descriptor)
        if existing != payload:
            raise AirflowException(
                "conflicting WhoScored quarantine-disappearance audit"
            )
    except OSError as exc:
        raise AirflowException(
            "cannot persist WhoScored quarantine-disappearance audit"
        ) from exc
    else:
        try:
            view = memoryview(payload)
            while view:
                written = os.write(descriptor, view)
                if written <= 0:
                    raise AirflowException(
                        "cannot complete WhoScored quarantine audit write"
                    )
                view = view[written:]
            os.fsync(descriptor)
        finally:
            os.close(descriptor)
    return {"audit_path": str(path), "audit_sha256": digest, **body}


def _dag_id_from_context(context: Mapping[str, Any]) -> str:
    dag = context.get("dag")
    return str(
        getattr(dag, "dag_id", None)
        or context.get("dag_id")
        or getattr(context.get("dag_run"), "dag_id", "")
        or ""
    )


def _validate_scheduled_scope_workloads(
    repository: Any,
    selected: Sequence[tuple[Any, Any]],
    signed_workloads: Sequence[Any],
) -> None:
    """Recreate signed due identities after discovery and before scope network."""

    from scrapers.whoscored.proxy_campaign import (
        scheduled_scope_player_pagination_target_limit,
        scheduled_scope_schedule_target_limit,
        scheduled_target_ids_sha256,
    )

    if len(selected) != len(signed_workloads):
        raise AirflowException("signed WhoScored workload selection is incomplete")
    for (scope, runtime), workload in zip(selected, signed_workloads):
        if scope.spec != workload.scope:
            raise AirflowException("signed WhoScored workload order is invalid")
        stage_ids = sorted(
            {int(stage_id) for stage_id in getattr(runtime, "stage_ids", ())}
        )
        start = getattr(runtime, "start", None)
        end = getattr(runtime, "end", None)
        if not stage_ids or start is None or end is None or end < start:
            raise AirflowException(
                f"signed WhoScored scope has no bounded schedule: {scope.spec}"
            )
        month_count = (end.year - start.year) * 12 + end.month - start.month + 1
        try:
            schedule_limit = scheduled_scope_schedule_target_limit(
                stage_count=len(stage_ids), season_month_count=month_count
            )
        except Exception as exc:
            raise AirflowException(
                f"signed WhoScored schedule target bound is invalid: {scope.spec}"
            ) from exc
        schedule_sha256 = scheduled_target_ids_sha256(
            [f"season:{scope.spec}", *(f"stage:{value}" for value in stage_ids)]
        )
        matches = repository.list_match_candidates(
            scope.competition_id,
            scope.season_id,
            limit=101,
            include_exact_count=True,
        )
        match_count = (
            int(matches[0].exact_candidate_count)
            if matches and matches[0].exact_candidate_count is not None
            else len(matches)
        )
        match_ids = sorted({int(item.game_id) for item in matches})
        previews = repository.list_preview_candidates(
            scope.competition_id,
            scope.season_id,
            limit=257,
        )
        preview_ids = sorted({int(item["game_id"]) for item in previews})
        non_pagination_count = schedule_limit + match_count + len(preview_ids)
        try:
            pagination_limit = scheduled_scope_player_pagination_target_limit(
                stage_count=len(stage_ids),
                non_pagination_target_count=non_pagination_count,
            )
        except Exception as exc:
            raise AirflowException(
                f"signed WhoScored pagination target bound is invalid: {scope.spec}"
            ) from exc
        if (
            match_count > 100
            or len(match_ids) != match_count
            or len(preview_ids) != len(previews)
            or schedule_limit != workload.schedule_target_limit
            or schedule_sha256 != workload.schedule_targets_sha256
            or pagination_limit != workload.player_pagination_target_limit
            or match_count != workload.match_target_count
            or scheduled_target_ids_sha256(match_ids) != workload.match_targets_sha256
            or len(preview_ids) != workload.preview_target_count
            or scheduled_target_ids_sha256(preview_ids)
            != workload.preview_targets_sha256
        ):
            raise AirflowException(
                f"signed WhoScored due target identity drifted: {scope.spec}"
            )


def _daily_scope_plan_specs(scope_plan: Mapping[str, Any]) -> tuple[str, list[str]]:
    common_fields = {
        "schema_version",
        "catalog_batch_id",
        "catalog_payload_sha256",
        "catalog_raw_provenance_sha256",
        "technical_exclusion_audit_sha256",
        "catalog_as_of_date",
        "parent_catalog_batch_id",
        "parent_catalog_payload_sha256",
        "parent_catalog_raw_provenance_sha256",
        "active_scopes",
        "active_scope_count",
        "active_scopes_sha256",
    }
    paid_fields = {
        "catalog_active_scope_count",
        "deferred_scopes",
        "deferred_scope_count",
        "deferred_scopes_sha256",
        "cohort_id",
        "cohort_sha256",
        "workload_sha256",
        "scope_workloads",
        "discovery_parent_target_count",
        "discovery_expansion_headroom",
        "discovery_target_limit",
        "profile_target_count",
        "profile_targets_sha256",
    }
    schema_version = scope_plan.get("schema_version")
    expected_fields = common_fields | (paid_fields if schema_version == 3 else set())
    if set(scope_plan) != expected_fields:
        raise AirflowException("invalid frozen WhoScored daily scope plan schema")
    batch_id = str(scope_plan.get("catalog_batch_id") or "")
    payload_sha256 = str(scope_plan.get("catalog_payload_sha256") or "")
    identity = _catalog_identity(
        {
            "schema_version": 1,
            **{
                key: scope_plan.get(key)
                for key in _CATALOG_IDENTITY_KEYS
                if key != "schema_version"
            },
        },
        exact=True,
    )
    scopes = scope_plan.get("active_scopes")
    if (
        schema_version not in {2, 3}
        or not re.fullmatch(r"[A-Za-z0-9_.-]{1,120}", batch_id)
        or re.fullmatch(r"[0-9a-f]{64}", payload_sha256) is None
        or identity["catalog_batch_id"] != batch_id
        or identity["catalog_payload_sha256"] != payload_sha256
        or not isinstance(scopes, list)
        or not scopes
        or any(not isinstance(value, str) or not value for value in scopes)
        or scopes != sorted(set(scopes))
        or scope_plan.get("active_scope_count") != len(scopes)
        or scope_plan.get("active_scopes_sha256")
        != hashlib.sha256(("\n".join(scopes) + "\n").encode("utf-8")).hexdigest()
    ):
        raise AirflowException("invalid frozen WhoScored daily scope plan identity")
    if schema_version == 3:
        from scrapers.whoscored.proxy_campaign import (
            SCHEDULED_DISCOVERY_EXPANSION_HEADROOM,
            SCHEDULED_DISCOVERY_TARGET_LIMIT_MAX,
            ScheduledScopeWorkload,
            canonical_json_bytes,
        )

        deferred = scope_plan.get("deferred_scopes")
        raw_workloads = scope_plan.get("scope_workloads")
        try:
            workloads = (
                [ScheduledScopeWorkload.from_dict(item) for item in raw_workloads]
                if isinstance(raw_workloads, list)
                else []
            )
        except Exception as exc:
            raise AirflowException("invalid paid WhoScored scope workloads") from exc
        discovery_parent_count = scope_plan.get("discovery_parent_target_count")
        discovery_target_limit = scope_plan.get("discovery_target_limit")
        profile_target_count = scope_plan.get("profile_target_count")
        if (
            not isinstance(deferred, list)
            or any(not isinstance(value, str) or not value for value in deferred)
            or deferred != sorted(set(deferred))
            or set(deferred) & set(scopes)
            or scope_plan.get("deferred_scope_count") != len(deferred)
            or scope_plan.get("catalog_active_scope_count")
            != len(scopes) + len(deferred)
            or scope_plan.get("deferred_scopes_sha256")
            != hashlib.sha256(
                ("\n".join(deferred) + ("\n" if deferred else "")).encode("utf-8")
            ).hexdigest()
            or re.fullmatch(
                r"[A-Za-z0-9][A-Za-z0-9._:-]{0,127}",
                str(scope_plan.get("cohort_id") or ""),
            )
            is None
            or re.fullmatch(r"[0-9a-f]{64}", str(scope_plan.get("cohort_sha256") or ""))
            is None
            or re.fullmatch(
                r"[0-9a-f]{64}", str(scope_plan.get("workload_sha256") or "")
            )
            is None
            or [item.scope for item in workloads] != scopes
            or scope_plan.get("workload_sha256")
            != hashlib.sha256(
                canonical_json_bytes([item.to_dict() for item in workloads])
            ).hexdigest()
            or isinstance(discovery_parent_count, bool)
            or not isinstance(discovery_parent_count, int)
            or discovery_parent_count <= 0
            or scope_plan.get("discovery_expansion_headroom")
            != SCHEDULED_DISCOVERY_EXPANSION_HEADROOM
            or discovery_target_limit
            != min(
                SCHEDULED_DISCOVERY_TARGET_LIMIT_MAX,
                discovery_parent_count + SCHEDULED_DISCOVERY_EXPANSION_HEADROOM,
            )
            or isinstance(profile_target_count, bool)
            or not isinstance(profile_target_count, int)
            or not 0 <= profile_target_count <= 256
            or re.fullmatch(
                r"[0-9a-f]{64}",
                str(scope_plan.get("profile_targets_sha256") or ""),
            )
            is None
        ):
            raise AirflowException("invalid paid WhoScored cohort/deferred identity")
    return batch_id, list(scopes)


def initialize_whoscored_schema() -> Dict[str, Any]:
    """Run additive schema evolution once before any dynamically mapped writer."""
    from scrapers.whoscored.repository import WhoScoredRepository

    WhoScoredRepository().ensure_schema(create_views=True)
    return {"status": "success"}


def validate_whoscored_runtime(**context: Any) -> Dict[str, Any]:
    """Fail unless one coherent release and transport authority are deployed."""

    executor = os.environ.get("AIRFLOW__CORE__EXECUTOR", "").strip()
    if not executor.endswith("LocalExecutor"):
        raise AirflowException(
            "WhoScored production requires AIRFLOW__CORE__EXECUTOR=LocalExecutor; "
            f"got {executor or 'unset'}"
        )
    params = context.get("params", {})
    transport = _transport_runtime(context)
    if not transport.is_paid and (
        not bool(params.get("direct_only", True))
        or not bool(params.get("require_zero_paid", True))
    ):
        raise AirflowException(
            "legacy booleans cannot authorize paid WhoScored traffic; use a "
            "signed direct_then_paid DagRun configuration"
        )
    try:
        contract = _WHOSCORED_RUNTIME_CONTRACT.validate_runtime_contract()
        pool_contract = _WHOSCORED_RUNTIME_CONTRACT.validate_airflow_source_pool(
            direct_pool=DIRECT_POOL,
            backfill_pool=os.environ.get(
                "WHOSCORED_BACKFILL_POOL", "whoscored_direct_pool"
            ),
        )
    except _WHOSCORED_RUNTIME_CONTRACT.RuntimeContractError as exc:
        raise AirflowException(str(exc)) from exc
    return {
        "status": "success",
        "executor": executor,
        "direct_only": not transport.is_paid,
        "transport_policy": transport.policy,
        "campaign_id": transport.campaign_id or None,
        "approval_id": transport.approval_id or None,
        "runtime_contract": contract,
        "source_pool_contract": pool_contract,
    }


def build_daily_discovery_command(
    *,
    alert_metadata: Optional[Mapping[str, Any]] = None,
    **context: Any,
) -> str:
    """Resolve signed discovery authority at task runtime, never DAG parse time."""

    work_item_id = "catalog-discovery"
    transport = _transport_runtime(
        context,
        task_id="discover_whoscored_catalog",
        work_item_id=work_item_id,
    )
    alert_guard = paid_alert_source_guard_command(
        transport,
        alert_metadata,
        context,
    )
    output = _run_dir_from_context(context) / "discovery.json"
    as_of_date = _logical_as_of_date(context)
    return (
        "cd /opt/airflow && "
        f"{alert_guard}"
        "python dags/scripts/run_whoscored_scraper.py discover "
        f"--as-of-date {shlex.quote(as_of_date)} "
        f"--output {shlex.quote(str(output))} "
        f"{transport.cli_args(work_item_id=work_item_id)}"
    )


def build_daily_commands(
    *,
    scope_plan: Optional[Mapping[str, Any]] = None,
    alert_metadata: Optional[Mapping[str, Any]] = None,
    **context: Any,
) -> list[str]:
    """Return one safely quoted CLI command per active persisted scope."""
    run_dir = _run_dir_from_context(context)
    cleanup_stale_run_directories(current_run_dir=run_dir)
    base_transport = _transport_runtime(context)
    commands: list[str] = []
    if scope_plan is None:
        batch_id = None
        scopes = _active_scope_specs()
    else:
        batch_id, scopes = _daily_scope_plan_specs(scope_plan)
    for scope in scopes:
        output = _scope_result_path(run_dir, scope)
        catalog_arg = f" --catalog-batch-id {shlex.quote(batch_id)}" if batch_id else ""
        work_item_id = stable_scope_work_item(scope)
        transport = _bind_transport_allocation(
            base_transport,
            task_id="ingest_active_scope",
            work_item_id=work_item_id,
        )
        alert_guard = paid_alert_source_guard_command(
            transport,
            alert_metadata,
            context,
        )
        commands.append(
            "cd /opt/airflow && "
            f"{alert_guard}"
            "python dags/scripts/run_whoscored_scraper.py daily "
            f"--scope {shlex.quote(scope)} --skip-profiles "
            f"--output {shlex.quote(str(output))}{catalog_arg} "
            f"{transport.cli_args(work_item_id=work_item_id)}"
        )
    return commands


def build_scope_validation_kwargs(
    *,
    scope_plan: Optional[Mapping[str, Any]] = None,
    **context: Any,
) -> list[dict[str, Any]]:
    run_dir = _run_dir_from_context(context)
    require_zero = not _transport_runtime(context).is_paid
    if scope_plan is None:
        batch_id = None
        scopes = _active_scope_specs()
    else:
        batch_id, scopes = _daily_scope_plan_specs(scope_plan)
    return [
        {
            "scope_spec": scope,
            "result_path": str(_scope_result_path(run_dir, scope)),
            "require_zero_paid": require_zero,
            "expected_catalog_batch_id": batch_id,
        }
        for scope in scopes
    ]


def build_daily_profile_command(
    *,
    scope_plan: Mapping[str, Any],
    capacity: Mapping[str, Any],
    alert_metadata: Optional[Mapping[str, Any]] = None,
    **context: Any,
) -> str:
    """Build one global profile command against the frozen active catalog."""

    batch_id, scopes = _daily_scope_plan_specs(scope_plan)
    selected_limit, candidate_sha256 = _daily_profile_capacity_specs(
        capacity,
        expected_catalog_batch_id=batch_id,
    )
    output = _run_dir_from_context(context) / "profiles.json"
    scope_args = " ".join(f"--scope {shlex.quote(scope)}" for scope in scopes)
    work_item_id = stable_profiles_work_item()
    transport = _transport_runtime(
        context,
        task_id="refresh_whoscored_profiles",
        work_item_id=work_item_id,
    )
    alert_guard = paid_alert_source_guard_command(
        transport,
        alert_metadata,
        context,
    )
    return (
        "cd /opt/airflow && "
        f"{alert_guard}"
        "python dags/scripts/run_whoscored_scraper.py daily --profiles-only "
        f"{scope_args} --catalog-batch-id {shlex.quote(batch_id)} "
        f"--profiles-limit {selected_limit} --output {shlex.quote(str(output))} "
        f"--expected-profile-candidate-count {selected_limit} "
        f"--expected-profile-candidate-sha256 {candidate_sha256} "
        f"{transport.cli_args(work_item_id=work_item_id)}"
    )


def _load_result(path: str) -> dict[str, Any]:
    try:
        with Path(path).open("r", encoding="utf-8") as handle:
            value = json.load(handle)
    except (OSError, ValueError) as exc:
        raise AirflowException(
            f"WhoScored result {path} is unavailable: {exc}"
        ) from exc
    if not isinstance(value, dict) or value.get("schema_version") != 3:
        raise AirflowException(f"WhoScored result {path} is not report schema v3")
    return value


_ATTEMPT_REPORT_RE = re.compile(
    r"(?P<output>.+\.json)\.attempt-"
    r"(?P<identity>[0-9a-f]{64})-(?P<content>[0-9a-f]{64})\.json"
)


def _attempt_report_identity_sha256(output_name: str, report: Mapping[str, Any]) -> str:
    airflow = report.get("airflow")
    if not isinstance(airflow, Mapping):
        raise AirflowException("WhoScored attempt report has no Airflow identity")
    dag_id = airflow.get("dag_id")
    run_id = airflow.get("dag_run_id")
    task_id = airflow.get("task_id")
    try:
        map_index = int(airflow.get("map_index"))
        try_number = int(airflow.get("try_number"))
    except (TypeError, ValueError) as exc:
        raise AirflowException(
            "WhoScored attempt report has an invalid Airflow identity"
        ) from exc
    if (
        not isinstance(dag_id, str)
        or not dag_id
        or not isinstance(run_id, str)
        or not run_id
        or not isinstance(task_id, str)
        or not task_id
        or map_index < -1
        or try_number < 1
    ):
        raise AirflowException(
            "WhoScored attempt report has an invalid Airflow identity"
        )
    identity = {
        "dag_id": dag_id,
        "run_id": run_id,
        "task_id": task_id,
        "map_index": map_index,
        "try_number": try_number,
        "output_name": output_name,
    }
    return hashlib.sha256(
        json.dumps(identity, sort_keys=True, separators=(",", ":")).encode("utf-8")
    ).hexdigest()


def _canonical_traffic_url(value: Any) -> str:
    """Keep the full URL while making equivalent query order deterministic."""
    raw = str(value or "").strip()
    parts = urlsplit(raw)
    if not parts.scheme or not parts.netloc:
        return raw.split("#", 1)[0]
    query = urlencode(sorted(parse_qsl(parts.query, keep_blank_values=True)))
    return urlunsplit(
        (
            parts.scheme.lower(),
            parts.netloc.lower(),
            parts.path or "/",
            query,
            "",
        )
    )


def _iter_jsonl_events(path: Path, *, label: str):
    """Stream bounded JSONL records and fail closed on truncation/corruption."""
    try:
        handle = path.open("rb")
    except OSError as exc:
        raise AirflowException(f"cannot read {label} {path}: {exc}") from exc
    with handle:
        line_number = 0
        while True:
            raw = handle.readline(MAX_LEDGER_EVENT_BYTES + 1)
            if not raw:
                return
            line_number += 1
            if len(raw) > MAX_LEDGER_EVENT_BYTES:
                raise AirflowException(
                    f"oversized {label} {path}:{line_number}: "
                    f"limit is {MAX_LEDGER_EVENT_BYTES} bytes"
                )
            try:
                event = json.loads(raw.decode("utf-8"))
            except (UnicodeDecodeError, ValueError) as exc:
                raise AirflowException(
                    f"corrupt {label} {path}:{line_number}: {exc}"
                ) from exc
            if not isinstance(event, dict):
                raise AirflowException(
                    f"corrupt {label} {path}:{line_number}: JSON object required"
                )
            yield event


def _file_sha256_and_bytes(path: Path) -> tuple[str, int]:
    """Hash one local ledger without loading a potentially large file."""

    digest = hashlib.sha256()
    size = 0
    try:
        handle = path.open("rb")
    except OSError as exc:
        raise AirflowException(f"cannot read request ledger {path}: {exc}") from exc
    with handle:
        while True:
            chunk = handle.read(1024 * 1024)
            if not chunk:
                break
            digest.update(chunk)
            size += len(chunk)
    return digest.hexdigest(), size


def _assert_staging_input_files(
    run_dir: Path,
    input_evidence: Mapping[str, Any],
    *,
    require_all: bool,
) -> None:
    for field, pattern in (
        ("reports", "*.json"),
        ("request_ledgers", "requests_*.jsonl"),
    ):
        records = input_evidence.get(field)
        if not isinstance(records, list):
            raise AirflowException("WhoScored input evidence file set is malformed")
        expected: dict[str, tuple[str, int]] = {}
        required: set[str] = set()
        for record in records:
            base_fields = {"source_name", "source_sha256", "source_bytes"}
            record_fields = set(record) if isinstance(record, Mapping) else set()
            durable = record.get("durable") if isinstance(record, Mapping) else None
            expected_fields = set(base_fields)
            if field == "request_ledgers":
                expected_fields.add("durable")
                if durable is True:
                    expected_fields.update(
                        {
                            "evidence_key",
                            "evidence_sha256",
                            "evidence_bytes",
                        }
                    )
            if (
                not isinstance(record, Mapping)
                or record_fields != expected_fields
                or not isinstance(record.get("source_name"), str)
                or Path(record["source_name"]).name != record["source_name"]
                or not isinstance(record.get("source_sha256"), str)
                or re.fullmatch(r"[0-9a-f]{64}", record["source_sha256"]) is None
                or type(record.get("source_bytes")) is not int
                or record["source_bytes"] < 0
                or (field == "request_ledgers" and type(durable) is not bool)
                or (
                    durable is True
                    and (
                        not isinstance(record.get("evidence_key"), str)
                        or not isinstance(record.get("evidence_sha256"), str)
                        or re.fullmatch(r"[0-9a-f]{64}", record["evidence_sha256"])
                        is None
                        or not record["evidence_key"].endswith(
                            f"/{record['evidence_sha256']}.json"
                        )
                        or type(record.get("evidence_bytes")) is not int
                        or record["evidence_bytes"] <= 0
                    )
                )
                or record["source_name"] in expected
            ):
                raise AirflowException(
                    "WhoScored input evidence file identity is malformed"
                )
            expected[record["source_name"]] = (
                record["source_sha256"],
                record["source_bytes"],
            )
            if field != "request_ledgers" or durable is False:
                required.add(record["source_name"])
        present: set[str] = set()
        for path in sorted(run_dir.glob(pattern)):
            identity = expected.get(path.name)
            if identity is None or _file_sha256_and_bytes(path) != identity:
                raise AirflowException(
                    f"WhoScored staging input changed after reconciliation: {path.name}"
                )
            present.add(path.name)
        if require_all and not required.issubset(present):
            raise AirflowException(
                "WhoScored staging inputs disappeared before durable completion"
            )


_DURABLE_REQUEST_EVIDENCE_FIELDS = {
    "schema_version",
    "evidence_type",
    "dag_id",
    "run_id",
    "task_id",
    "map_index",
    "try_number",
    "source_name",
    "source_sha256",
    "source_bytes",
    "event_count",
    "request_count",
    "wire_bytes",
    "paid_proxy_bytes",
    "events",
}


def _validated_durable_request_evidence(
    ops_store: Any,
    *,
    prefix: str,
    dag_id: str,
    run_id: str,
) -> tuple[
    dict[str, tuple[str, int]],
    list[tuple[str, Mapping[str, Any]]],
    list[dict[str, Any]],
]:
    """Bind reconstructed JSONL bytes to each immutable evidence object."""

    durable_by_source: dict[str, tuple[str, int]] = {}
    durable_events: list[tuple[str, Mapping[str, Any]]] = []
    durable_records: list[dict[str, Any]] = []
    durable_event_ids: set[str] = set()
    normalized_prefix = prefix.strip("/")
    for key, evidence in ops_store.iter_content_addressed_json(normalized_prefix):
        evidence_payload = (
            json.dumps(
                evidence,
                ensure_ascii=False,
                sort_keys=True,
                separators=(",", ":"),
            ).encode("utf-8")
            + b"\n"
        )
        evidence_sha256 = hashlib.sha256(evidence_payload).hexdigest()
        source_name = evidence.get("source_name")
        source_sha256 = evidence.get("source_sha256")
        source_bytes = evidence.get("source_bytes")
        events = evidence.get("events")
        map_index = evidence.get("map_index")
        try_number = evidence.get("try_number")
        counters = {
            field: evidence.get(field)
            for field in (
                "event_count",
                "request_count",
                "wire_bytes",
                "paid_proxy_bytes",
            )
        }
        if (
            set(evidence) != _DURABLE_REQUEST_EVIDENCE_FIELDS
            or key != f"{normalized_prefix}/{evidence_sha256}.json"
            or evidence.get("schema_version") != 1
            or evidence.get("evidence_type") != "whoscored_request_ledger"
            or evidence.get("dag_id") != dag_id
            or evidence.get("run_id") != run_id
            or not isinstance(evidence.get("task_id"), str)
            or not evidence.get("task_id")
            or isinstance(map_index, bool)
            or not isinstance(map_index, int)
            or map_index < -1
            or isinstance(try_number, bool)
            or not isinstance(try_number, int)
            or try_number < 1
            or not isinstance(source_name, str)
            or Path(source_name).name != source_name
            or not source_name.startswith("requests_")
            or not source_name.endswith(".jsonl")
            or not isinstance(source_sha256, str)
            or re.fullmatch(r"[0-9a-f]{64}", source_sha256) is None
            or type(source_bytes) is not int
            or source_bytes < 0
            or not isinstance(events, list)
            or any(type(value) is not int or value < 0 for value in counters.values())
            or counters["event_count"] != len(events)
        ):
            raise AirflowException(
                f"invalid durable WhoScored request ledger evidence {key}"
            )
        expected_identity = {
            "dag_id": evidence["dag_id"],
            "run_id": evidence["run_id"],
            "task_id": evidence["task_id"],
            "map_index": map_index,
            "try_number": try_number,
        }
        reconstructed_digest = hashlib.sha256()
        reconstructed_bytes = 0
        reconstructed_requests = 0
        reconstructed_wire_bytes = 0
        reconstructed_paid_bytes = 0
        evidence_events: list[tuple[str, Mapping[str, Any]]] = []
        evidence_event_ids: set[str] = set()
        for event in events:
            event_id = event.get("event_id") if isinstance(event, Mapping) else None
            if (
                not isinstance(event, Mapping)
                or event.get("event_version") != "whoscored-request-v1"
                or not isinstance(event_id, str)
                or re.fullmatch(r"[0-9a-f]{32}", event_id) is None
                or any(
                    event.get(field) != value
                    for field, value in expected_identity.items()
                )
                or event_id in durable_event_ids
                or event_id in evidence_event_ids
            ):
                raise AirflowException(
                    f"invalid event in durable WhoScored request ledger {key}"
                )
            rendered = (
                json.dumps(
                    dict(event),
                    ensure_ascii=False,
                    sort_keys=True,
                    separators=(",", ":"),
                ).encode("utf-8")
                + b"\n"
            )
            reconstructed_digest.update(rendered)
            reconstructed_bytes += len(rendered)
            event_counters: dict[str, int] = {}
            for field in ("request_bytes", "response_bytes", "paid_proxy_bytes"):
                value = event.get(field, 0)
                if type(value) is not int or value < 0:
                    raise AirflowException(
                        f"invalid {field} in durable WhoScored request ledger {key}"
                    )
                event_counters[field] = value
            if str(event.get("status") or "") == "accounted":
                reconstructed_paid_bytes += event_counters["paid_proxy_bytes"]
            else:
                if event_counters["paid_proxy_bytes"]:
                    raise AirflowException(
                        "unaccounted paid bytes in durable WhoScored request "
                        f"ledger {key}"
                    )
                reconstructed_requests += 1
                reconstructed_wire_bytes += (
                    event_counters["request_bytes"] + event_counters["response_bytes"]
                )
            evidence_event_ids.add(event_id)
            evidence_events.append((f"durable request ledger {key}", event))
        if (
            reconstructed_digest.hexdigest() != source_sha256
            or reconstructed_bytes != source_bytes
            or counters["request_count"] != reconstructed_requests
            or counters["wire_bytes"] != reconstructed_wire_bytes
            or counters["paid_proxy_bytes"] != reconstructed_paid_bytes
        ):
            raise AirflowException(
                f"durable WhoScored request ledger summary mismatch {key}"
            )
        source_identity = (source_sha256, source_bytes)
        if source_name in durable_by_source:
            if durable_by_source[source_name] != source_identity:
                raise AirflowException(
                    f"conflicting durable WhoScored request ledgers for {source_name}"
                )
            raise AirflowException(
                f"duplicate durable WhoScored request ledger {source_name}"
            )
        durable_by_source[source_name] = source_identity
        durable_event_ids.update(evidence_event_ids)
        durable_events.extend(evidence_events)
        durable_records.append(
            {
                "source_name": source_name,
                "source_sha256": source_sha256,
                "source_bytes": source_bytes,
                "durable": True,
                "evidence_key": key,
                "evidence_sha256": evidence_sha256,
                "evidence_bytes": len(evidence_payload),
            }
        )
    durable_records.sort(key=lambda item: item["evidence_key"])
    return durable_by_source, durable_events, durable_records


def _counter_add(target: dict[str, int], key: str, value: int) -> None:
    target[key] = target.get(key, 0) + max(0, int(value))


def _traffic_task_key(event: Mapping[str, Any], *, include_try: bool = False) -> str:
    task_id = str(event.get("task_id") or "unknown")
    try:
        map_index = int(event.get("map_index", -1))
    except (TypeError, ValueError):
        map_index = -1
    key = f"{task_id}[{map_index}]"
    if include_try:
        try:
            try_number = int(event.get("try_number", 0))
        except (TypeError, ValueError):
            try_number = 0
        key += f"/try{try_number}"
    return key


def _validate_paid_report_identity(
    transport: PaidRuntime,
    report: Mapping[str, Any],
    *,
    dag_id: str,
    run_id: str,
) -> None:
    if not transport.is_paid:
        return
    if str(report.get("transport_policy") or "") != "direct_then_paid":
        raise AirflowException("paid WhoScored report lost its transport policy")
    approval = getattr(transport, "approval", None)
    airflow = report.get("airflow")
    if approval is None or not isinstance(airflow, Mapping):
        raise AirflowException("WhoScored paid report has no signed identity")
    from scrapers.whoscored.proxy_campaign import (
        ProxyCampaignError,
        deterministic_proxy_attempt_id,
    )

    allocation_id = str(report.get("proxy_allocation_id") or "")
    try:
        allocation = approval.allocation(allocation_id)
        map_index = int(airflow.get("map_index"))
        try_number = int(airflow.get("try_number"))
        attempt_id = deterministic_proxy_attempt_id(
            dag_id=dag_id,
            run_id=run_id,
            task_id=str(airflow.get("task_id") or ""),
            map_index=map_index,
            try_number=try_number,
        )
    except (AttributeError, ProxyCampaignError, TypeError, ValueError) as exc:
        raise AirflowException("WhoScored paid report identity is malformed") from exc
    if (
        report.get("transport_policy") != "direct_then_paid"
        or report.get("proxy_approval_id") != approval.approval_id
        or report.get("proxy_approval_sha256") != approval.approval_sha256
        or report.get("proxy_work_item_id") != allocation.work_item_id
        or report.get("proxy_attempt_id") != attempt_id
        or airflow.get("dag_id") != dag_id
        or airflow.get("dag_run_id") != run_id
        or airflow.get("task_id") != allocation.task_id
        or isinstance(airflow.get("map_index"), bool)
        or isinstance(airflow.get("try_number"), bool)
        or map_index < -1
        or try_number < 1
    ):
        raise AirflowException("WhoScored paid report is outside signed authority")


def _campaign_ledger_paid_bytes(
    transport: PaidRuntime,
    paid_events: Sequence[Mapping[str, Any]],
    request_paid_events: Sequence[Mapping[str, Any]] = (),
    *,
    dag_id: str,
    run_id: str,
    reconciliation_evidence: Optional[dict[str, Any]] = None,
) -> int:
    """Reconcile this DagRun's proxy events with the authenticated campaign WAL."""

    if not transport.is_paid or transport.approval is None:
        return 0
    from scrapers.whoscored.proxy_campaign import (
        PROXY_CAMPAIGN_METER,
        ProxyCampaignError,
        canonical_json_bytes,
        deterministic_proxy_attempt_id,
    )

    approval = transport.approval
    if approval.run_id != str(run_id) or approval.allowed_dag_ids != (str(dag_id),):
        raise AirflowException(
            "WhoScored signed campaign belongs to another Airflow DagRun"
        )

    approved_allocations = {item.allocation_id: item for item in approval.allocations}
    request_leases: dict[tuple[str, str, str], dict[str, Any]] = {}
    request_event_ids: set[str] = set()
    request_lease_hashes: set[str] = set()
    request_batch_fields = {
        "gateway_target_manifest_sha256",
        "gateway_endpoint_provider_bytes",
        "gateway_bootstrap_provider_bytes",
    }
    for event in request_paid_events:
        event_id = event.get("event_id")
        allocation_id = str(event.get("proxy_allocation_id") or "")
        allocation = approved_allocations.get(allocation_id)
        raw_map_index = event.get("map_index")
        raw_try_number = event.get("try_number")
        if (
            event.get("event_version") != "whoscored-request-v1"
            or not isinstance(event_id, str)
            or re.fullmatch(r"[0-9a-f]{32}", event_id) is None
            or event_id in request_event_ids
            or event.get("status") != "accounted"
            or event.get("dag_id") != str(dag_id)
            or event.get("run_id") != str(run_id)
            or event.get("proxy_campaign_id") != approval.campaign_id
            or event.get("proxy_approval_id") != approval.approval_id
            or event.get("proxy_approval_sha256") != approval.approval_sha256
            or allocation is None
            or event.get("task_id") != allocation.task_id
            or event.get("proxy_work_item_id") != allocation.work_item_id
            or isinstance(raw_map_index, bool)
            or not isinstance(raw_map_index, int)
            or raw_map_index < -1
            or isinstance(raw_try_number, bool)
            or not isinstance(raw_try_number, int)
            or raw_try_number < 1
        ):
            raise AirflowException(
                "WhoScored paid request event is outside signed authority"
            )
        request_event_ids.add(event_id)
        try:
            attempt_id = deterministic_proxy_attempt_id(
                dag_id=str(dag_id),
                run_id=str(run_id),
                task_id=allocation.task_id,
                map_index=raw_map_index,
                try_number=raw_try_number,
            )
        except ProxyCampaignError as exc:
            raise AirflowException(
                "WhoScored paid request attempt identity is malformed"
            ) from exc
        lease_hash = event.get("lease_id_hash")
        canonical_url = event.get("url")
        billed = event.get("paid_proxy_bytes")
        if (
            event.get("proxy_attempt_id") != attempt_id
            or "lease_id" in event
            or not isinstance(lease_hash, str)
            or re.fullmatch(r"[0-9a-f]{64}", lease_hash) is None
            or not isinstance(canonical_url, str)
            or not canonical_url
            or type(billed) is not int
            or billed < 0
        ):
            raise AirflowException(
                "WhoScored paid request lease accounting is malformed"
            )
        present_batch_fields = request_batch_fields & set(event)
        batch_accounting: dict[str, Any] | None = None
        if present_batch_fields:
            manifest_sha256 = event.get("gateway_target_manifest_sha256")
            raw_endpoint_bytes = event.get("gateway_endpoint_provider_bytes")
            bootstrap_bytes = event.get("gateway_bootstrap_provider_bytes")
            request_bytes = event.get("request_bytes")
            response_bytes = event.get("response_bytes")
            resource_bytes = event.get("resource_bytes")
            if (
                present_batch_fields != request_batch_fields
                or event.get("route") != "paid_lease"
                or event.get("final_paid_route") != "paid_flaresolverr"
                or event.get("gateway_cleanup_complete") is not True
                or not isinstance(manifest_sha256, str)
                or re.fullmatch(r"[0-9a-f]{64}", manifest_sha256) is None
                or not isinstance(raw_endpoint_bytes, Mapping)
                or not 1 <= len(raw_endpoint_bytes) <= 8
                or type(bootstrap_bytes) is not int
                or bootstrap_bytes < 0
                or type(request_bytes) is not int
                or request_bytes < 0
                or type(response_bytes) is not int
                or response_bytes < 0
                or type(resource_bytes) is not int
                or resource_bytes != billed
                or request_bytes + response_bytes != billed
            ):
                raise AirflowException(
                    "WhoScored paid batch request accounting is malformed"
                )
            endpoint_bytes: dict[str, int] = {}
            for endpoint_digest, raw_amount in raw_endpoint_bytes.items():
                if (
                    not isinstance(endpoint_digest, str)
                    or re.fullmatch(r"[0-9a-f]{64}", endpoint_digest) is None
                    or type(raw_amount) is not int
                    or raw_amount < 0
                ):
                    raise AirflowException(
                        "WhoScored paid batch endpoint accounting is malformed"
                    )
                endpoint_bytes[endpoint_digest] = raw_amount
            canonical_url_sha256 = hashlib.sha256(
                canonical_url.encode("utf-8")
            ).hexdigest()
            if (
                canonical_url_sha256 not in endpoint_bytes
                or bootstrap_bytes + sum(endpoint_bytes.values()) != billed
            ):
                raise AirflowException(
                    "WhoScored paid batch attribution differs from aggregate bytes"
                )
            batch_accounting = {
                "target_manifest_sha256": manifest_sha256,
                "logical_target_units": len(endpoint_bytes),
                "endpoint_provider_bytes": endpoint_bytes,
                "bootstrap_provider_bytes": bootstrap_bytes,
            }
        attempt_hash = hashlib.sha256(attempt_id.encode("utf-8")).hexdigest()
        if lease_hash in request_lease_hashes:
            raise AirflowException("WhoScored paid request lease is duplicated")
        request_lease_hashes.add(lease_hash)
        request_leases[(allocation_id, attempt_hash, lease_hash)] = {
            "canonical_url_sha256": hashlib.sha256(
                canonical_url.encode("utf-8")
            ).hexdigest(),
            "provider_billed_bytes": billed,
            "batch_accounting": batch_accounting,
        }
    proxy_leases: dict[tuple[str, str, str], dict[str, Any]] = {}
    proxy_lease_bindings: dict[str, tuple[str, str, str]] = {}
    seen_event_ids: set[str] = set()
    for event in paid_events:
        event_id = event.get("event_id")
        event_type = event.get("event_type")
        allocation_id = str(event.get("allocation_id") or "")
        allocation = approved_allocations.get(allocation_id)
        if (
            event.get("event_version") != "paid-proxy-v2"
            or not isinstance(event_id, str)
            or re.fullmatch(r"[0-9a-f]{24}", event_id) is None
            or event_id in seen_event_ids
            or event_type
            not in {
                "lease_created",
                "endpoint_started",
                "bytes",
                "endpoint_switched",
                "endpoint_finished",
                "lease_closed",
            }
            or event.get("dag_id") != str(dag_id)
            or event.get("run_id") != str(run_id)
            or event.get("proxy_campaign_id") != approval.campaign_id
            or event.get("proxy_approval_id") != approval.approval_id
            or event.get("proxy_approval_sha256") != approval.approval_sha256
            or event.get("provider_meter") != PROXY_CAMPAIGN_METER
            or allocation is None
            or event.get("task_id") != allocation.task_id
            or event.get("proxy_work_item_id") != allocation.work_item_id
        ):
            raise AirflowException(
                "WhoScored proxy ledger event is outside the signed campaign"
            )
        seen_event_ids.add(event_id)
        raw_map_index = event.get("map_index", -1)
        raw_try_number = event.get("try_number", 0)
        if (
            isinstance(raw_map_index, bool)
            or not isinstance(raw_map_index, int)
            or raw_map_index < -1
            or isinstance(raw_try_number, bool)
            or not isinstance(raw_try_number, int)
            or raw_try_number < 1
        ):
            raise AirflowException(
                "WhoScored proxy ledger has an invalid Airflow attempt identity"
            )
        try:
            attempt_id = deterministic_proxy_attempt_id(
                dag_id=str(event.get("dag_id") or ""),
                run_id=str(event.get("run_id") or ""),
                task_id=str(event.get("task_id") or ""),
                map_index=raw_map_index,
                try_number=raw_try_number,
            )
        except (ProxyCampaignError, TypeError, ValueError) as exc:
            raise AirflowException(
                "WhoScored proxy ledger has an invalid Airflow attempt identity"
            ) from exc
        if event.get("proxy_attempt_id") != attempt_id:
            raise AirflowException(
                "WhoScored proxy ledger attempt differs from its Airflow identity"
            )
        lease_id = event.get("lease_id")
        canonical_url = event.get("canonical_url")
        if (
            not isinstance(lease_id, str)
            or not lease_id
            or not isinstance(canonical_url, str)
            or not canonical_url
        ):
            raise AirflowException("WhoScored proxy ledger lease identity is malformed")
        attempt_hash = hashlib.sha256(attempt_id.encode("utf-8")).hexdigest()
        lease_hash = hashlib.sha256(lease_id.encode("utf-8")).hexdigest()
        canonical_url_hash = hashlib.sha256(canonical_url.encode("utf-8")).hexdigest()
        lease_key = (allocation_id, attempt_hash, lease_hash)
        previous_binding = proxy_lease_bindings.setdefault(lease_hash, lease_key)
        if previous_binding != lease_key:
            raise AirflowException(
                "WhoScored proxy ledger lease is bound to multiple attempts"
            )
        lease = proxy_leases.get(lease_key)
        if lease is not None and lease["canonical_url_sha256"] != canonical_url_hash:
            raise AirflowException("WhoScored proxy ledger lease changed canonical URL")
        if event_type == "lease_created":
            max_bytes = event.get("max_bytes")
            binding_fields = {
                "target_manifest_sha256",
                "logical_target_units",
                "expected_endpoint_labels",
            }
            present_binding_fields = binding_fields & set(event)
            batch_binding: dict[str, Any] | None = None
            if present_binding_fields:
                manifest_sha256 = event.get("target_manifest_sha256")
                logical_target_units = event.get("logical_target_units")
                expected_endpoint_labels = event.get("expected_endpoint_labels")
                if (
                    present_binding_fields != binding_fields
                    or not isinstance(manifest_sha256, str)
                    or re.fullmatch(r"[0-9a-f]{64}", manifest_sha256) is None
                    or type(logical_target_units) is not int
                    or not 1 <= logical_target_units <= 8
                    or not isinstance(expected_endpoint_labels, list)
                    or len(expected_endpoint_labels) != logical_target_units + 1
                    or any(
                        not isinstance(label, str)
                        or re.fullmatch(r"(?:bootstrap|target):[0-9a-f]{64}", label)
                        is None
                        for label in expected_endpoint_labels
                    )
                    or expected_endpoint_labels[0].split(":", 1)[0] != "bootstrap"
                    or expected_endpoint_labels[1:]
                    != sorted(expected_endpoint_labels[1:])
                    or len(set(expected_endpoint_labels))
                    != len(expected_endpoint_labels)
                    or any(
                        not label.startswith("target:")
                        for label in expected_endpoint_labels[1:]
                    )
                ):
                    raise AirflowException(
                        "WhoScored proxy batch lease binding is malformed"
                    )
                batch_binding = {
                    "target_manifest_sha256": manifest_sha256,
                    "logical_target_units": logical_target_units,
                    "expected_endpoint_labels": tuple(expected_endpoint_labels),
                }
            if (
                lease is not None
                or type(max_bytes) is not int
                or max_bytes <= 0
                or max_bytes > allocation.budget_bytes
            ):
                raise AirflowException(
                    "WhoScored proxy ledger lease creation is malformed"
                )
            proxy_leases[lease_key] = {
                "canonical_url_sha256": canonical_url_hash,
                "provider_billed_bytes": 0,
                "closed": False,
                "batch_binding": batch_binding,
                "active_endpoint": None,
                "endpoint_provider_bytes": {},
                "seen_endpoint_request_ids": set(),
            }
            continue
        if lease is None or lease["closed"] is True:
            raise AirflowException(
                "WhoScored proxy ledger lease lifecycle is missing or out of order"
            )
        if event_type == "endpoint_started":
            request_id = event.get("request_id")
            endpoint = event.get("endpoint")
            lease_total = event.get("lease_total_bytes")
            batch_binding = lease["batch_binding"]
            expected_labels = (
                set(batch_binding["expected_endpoint_labels"])
                if batch_binding is not None
                else None
            )
            endpoint_map = lease["endpoint_provider_bytes"]
            seen_request_ids = lease["seen_endpoint_request_ids"]
            if (
                lease["active_endpoint"] is not None
                or not isinstance(request_id, str)
                or re.fullmatch(r"[0-9a-f]{24}", request_id) is None
                or request_id in seen_request_ids
                or not isinstance(endpoint, str)
                or not endpoint
                or type(lease_total) is not int
                or lease_total != lease["provider_billed_bytes"]
                or (expected_labels is not None and endpoint not in expected_labels)
                or (
                    batch_binding is not None
                    and (
                        endpoint in endpoint_map
                        or (
                            not endpoint_map
                            and endpoint
                            != batch_binding["expected_endpoint_labels"][0]
                        )
                    )
                )
            ):
                raise AirflowException("WhoScored proxy endpoint start is malformed")
            seen_request_ids.add(request_id)
            lease["active_endpoint"] = {
                "request_id": request_id,
                "endpoint": endpoint,
                "start_bytes": lease_total,
            }
            continue
        if event_type == "bytes":
            billed = event.get("bytes")
            active_endpoint = lease["active_endpoint"]
            if (
                event.get("direction") not in {"up", "down"}
                or type(billed) is not int
                or billed <= 0
                or (
                    active_endpoint is not None
                    and event.get("endpoint") != active_endpoint["endpoint"]
                )
                or (lease["batch_binding"] is not None and active_endpoint is None)
            ):
                raise AirflowException("WhoScored proxy ledger byte event is malformed")
            lease["provider_billed_bytes"] += billed
            lease_total = event.get("lease_total_bytes")
            if lease_total is not None and (
                type(lease_total) is not int
                or lease_total != lease["provider_billed_bytes"]
            ):
                raise AirflowException("WhoScored proxy byte event total is malformed")
            continue
        if event_type == "endpoint_switched":
            active_endpoint = lease["active_endpoint"]
            request_id = event.get("request_id")
            endpoint = event.get("endpoint")
            provider_bytes = event.get("provider_bytes")
            lease_total = event.get("lease_total_bytes")
            next_request_id = event.get("next_request_id")
            next_endpoint = event.get("next_endpoint")
            batch_binding = lease["batch_binding"]
            expected_labels = (
                set(batch_binding["expected_endpoint_labels"])
                if batch_binding is not None
                else None
            )
            endpoint_map = lease["endpoint_provider_bytes"]
            seen_request_ids = lease["seen_endpoint_request_ids"]
            if (
                not isinstance(active_endpoint, Mapping)
                or request_id != active_endpoint.get("request_id")
                or endpoint != active_endpoint.get("endpoint")
                or type(provider_bytes) is not int
                or provider_bytes < 0
                or provider_bytes
                != lease["provider_billed_bytes"]
                - int(active_endpoint.get("start_bytes", -1))
                or type(lease_total) is not int
                or lease_total != lease["provider_billed_bytes"]
                or not isinstance(next_request_id, str)
                or re.fullmatch(r"[0-9a-f]{24}", next_request_id) is None
                or next_request_id in seen_request_ids
                or not isinstance(next_endpoint, str)
                or not next_endpoint
                or next_endpoint == endpoint
                or (
                    expected_labels is not None
                    and (
                        next_endpoint not in expected_labels
                        or next_endpoint in endpoint_map
                    )
                )
            ):
                raise AirflowException("WhoScored proxy endpoint switch is malformed")
            endpoint_map.setdefault(endpoint, []).append(provider_bytes)
            seen_request_ids.add(next_request_id)
            lease["active_endpoint"] = {
                "request_id": next_request_id,
                "endpoint": next_endpoint,
                "start_bytes": lease_total,
            }
            continue
        if event_type == "endpoint_finished":
            active_endpoint = lease["active_endpoint"]
            request_id = event.get("request_id")
            endpoint = event.get("endpoint")
            provider_bytes = event.get("provider_bytes")
            lease_total = event.get("lease_total_bytes")
            if (
                not isinstance(active_endpoint, Mapping)
                or request_id != active_endpoint.get("request_id")
                or endpoint != active_endpoint.get("endpoint")
                or type(provider_bytes) is not int
                or provider_bytes < 0
                or provider_bytes
                != lease["provider_billed_bytes"]
                - int(active_endpoint.get("start_bytes", -1))
                or type(lease_total) is not int
                or lease_total != lease["provider_billed_bytes"]
            ):
                raise AirflowException("WhoScored proxy endpoint finish is malformed")
            endpoint_map = lease["endpoint_provider_bytes"]
            endpoint_map.setdefault(endpoint, []).append(provider_bytes)
            lease["active_endpoint"] = None
            continue
        closed_total = event.get("total_bytes")
        raw_endpoint_map = event.get("endpoint_request_provider_bytes")
        if (
            type(closed_total) is not int
            or closed_total < 0
            or closed_total != lease["provider_billed_bytes"]
            or lease["active_endpoint"] is not None
        ):
            raise AirflowException(
                "WhoScored proxy ledger close differs from byte deltas"
            )
        if raw_endpoint_map is not None:
            if not isinstance(raw_endpoint_map, Mapping):
                raise AirflowException(
                    "WhoScored proxy close endpoint accounting is malformed"
                )
            close_endpoint_map: dict[str, list[int]] = {}
            for endpoint, observations in raw_endpoint_map.items():
                if (
                    not isinstance(endpoint, str)
                    or re.fullmatch(r"(?:bootstrap|target):[0-9a-f]{64}", endpoint)
                    is None
                    or not isinstance(observations, list)
                    or not observations
                    or any(
                        type(amount) is not int or amount < 0 for amount in observations
                    )
                ):
                    raise AirflowException(
                        "WhoScored proxy close endpoint accounting is malformed"
                    )
                close_endpoint_map[endpoint] = list(observations)
            if close_endpoint_map != lease["endpoint_provider_bytes"]:
                raise AirflowException(
                    "WhoScored proxy close differs from endpoint WAL events"
                )
        elif lease["batch_binding"] is not None or lease["endpoint_provider_bytes"]:
            raise AirflowException("WhoScored proxy close has no endpoint accounting")
        lease["closed"] = True

    try:
        snapshot = paid_campaign_gateway_call(approval, "snapshot")
    except WhoScoredProxyRuntimeError:
        try:
            snapshot = paid_campaign_gateway_call(approval, "sealed_snapshot")
        except WhoScoredProxyRuntimeError as sealed_exc:
            raise AirflowException(
                "WhoScored campaign gateway snapshot is unavailable"
            ) from sealed_exc
    allocations = snapshot.get("allocations")
    active_claims = snapshot.get("active_claims")
    if not isinstance(allocations, Mapping) or not isinstance(active_claims, Mapping):
        raise AirflowException("WhoScored campaign ledger snapshot is malformed")
    current_run_hash = hashlib.sha256(str(run_id).encode("utf-8")).hexdigest()
    current_dag_hash = hashlib.sha256(str(dag_id).encode("utf-8")).hexdigest()
    if (
        snapshot.get("campaign_id") != approval.campaign_id
        or snapshot.get("approval_id") != approval.approval_id
        or snapshot.get("approval_sha256") != approval.approval_sha256
        or snapshot.get("run_id_hash") != current_run_hash
        or snapshot.get("dag_id_hash") != current_dag_hash
        or snapshot.get("status") == "revoked"
        or set(allocations) != set(approved_allocations)
    ):
        raise AirflowException(
            "WhoScored campaign ledger snapshot differs from signed authority"
        )
    if active_claims:
        raise AirflowException(
            "WhoScored campaign ledger still has active claims for this DagRun"
        )

    campaign_leases: dict[tuple[str, str, str], Mapping[str, Any]] = {}
    campaign_batch_bindings: dict[tuple[str, str, str], dict[str, Any] | None] = {}
    campaign_lease_bindings: dict[str, tuple[str, str, str]] = {}
    campaign_paid = 0
    campaign_logical_target_units = 0
    for allocation_id, value in allocations.items():
        attempts = value.get("attempts") if isinstance(value, Mapping) else None
        allocation = approved_allocations.get(str(allocation_id))
        allocation_spent = (
            value.get("spent_provider_bytes") if isinstance(value, Mapping) else None
        )
        if (
            allocation is None
            or not isinstance(attempts, list)
            or value.get("allocation_id") != allocation_id
            or value.get("task_id") != allocation.task_id
            or type(allocation_spent) is not int
            or allocation_spent < 0
        ):
            raise AirflowException("WhoScored campaign allocation ledger is malformed")
        allocation_attempt_bytes = 0
        allocation_logical_target_units = 0
        allocation_has_batch = False
        for attempt in attempts:
            if not isinstance(attempt, Mapping):
                raise AirflowException("WhoScored campaign attempt ledger is malformed")
            attempt_hash = attempt.get("attempt_id_hash")
            lease_hash = attempt.get("lease_id_hash")
            billed = attempt.get("provider_billed_bytes")
            canonical_url_hash = attempt.get("canonical_url_sha256")
            if (
                not isinstance(attempt_hash, str)
                or re.fullmatch(r"[0-9a-f]{64}", attempt_hash) is None
                or not isinstance(lease_hash, str)
                or re.fullmatch(r"[0-9a-f]{64}", lease_hash) is None
                or not isinstance(canonical_url_hash, str)
                or re.fullmatch(r"[0-9a-f]{64}", canonical_url_hash) is None
                or type(billed) is not int
                or billed < 0
            ):
                raise AirflowException(
                    "WhoScored campaign attempt accounting is malformed"
                )
            binding_fields = {
                "target_manifest_sha256",
                "logical_target_units",
                "expected_endpoint_labels",
            }
            present_binding_fields = binding_fields & set(attempt)
            batch_binding: dict[str, Any] | None = None
            if present_binding_fields:
                manifest_sha256 = attempt.get("target_manifest_sha256")
                logical_target_units = attempt.get("logical_target_units")
                expected_endpoint_labels = attempt.get("expected_endpoint_labels")
                if (
                    present_binding_fields != binding_fields
                    or not isinstance(manifest_sha256, str)
                    or re.fullmatch(r"[0-9a-f]{64}", manifest_sha256) is None
                    or type(logical_target_units) is not int
                    or not 1 <= logical_target_units <= 8
                    or not isinstance(expected_endpoint_labels, list)
                    or len(expected_endpoint_labels) != logical_target_units + 1
                    or any(
                        not isinstance(label, str)
                        or re.fullmatch(r"(?:bootstrap|target):[0-9a-f]{64}", label)
                        is None
                        for label in expected_endpoint_labels
                    )
                    or not expected_endpoint_labels[0].startswith("bootstrap:")
                    or expected_endpoint_labels[1:]
                    != sorted(expected_endpoint_labels[1:])
                    or any(
                        not label.startswith("target:")
                        for label in expected_endpoint_labels[1:]
                    )
                    or len(set(expected_endpoint_labels))
                    != len(expected_endpoint_labels)
                ):
                    raise AirflowException(
                        "WhoScored campaign batch attempt binding is malformed"
                    )
                batch_binding = {
                    "target_manifest_sha256": manifest_sha256,
                    "logical_target_units": logical_target_units,
                    "expected_endpoint_labels": tuple(expected_endpoint_labels),
                }
            lease_key = (str(allocation_id), attempt_hash, lease_hash)
            previous_binding = campaign_lease_bindings.setdefault(lease_hash, lease_key)
            if previous_binding != lease_key or lease_key in campaign_leases:
                raise AirflowException(
                    "WhoScored campaign lease accounting is duplicated or rebound"
                )
            campaign_leases[lease_key] = attempt
            campaign_batch_bindings[lease_key] = batch_binding
            allocation_has_batch = allocation_has_batch or batch_binding is not None
            allocation_attempt_bytes += billed
            allocation_logical_target_units += (
                batch_binding["logical_target_units"]
                if batch_binding is not None
                else 1
            )
            campaign_paid += billed
        allocation_leases_used = value.get("leases_used")
        if allocation_attempt_bytes != allocation_spent:
            raise AirflowException("WhoScored campaign allocation byte counters differ")
        if (allocation_has_batch and type(allocation_leases_used) is not int) or (
            allocation_leases_used is not None
            and (
                type(allocation_leases_used) is not int
                or allocation_leases_used != allocation_logical_target_units
            )
        ):
            raise AirflowException(
                "WhoScored campaign allocation target counters differ"
            )
        campaign_logical_target_units += allocation_logical_target_units
    snapshot_spent = snapshot.get("spent_provider_bytes")
    snapshot_leases_used = snapshot.get("leases_used")
    if (
        type(snapshot_spent) is not int
        or snapshot_spent < 0
        or snapshot_spent != campaign_paid
    ):
        raise AirflowException("WhoScored campaign total byte counters differ")
    campaign_has_batch = any(
        value is not None for value in campaign_batch_bindings.values()
    )
    if (campaign_has_batch and type(snapshot_leases_used) is not int) or (
        snapshot_leases_used is not None
        and (
            type(snapshot_leases_used) is not int
            or snapshot_leases_used != campaign_logical_target_units
        )
    ):
        raise AirflowException("WhoScored campaign total target counters differ")
    if set(campaign_leases) != set(proxy_leases):
        raise AirflowException(
            "WhoScored campaign/proxy ledger attempt and lease sets differ"
        )
    if set(request_leases) != set(proxy_leases):
        raise AirflowException(
            "WhoScored request/proxy ledger attempt and lease sets differ"
        )
    for lease_key, attempt in campaign_leases.items():
        proxy_lease = proxy_leases[lease_key]
        request_lease = request_leases[lease_key]
        request_batch = request_lease["batch_accounting"]
        proxy_batch = proxy_lease["batch_binding"]
        campaign_batch = campaign_batch_bindings[lease_key]
        if proxy_lease["closed"] is not True:
            raise AirflowException(
                "WhoScored proxy ledger lease lifecycle is incomplete"
            )
        if (
            attempt["canonical_url_sha256"] != proxy_lease["canonical_url_sha256"]
            or attempt["provider_billed_bytes"] != proxy_lease["provider_billed_bytes"]
        ):
            raise AirflowException(
                "WhoScored campaign/proxy ledger lease accounting differs"
            )
        if (
            request_lease["canonical_url_sha256"] != proxy_lease["canonical_url_sha256"]
            or request_lease["provider_billed_bytes"]
            != proxy_lease["provider_billed_bytes"]
        ):
            raise AirflowException(
                "WhoScored request/proxy ledger lease accounting differs"
            )
        if (request_batch is None) != (proxy_batch is None) or (
            proxy_batch is None
        ) != (campaign_batch is None):
            raise AirflowException(
                "WhoScored batch binding is missing from one accounting witness"
            )
        if request_batch is not None:
            assert proxy_batch is not None
            assert campaign_batch is not None
            expected_labels = proxy_batch["expected_endpoint_labels"]
            target_labels = tuple(
                sorted(
                    "target:" + digest
                    for digest in request_batch["endpoint_provider_bytes"]
                )
            )
            bootstrap_labels = tuple(
                label for label in expected_labels if label.startswith("bootstrap:")
            )
            endpoint_observations = proxy_lease["endpoint_provider_bytes"]
            if (
                proxy_batch != campaign_batch
                or request_batch["target_manifest_sha256"]
                != proxy_batch["target_manifest_sha256"]
                or request_batch["logical_target_units"]
                != proxy_batch["logical_target_units"]
                or tuple(expected_labels[1:]) != target_labels
                or len(bootstrap_labels) != 1
                or set(endpoint_observations) != set(expected_labels)
                or any(
                    not isinstance(observations, list) or len(observations) != 1
                    for observations in endpoint_observations.values()
                )
                or endpoint_observations[bootstrap_labels[0]][0]
                != request_batch["bootstrap_provider_bytes"]
                or any(
                    endpoint_observations[label][0]
                    != request_batch["endpoint_provider_bytes"][
                        label.removeprefix("target:")
                    ]
                    for label in target_labels
                )
            ):
                raise AirflowException(
                    "WhoScored batch request/proxy/campaign attribution differs"
                )
    attempt_accounting: list[dict[str, Any]] = []
    for (allocation_id, attempt_hash, lease_hash), attempt in sorted(
        campaign_leases.items()
    ):
        item: dict[str, Any] = {
            "allocation_id": allocation_id,
            "attempt_id_hash": attempt_hash,
            "lease_id_hash": lease_hash,
            "canonical_url_sha256": attempt["canonical_url_sha256"],
            "provider_billed_bytes": attempt["provider_billed_bytes"],
        }
        batch_binding = campaign_batch_bindings[
            (allocation_id, attempt_hash, lease_hash)
        ]
        if batch_binding is not None:
            item.update(
                {
                    "target_manifest_sha256": batch_binding["target_manifest_sha256"],
                    "logical_target_units": batch_binding["logical_target_units"],
                    "expected_endpoint_labels": list(
                        batch_binding["expected_endpoint_labels"]
                    ),
                }
            )
        attempt_accounting.append(item)
    attempt_accounting_sha256 = hashlib.sha256(
        canonical_json_bytes(attempt_accounting)
    ).hexdigest()
    try:
        snapshot = paid_campaign_gateway_call(
            approval,
            "seal_for_reconciliation",
            dag_id=str(dag_id),
            run_id=str(run_id),
            provider_billed_bytes=campaign_paid,
            attempt_accounting_sha256=attempt_accounting_sha256,
        )
    except WhoScoredProxyRuntimeError as exc:
        raise AirflowException(
            f"WhoScored campaign gateway cannot seal reconciliation: {exc}"
        ) from exc
    snapshot_evidence = {
        key: value for key, value in snapshot.items() if key != "updated_at"
    }
    snapshot_sha256 = hashlib.sha256(
        json.dumps(
            snapshot_evidence,
            ensure_ascii=False,
            sort_keys=True,
            separators=(",", ":"),
        ).encode("utf-8")
    ).hexdigest()
    if reconciliation_evidence is not None:
        reconciliation_evidence["campaign_snapshot_sha256"] = snapshot_sha256
    return campaign_paid


def _proxy_events_sha256(events: Sequence[Mapping[str, Any]]) -> str:
    return hashlib.sha256(
        json.dumps(
            [dict(event) for event in events],
            ensure_ascii=False,
            sort_keys=True,
            separators=(",", ":"),
        ).encode("utf-8")
    ).hexdigest()


def _traffic_reconciliation_context(
    transport: PaidRuntime,
) -> dict[str, Any]:
    runtime = _WHOSCORED_RUNTIME_CONTRACT.validate_runtime_contract()
    approval = getattr(transport, "approval", None)
    policy = str(
        getattr(
            transport,
            "policy",
            "direct_then_paid" if transport.is_paid else "direct_only",
        )
    )
    return {
        "contract": TRAFFIC_RECONCILIATION_CONTRACT,
        "runtime_manifest_sha256": runtime["manifest_sha256"],
        "runtime_code_tree_sha256": runtime["code_tree_sha256"],
        "transport_policy": policy,
        "campaign_id": getattr(approval, "campaign_id", None),
        "approval_id": getattr(approval, "approval_id", None),
        "approval_sha256": getattr(approval, "approval_sha256", None),
    }


def _current_campaign_snapshot_sha256(transport: PaidRuntime) -> Optional[str]:
    approval = getattr(transport, "approval", None)
    if not transport.is_paid or approval is None:
        return None
    try:
        snapshot = paid_campaign_gateway_call(approval, "sealed_snapshot")
    except WhoScoredProxyRuntimeError as exc:
        raise AirflowException(
            f"WhoScored sealed campaign gateway snapshot is unavailable: {exc}"
        ) from exc
    evidence = {key: value for key, value in snapshot.items() if key != "updated_at"}
    return hashlib.sha256(
        json.dumps(
            evidence,
            ensure_ascii=False,
            sort_keys=True,
            separators=(",", ":"),
        ).encode("utf-8")
    ).hexdigest()


def _sql_string(value: str) -> str:
    return "'" + str(value).replace("'", "''") + "'"


def plan_daily_profile_capacity(
    *,
    scope_plan: Optional[Mapping[str, Any]] = None,
    **_context: Any,
) -> Dict[str, Any]:
    """Freeze the exact due backlog or fail before any source request."""

    from scrapers.whoscored.repository import (
        PROFILE_REFRESH_DAYS,
        ProfileCandidateCapacityExceeded,
    )
    from scrapers.whoscored.profile_policy import daily_profile_candidate_hard_cap

    try:
        maximum_limit = daily_profile_candidate_hard_cap()
    except ValueError as exc:
        raise AirflowException("invalid WhoScored daily profile capacity") from exc
    # Profile HTML uses the hard 30 requests/minute limiter. 3,000 pages
    # consume the entire 100-minute source budget and leave 20 minutes for
    # parsing/commit inside the two-hour task timeout.
    if scope_plan is None:
        raise AirflowException("daily profile capacity requires a frozen scope plan")
    batch_id, scope_specs = _daily_scope_plan_specs(scope_plan)
    from dags.scripts import run_whoscored_scraper as runner

    repository = runner._new_repository()
    catalog = repository.load_discovered_catalog(batch_id=batch_id)
    requested = [runner.RunnerScope.parse(spec) for spec in scope_specs]
    selected = runner._select_catalog_snapshot_scopes(
        catalog,
        requested,
        active_only=True,
    )
    try:
        frozen = repository.profile_candidate_snapshot(
            scopes=[getattr(value, "scope", value) for _scope, value in selected],
            hard_cap=maximum_limit,
        )
    except ProfileCandidateCapacityExceeded as exc:
        raise AirflowException(
            "WhoScored profile repair backlog cannot fit one bounded daily run: "
            f"count={exc.count}, hard_cap={exc.hard_cap}"
        ) from exc
    candidate_count = frozen.count
    candidate_sha256 = frozen.payload_sha256
    if (
        type(candidate_count) is not int
        or not 0 <= candidate_count <= maximum_limit
        or re.fullmatch(r"[0-9a-f]{64}", str(candidate_sha256 or "")) is None
    ):
        raise AirflowException("invalid exact WhoScored profile candidate snapshot")
    if scope_plan.get("schema_version") == 3 and (
        candidate_count != scope_plan.get("profile_target_count")
        or candidate_sha256 != scope_plan.get("profile_targets_sha256")
    ):
        raise AirflowException(
            "signed WhoScored profile target identity drifted after discovery"
        )
    summary = {
        "schema_version": 1,
        "status": "success",
        "catalog_batch_id": batch_id,
        "refresh_days": PROFILE_REFRESH_DAYS,
        "candidate_count": candidate_count,
        "candidate_payload_sha256": str(candidate_sha256),
        "selected_limit": candidate_count,
        "maximum_limit": maximum_limit,
        "theoretical_roster_capacity": maximum_limit * PROFILE_REFRESH_DAYS,
    }
    return summary


def _daily_profile_capacity_specs(
    capacity: Mapping[str, Any],
    *,
    expected_catalog_batch_id: str,
) -> tuple[int, str]:
    """Validate the planner XCom without accepting a partial identity."""

    from scrapers.whoscored.repository import PROFILE_REFRESH_DAYS

    if not isinstance(capacity, Mapping) or set(capacity) != {
        "schema_version",
        "status",
        "catalog_batch_id",
        "refresh_days",
        "candidate_count",
        "candidate_payload_sha256",
        "selected_limit",
        "maximum_limit",
        "theoretical_roster_capacity",
    }:
        raise AirflowException("invalid frozen WhoScored profile capacity schema")
    count = capacity.get("candidate_count")
    selected_limit = capacity.get("selected_limit")
    maximum_limit = capacity.get("maximum_limit")
    refresh_days = capacity.get("refresh_days")
    theoretical_capacity = capacity.get("theoretical_roster_capacity")
    payload_sha256 = str(capacity.get("candidate_payload_sha256") or "")
    if (
        type(capacity.get("schema_version")) is not int
        or capacity.get("schema_version") != 1
        or capacity.get("status") != "success"
        or capacity.get("catalog_batch_id") != expected_catalog_batch_id
        or type(refresh_days) is not int
        or refresh_days != PROFILE_REFRESH_DAYS
        or type(count) is not int
        or type(selected_limit) is not int
        or count != selected_limit
        or not 0 <= count <= 3_000
        or type(maximum_limit) is not int
        or not count <= maximum_limit <= 3_000
        or type(theoretical_capacity) is not int
        or theoretical_capacity != maximum_limit * PROFILE_REFRESH_DAYS
        or re.fullmatch(r"[0-9a-f]{64}", payload_sha256) is None
    ):
        raise AirflowException("invalid frozen WhoScored profile capacity identity")
    return count, payload_sha256


def _expected_feed_state_keys(stage_ids: Sequence[int]) -> frozenset[str]:
    """Build the exact source-feed contract from the current parser catalogs."""
    # Lazy imports preserve the DAG's parse-time isolation from scraper runtime
    # dependencies while keeping DQ aligned with the fetch catalogs.
    from scrapers.whoscored.service import (
        PLAYER_DETAILED_STAT_TABS,
        PLAYER_STAGE_STAT_TABS,
        TEAM_DETAILED_STAT_TABS,
        TEAM_STAGE_STAT_TABS,
    )
    from scrapers.whoscored.stage_feeds import STAGE_TEAM_FEED_CATALOG

    suffixes = {
        *(
            f"team:{category}:{subcategory}"
            for category, subcategory, *_ in TEAM_STAGE_STAT_TABS
        ),
        *(
            f"team-detailed:{spec.category}:{spec.subcategory}"
            for spec in TEAM_DETAILED_STAT_TABS
        ),
        *(
            f"player:{category}:{subcategory}"
            for category, subcategory, _inc_pens in PLAYER_STAGE_STAT_TABS
        ),
        *(
            f"player-detailed:{spec.category}:{spec.subcategory}"
            for spec in PLAYER_DETAILED_STAT_TABS
        ),
        *(f"stagestatfeed:{spec.type_id}" for spec in STAGE_TEAM_FEED_CATALOG),
        "referee:summary",
    }
    if len(suffixes) != EXPECTED_FEEDS_PER_STAGE:
        raise AirflowException(
            "WhoScored source feed catalog drifted without a DQ contract update: "
            f"expected {EXPECTED_FEEDS_PER_STAGE}, got {len(suffixes)}"
        )

    normalized_stages: set[int] = set()
    for raw_stage_id in stage_ids:
        try:
            stage_id = int(raw_stage_id)
        except (TypeError, ValueError) as exc:
            raise AirflowException(
                f"WhoScored catalog contains invalid stage id {raw_stage_id!r}"
            ) from exc
        if stage_id <= 0:
            raise AirflowException(
                f"WhoScored catalog contains invalid stage id {raw_stage_id!r}"
            )
        normalized_stages.add(stage_id)
    return frozenset(
        f"{stage_id}:{suffix}" for stage_id in normalized_stages for suffix in suffixes
    )


def _strict_json_object_pairs(pairs: Sequence[tuple[str, Any]]) -> dict[str, Any]:
    value: dict[str, Any] = {}
    for key, item in pairs:
        if key in value:
            raise ValueError(f"duplicate JSON key {key!r}")
        value[key] = item
    return value


def _feed_state_integrity_summary(
    dataset_states_json: Any,
    stage_ids: Sequence[int],
) -> Dict[str, int]:
    """Compare the manifest feed map with the exact per-stage source contract."""
    expected = _expected_feed_state_keys(stage_ids)
    stage_count = len(expected) // EXPECTED_FEEDS_PER_STAGE
    feeds: Mapping[str, Any] = {}
    malformed = 0
    if not isinstance(dataset_states_json, str) or not dataset_states_json.strip():
        malformed = 1
    else:
        try:
            states = json.loads(
                dataset_states_json,
                object_pairs_hook=_strict_json_object_pairs,
            )
        except (TypeError, ValueError):
            states = None
            malformed = 1
        if isinstance(states, Mapping):
            candidate = states.get("__feeds__")
            if isinstance(candidate, Mapping):
                feeds = candidate
            else:
                malformed = 1
        elif states is not None:
            malformed = 1

    allowed_statuses = {"available", "empty", "not_available"}
    malformed += sum(
        not isinstance(key, str)
        or not isinstance(status, str)
        or status not in allowed_statuses
        for key, status in feeds.items()
    )
    actual = frozenset(key for key in feeds if isinstance(key, str))
    return {
        "feed_state_stage_count": stage_count,
        "expected_feed_state_count": len(expected),
        "actual_feed_state_count": len(actual),
        "missing_feed_state_count": len(expected - actual),
        "extra_feed_state_count": len(actual - expected),
        "malformed_feed_state_count": int(malformed),
        "unavailable_feed_count": sum(
            key in expected and status == "not_available"
            for key, status in feeds.items()
        ),
    }


def _producer_commits_from_report(
    result: Mapping[str, Any],
) -> Dict[str, tuple[str, ...]]:
    """Read the complete, versioned producer commit projection fail-closed."""

    raw = result.get("producer_commits")
    expected_fields = {"schema_version", *PRODUCER_COMMIT_PATTERNS}
    if (
        not isinstance(raw, Mapping)
        or set(raw) != expected_fields
        or type(raw.get("schema_version")) is not int
        or raw.get("schema_version") != 1
    ):
        raise AirflowException("WhoScored producer commit projection is invalid")

    commits: Dict[str, tuple[str, ...]] = {}
    for kind, pattern in PRODUCER_COMMIT_PATTERNS.items():
        values = raw.get(kind)
        if (
            not isinstance(values, list)
            or any(
                not isinstance(value, str) or pattern.fullmatch(value) is None
                for value in values
            )
            or len(values) != len(set(values))
        ):
            raise AirflowException(
                f"WhoScored producer {kind} commit identities are invalid"
            )
        commits[kind] = tuple(values)
    return commits


def _producer_attempts_from_report(
    result: Mapping[str, Any],
) -> Dict[str, tuple[Mapping[str, Any], ...]]:
    raw = result.get("producer_attempts")
    if (
        not isinstance(raw, Mapping)
        or set(raw) != {"schema_version", *PRODUCER_ATTEMPT_KINDS}
        or type(raw.get("schema_version")) is not int
        or raw.get("schema_version") != 1
    ):
        raise AirflowException("WhoScored producer attempted projection is invalid")
    attempts: Dict[str, tuple[Mapping[str, Any], ...]] = {}
    for kind in PRODUCER_ATTEMPT_KINDS:
        records = raw.get(kind)
        if not isinstance(records, list):
            raise AirflowException(
                f"WhoScored producer {kind} attempted projection is invalid"
            )
        normalized = []
        for record in records:
            if (
                not isinstance(record, Mapping)
                or set(record) != {"schema_version", "scope", "count", "payload_sha256"}
                or type(record.get("schema_version")) is not int
                or record.get("schema_version") != 1
                or not isinstance(record.get("scope"), str)
                or not record.get("scope")
                or type(record.get("count")) is not int
                or record["count"] < 0
                or not isinstance(record.get("payload_sha256"), str)
                or re.fullmatch(r"[0-9a-f]{64}", record["payload_sha256"]) is None
            ):
                raise AirflowException(
                    f"WhoScored producer {kind} attempted snapshot is invalid"
                )
            normalized.append(dict(record))
        attempts[kind] = tuple(normalized)
    return attempts


def _exact_physical_counts(
    cursor: Any,
    *,
    dataset_tables: Mapping[str, str],
    batch_column: str,
    batch_ids: Sequence[str],
    scope_filter: str = "",
) -> Dict[tuple[str, str], int]:
    if not batch_ids:
        return {}
    values = ",".join(_sql_string(value) for value in batch_ids)
    clauses = []
    for dataset, table in dataset_tables.items():
        clauses.append(
            "SELECT "
            f"{_sql_string(dataset)} AS dataset, {batch_column} AS batch_id, "
            f"COUNT(*) AS rows_count FROM iceberg.bronze.{table} "
            f"WHERE {batch_column} IN ({values}){scope_filter} "
            f"GROUP BY {batch_column}"
        )
    cursor.execute(" UNION ALL ".join(clauses))
    return {
        (str(dataset), str(batch_id)): int(rows_count or 0)
        for dataset, batch_id, rows_count in cursor.fetchall()
    }


def _exact_dataset_mismatches(
    rows: Sequence[Sequence[Any]],
    *,
    dataset_names: Sequence[str],
    physical: Mapping[tuple[str, str], int],
) -> tuple[int, int]:
    expected_names = set(dataset_names)
    expected_by_batch: Dict[str, Dict[str, int]] = {}
    mismatches = 0
    for row in rows:
        batch_id = str(row[0])
        try:
            raw_counts = json.loads(str(row[1]))
        except (TypeError, ValueError, json.JSONDecodeError):
            raw_counts = None
        if (
            batch_id in expected_by_batch
            or not isinstance(raw_counts, Mapping)
            or set(raw_counts) != expected_names
            or any(type(value) is not int or value < 0 for value in raw_counts.values())
        ):
            mismatches += 1
            continue
        expected_by_batch[batch_id] = {
            str(name): int(value) for name, value in raw_counts.items()
        }

    for batch_id, expected in expected_by_batch.items():
        mismatches += sum(
            physical.get((dataset, batch_id), 0) != rows_count
            for dataset, rows_count in expected.items()
        )
    return len(rows), mismatches


def _scope_producer_integrity_summary(
    scope_spec: str,
    commits: Mapping[str, Sequence[str]],
) -> Dict[str, Any]:
    """Validate only the immutable batches named by this producer report."""

    competition_id, separator, season_id = scope_spec.rpartition("=")
    if not separator or not competition_id or not season_id:
        raise AirflowException(f"invalid persisted scope {scope_spec!r}")

    from scrapers.whoscored.parsers import (
        MATCH_AVAILABILITY_VERSION,
        PARSER_VERSION,
    )
    from scrapers.whoscored.repository import (
        MATCH_DATASET_TABLES,
        PREVIEW_DATASET_TABLES,
        entity_id_payload_sha256,
    )
    from scrapers.base.trino_manager import get_trino_connection

    league_sql = _sql_string(competition_id)
    season_sql = _sql_string(season_id)
    scope_filter = f" AND league = {league_sql} AND season = {season_sql}"
    metrics = {
        "expected_scope_batches": len(commits["scope"]),
        "exact_scope_manifests": 0,
        "exact_scope_dataset_mismatches": 0,
        "expected_match_batches": len(commits["match"]),
        "exact_match_manifests": 0,
        "exact_match_dataset_mismatches": 0,
        "expected_match_not_available_batches": len(commits["match_not_available"]),
        "exact_match_not_available_manifests": 0,
        "exact_match_not_available_physical_rows": 0,
        "exact_match_outcome_count": 0,
        "exact_match_outcome_duplicates": 0,
        "exact_match_outcome_payload_sha256": "",
        "expected_preview_batches": len(commits["preview"]),
        "exact_preview_manifests": 0,
        "exact_preview_dataset_mismatches": 0,
        "expected_preview_not_available_batches": len(commits["preview_not_available"]),
        "exact_preview_not_available_manifests": 0,
        "exact_preview_not_available_physical_rows": 0,
        "exact_preview_outcome_count": 0,
        "exact_preview_outcome_duplicates": 0,
        "exact_preview_outcome_payload_sha256": "",
    }
    outcome_entity_ids: Dict[str, list[int]] = {"match": [], "preview": []}

    conn = get_trino_connection()
    try:
        cur = conn.cursor()
        try:
            configurations = (
                (
                    "scope",
                    "whoscored_scope_ingest_manifest",
                    "batch_id",
                    "COALESCE(completed_at, _ingested_at)",
                    {table: table for table in SCOPE_PARITY_TABLES},
                    "_scope_batch_id",
                    " AND entity_group = 'season'",
                    " AND payload_sha256 IS NOT NULL AND raw_uris_json IS NOT NULL",
                ),
                (
                    "match",
                    "whoscored_match_ingest_manifest",
                    "batch_id",
                    "COALESCE(completed_at, fetched_at, _ingested_at)",
                    MATCH_DATASET_TABLES,
                    "_game_batch_id",
                    "",
                    " AND payload_sha256 IS NOT NULL AND raw_uri IS NOT NULL",
                ),
                (
                    "preview",
                    "whoscored_preview_ingest_manifest",
                    "batch_id",
                    "COALESCE(completed_at, fetched_at, _ingested_at)",
                    PREVIEW_DATASET_TABLES,
                    "_preview_batch_id",
                    "",
                    " AND payload_sha256 IS NOT NULL AND raw_uri IS NOT NULL",
                ),
            )
            for (
                kind,
                manifest_table,
                manifest_batch_column,
                observation_order,
                dataset_tables,
                physical_batch_column,
                manifest_extra_filter,
                success_filter,
            ) in configurations:
                batch_ids = tuple(commits[kind])
                if batch_ids:
                    values = ",".join(_sql_string(value) for value in batch_ids)
                    natural_projection = (
                        ", game_id" if kind in outcome_entity_ids else ""
                    )
                    cur.execute(
                        f"SELECT batch_id, entity_counts_json{natural_projection} FROM ("
                        f"SELECT m.*, {manifest_batch_column} AS exact_batch_id, "
                        "ROW_NUMBER() OVER (PARTITION BY "
                        f"{manifest_batch_column} ORDER BY {observation_order} DESC, "
                        "_batch_id DESC) AS exact_rank "
                        f"FROM iceberg.bronze.{manifest_table} m "
                        f"WHERE league = {league_sql} AND season = {season_sql} "
                        f"AND {manifest_batch_column} IN ({values})"
                        f"{manifest_extra_filter}"
                        ") WHERE exact_rank = 1 AND state = 'success' "
                        f"AND parser_version = {_sql_string(PARSER_VERSION)}"
                        f"{success_filter}"
                    )
                    manifest_rows = cur.fetchall()
                    physical = _exact_physical_counts(
                        cur,
                        dataset_tables=dataset_tables,
                        batch_column=physical_batch_column,
                        batch_ids=batch_ids,
                        scope_filter=scope_filter,
                    )
                    manifests, mismatches = _exact_dataset_mismatches(
                        manifest_rows,
                        dataset_names=tuple(dataset_tables),
                        physical=physical,
                    )
                    metrics[f"exact_{kind}_manifests"] = manifests
                    metrics[f"exact_{kind}_dataset_mismatches"] = mismatches
                    if kind in outcome_entity_ids:
                        outcome_entity_ids[kind].extend(
                            int(row[2]) for row in manifest_rows
                        )

                if kind not in outcome_entity_ids:
                    continue
                not_available_ids = tuple(commits[f"{kind}_not_available"])
                if not not_available_ids:
                    continue
                not_available_values = ",".join(
                    _sql_string(value) for value in not_available_ids
                )
                cur.execute(
                    "SELECT batch_id, game_id FROM ("
                    f"SELECT m.*, ROW_NUMBER() OVER (PARTITION BY {manifest_batch_column} "
                    f"ORDER BY {observation_order} DESC, _batch_id DESC) AS exact_rank "
                    f"FROM iceberg.bronze.{manifest_table} m "
                    f"WHERE league = {league_sql} AND season = {season_sql} "
                    f"AND {manifest_batch_column} IN ({not_available_values})"
                    ") WHERE exact_rank = 1 AND state = 'not_available' "
                    f"AND parser_version = {_sql_string(PARSER_VERSION)} "
                    "AND availability_version = "
                    f"{_sql_string(MATCH_AVAILABILITY_VERSION)} "
                    "AND failure_code IS NOT NULL "
                    "AND (raw_uri IS NOT NULL OR http_status IN (404, 410))"
                )
                not_available_rows = cur.fetchall()
                metrics[f"exact_{kind}_not_available_manifests"] = len(
                    not_available_rows
                )
                outcome_entity_ids[kind].extend(
                    int(row[1]) for row in not_available_rows
                )
                not_available_physical = _exact_physical_counts(
                    cur,
                    dataset_tables=dataset_tables,
                    batch_column=physical_batch_column,
                    batch_ids=not_available_ids,
                    scope_filter=scope_filter,
                )
                metrics[f"exact_{kind}_not_available_physical_rows"] = sum(
                    not_available_physical.values()
                )
        finally:
            cur.close()
    finally:
        conn.close()
    for kind, entity_ids in outcome_entity_ids.items():
        metrics[f"exact_{kind}_outcome_count"] = len(entity_ids)
        duplicate_count = len(entity_ids) - len(set(entity_ids))
        metrics[f"exact_{kind}_outcome_duplicates"] = duplicate_count
        if not duplicate_count:
            metrics[f"exact_{kind}_outcome_payload_sha256"] = entity_id_payload_sha256(
                entity_ids
            )
    return metrics


def _profile_producer_integrity_summary(
    commits: Mapping[str, Sequence[str]],
) -> Dict[str, Any]:
    """Validate exact profile content batches, independent of current views."""

    from scrapers.whoscored.parsers import (
        MATCH_AVAILABILITY_VERSION,
        PARSER_VERSION,
    )
    from scrapers.whoscored.repository import entity_id_payload_sha256
    from scrapers.base.trino_manager import get_trino_connection

    batch_ids = tuple(commits["profile"])
    not_available_ids = tuple(commits["profile_not_available"])
    metrics = {
        "expected_profile_batches": len(batch_ids),
        "exact_profile_manifests": 0,
        "exact_profile_row_mismatches": 0,
        "exact_profile_participation_mismatches": 0,
        "expected_profile_not_available_batches": len(not_available_ids),
        "exact_profile_not_available_manifests": 0,
        "exact_profile_not_available_physical_rows": 0,
        "exact_profile_outcome_count": 0,
        "exact_profile_outcome_duplicates": 0,
        "exact_profile_outcome_payload_sha256": entity_id_payload_sha256(()),
    }
    if not batch_ids and not not_available_ids:
        return metrics

    conn = get_trino_connection()
    try:
        cur = conn.cursor()
        try:
            manifest_rows = []
            physical: Dict[tuple[str, str], int] = {}
            if batch_ids:
                values = ",".join(_sql_string(value) for value in batch_ids)
                cur.execute(
                    "SELECT _profile_batch_id, participations_count, player_id FROM ("
                    "SELECT m.*, ROW_NUMBER() OVER (PARTITION BY _profile_batch_id "
                    "ORDER BY COALESCE(completed_at, fetched_at, _ingested_at) DESC, "
                    "_batch_id DESC) AS exact_rank "
                    "FROM iceberg.bronze.whoscored_profile_ingest_manifest m "
                    f"WHERE _profile_batch_id IN ({values})"
                    ") WHERE exact_rank = 1 AND state = 'success' "
                    f"AND parser_version = {_sql_string(PARSER_VERSION)} "
                    "AND payload_sha256 IS NOT NULL AND raw_uri IS NOT NULL"
                )
                manifest_rows = cur.fetchall()
                physical = _exact_physical_counts(
                    cur,
                    dataset_tables={
                        "profile": "whoscored_player_profile_versions",
                        "participation": "whoscored_player_stage_participations",
                    },
                    batch_column="_profile_batch_id",
                    batch_ids=batch_ids,
                )
            not_available_rows = []
            not_available_physical: Dict[tuple[str, str], int] = {}
            if not_available_ids:
                values = ",".join(_sql_string(value) for value in not_available_ids)
                cur.execute(
                    "SELECT _profile_batch_id, player_id FROM ("
                    "SELECT m.*, ROW_NUMBER() OVER (PARTITION BY _profile_batch_id "
                    "ORDER BY COALESCE(completed_at, fetched_at, _ingested_at) DESC, "
                    "_batch_id DESC) AS exact_rank "
                    "FROM iceberg.bronze.whoscored_profile_ingest_manifest m "
                    f"WHERE _profile_batch_id IN ({values})"
                    ") WHERE exact_rank = 1 AND state = 'not_available' "
                    f"AND parser_version = {_sql_string(PARSER_VERSION)} "
                    "AND availability_version = "
                    f"{_sql_string(MATCH_AVAILABILITY_VERSION)} "
                    "AND failure_code IS NOT NULL "
                    "AND (raw_uri IS NOT NULL OR http_status IN (404, 410))"
                )
                not_available_rows = cur.fetchall()
                not_available_physical = _exact_physical_counts(
                    cur,
                    dataset_tables={
                        "profile": "whoscored_player_profile_versions",
                        "participation": "whoscored_player_stage_participations",
                    },
                    batch_column="_profile_batch_id",
                    batch_ids=not_available_ids,
                )
        finally:
            cur.close()
    finally:
        conn.close()

    expected: Dict[str, int] = {}
    malformed = 0
    player_ids = []
    for batch_id, participations_count, player_id in manifest_rows:
        normalized = str(batch_id)
        if (
            normalized in expected
            or type(participations_count) is not int
            or participations_count < 0
        ):
            malformed += 1
            continue
        expected[normalized] = int(participations_count)
        player_ids.append(int(player_id))
    metrics["exact_profile_manifests"] = len(manifest_rows)
    metrics["exact_profile_row_mismatches"] = malformed + sum(
        physical.get(("profile", batch_id), 0) != 1 for batch_id in expected
    )
    metrics["exact_profile_participation_mismatches"] = sum(
        physical.get(("participation", batch_id), 0) != count
        for batch_id, count in expected.items()
    )
    metrics["exact_profile_not_available_manifests"] = len(not_available_rows)
    metrics["exact_profile_not_available_physical_rows"] = sum(
        not_available_physical.values()
    )
    player_ids.extend(int(row[1]) for row in not_available_rows)
    metrics["exact_profile_outcome_count"] = len(player_ids)
    duplicate_count = len(player_ids) - len(set(player_ids))
    metrics["exact_profile_outcome_duplicates"] = duplicate_count
    if not duplicate_count:
        metrics["exact_profile_outcome_payload_sha256"] = entity_id_payload_sha256(
            player_ids
        )
    return metrics


def _scope_integrity_summary(scope_spec: str) -> Dict[str, int]:
    competition_id, separator, season_id = scope_spec.rpartition("=")
    if not separator or not competition_id or not season_id:
        raise AirflowException(f"invalid persisted scope {scope_spec!r}")

    from scrapers.whoscored.parsers import MATCH_AVAILABILITY_VERSION, PARSER_VERSION
    from scrapers.base.trino_manager import get_trino_connection

    league_sql = _sql_string(competition_id)
    season_sql = _sql_string(season_id)
    conn = get_trino_connection()
    try:
        cur = conn.cursor()
        try:
            cur.execute(
                f"""
                WITH latest_match AS (
                    SELECT * FROM (
                        SELECT m.*, ROW_NUMBER() OVER (
                            PARTITION BY league, season, game_id
                            ORDER BY COALESCE(completed_at, fetched_at, _ingested_at) DESC,
                                     batch_id DESC
                        ) AS rn
                        FROM iceberg.bronze.whoscored_match_ingest_manifest m
                        WHERE league = {league_sql} AND season = {season_sql}
                          AND batch_id LIKE 'ws2-%' AND raw_uri IS NOT NULL
                          AND parser_version = {_sql_string(PARSER_VERSION)}
                    ) WHERE rn = 1 AND state = 'success'
                ),
                latest_covered_match AS (
                    SELECT CAST(game_id AS BIGINT) AS game_id
                    FROM iceberg.bronze.whoscored_match_ingest_latest
                    WHERE league = {league_sql} AND season = {season_sql}
                      AND parser_version = {_sql_string(PARSER_VERSION)}
                      AND (
                          (
                              state = 'success'
                              AND batch_id LIKE 'ws2-%'
                              AND raw_uri IS NOT NULL
                              AND payload_sha256 IS NOT NULL
                          ) OR (
                              state = 'not_available'
                              AND availability_version =
                                  {_sql_string(MATCH_AVAILABILITY_VERSION)}
                              AND failure_code IS NOT NULL
                              AND (
                                  raw_uri IS NOT NULL
                                  OR http_status IN (404, 410)
                              )
                          )
                      )
                ),
                schedule AS (
                    SELECT * FROM (
                        SELECT s.*, ROW_NUMBER() OVER (
                            PARTITION BY league, season, game_id
                            ORDER BY _ingested_at DESC
                        ) AS rn
                        FROM iceberg.bronze.whoscored_schedule_current s
                        WHERE league = {league_sql} AND season = {season_sql}
                    ) WHERE rn = 1
                ),
                completed AS (
                    SELECT * FROM schedule
                    WHERE date >= CAST(
                        CURRENT_TIMESTAMP - INTERVAL '7' DAY AS TIMESTAMP
                    ) AND (
                        status = 6 OR (
                            status = 1 AND home_score IS NOT NULL
                            AND away_score IS NOT NULL
                            AND date <= CAST(
                                CURRENT_TIMESTAMP - INTERVAL '3' HOUR AS TIMESTAMP
                            )
                        )
                    )
                ),
                events_by_game AS (
                    SELECT game_id, COUNT(*) AS rows_count,
                           MAX(COALESCE(expanded_minute, minute)) AS max_minute
                    FROM iceberg.bronze.whoscored_events_current
                    WHERE league = {league_sql} AND season = {season_sql}
                    GROUP BY game_id
                ),
                lineups_by_game AS (
                    SELECT game_id, COUNT(*) AS rows_count
                    FROM iceberg.bronze.whoscored_lineups_current
                    WHERE league = {league_sql} AND season = {season_sql}
                    GROUP BY game_id
                ),
                incidents_by_game AS (
                    SELECT game_id, COUNT(*) AS rows_count
                    FROM iceberg.bronze.whoscored_match_incidents_current
                    WHERE league = {league_sql} AND season = {season_sql}
                    GROUP BY game_id
                ),
                bets_by_game AS (
                    SELECT game_id, COUNT(*) AS rows_count
                    FROM iceberg.bronze.whoscored_match_bets_current
                    WHERE league = {league_sql} AND season = {season_sql}
                    GROUP BY game_id
                ),
                schedule_counts AS (
                    SELECT COUNT(*) AS physical_rows, COUNT(DISTINCT game_id) AS games
                    FROM schedule
                )
                SELECT
                    (SELECT physical_rows FROM schedule_counts),
                    (SELECT games FROM schedule_counts),
                    (SELECT COUNT(*) FROM latest_match),
                    (SELECT COALESCE(SUM(events_count), 0) FROM latest_match),
                    (SELECT COUNT(*) FROM iceberg.bronze.whoscored_events_current
                     WHERE league = {league_sql} AND season = {season_sql}
                       AND _game_batch_id IN (
                           SELECT batch_id FROM latest_match
                       )),
                    (SELECT COALESCE(SUM(lineups_count), 0) FROM latest_match),
                    (SELECT COUNT(*) FROM iceberg.bronze.whoscored_lineups_current
                     WHERE league = {league_sql} AND season = {season_sql}
                       AND _game_batch_id IN (
                           SELECT batch_id FROM latest_match
                       )),
                    (SELECT COUNT(*) FROM completed),
                    (SELECT COUNT(*) FROM completed c
                     LEFT JOIN latest_covered_match m
                     ON m.game_id = CAST(c.game_id AS BIGINT) WHERE m.game_id IS NULL),
                    (SELECT COUNT(*) FROM latest_match m
                     LEFT JOIN events_by_game e ON e.game_id = m.game_id
                     WHERE COALESCE(e.rows_count, 0) <> m.events_count),
                    (SELECT COUNT(*) FROM latest_match m
                     LEFT JOIN lineups_by_game l ON l.game_id = m.game_id
                     WHERE COALESCE(l.rows_count, 0) <> m.lineups_count),
                    (SELECT COUNT(*)
                     FROM completed c
                     JOIN latest_match m
                       ON m.game_id = CAST(c.game_id AS BIGINT)
                     LEFT JOIN events_by_game e ON e.game_id = m.game_id
                     LEFT JOIN iceberg.bronze.whoscored_matches_current h
                       ON h.league = m.league AND h.season = m.season
                      AND CAST(h.game_id AS BIGINT) = m.game_id
                      AND h._game_batch_id LIKE 'ws2-%'
                     WHERE m.is_opta = TRUE AND (
                         COALESCE(json_extract_scalar(
                             json_parse(m.dataset_statuses_json), '$.events'
                         ), '') <> 'available'
                         OR COALESCE(e.rows_count, 0) < GREATEST(
                             20, COALESCE(h.expanded_max_minute, 90)
                         )
                         OR COALESCE(e.max_minute, -1) < GREATEST(
                             1, COALESCE(h.expanded_max_minute, 90) - 15
                         )
                     )),
                    (SELECT COUNT(*) FROM schedule s
                     LEFT JOIN incidents_by_game i
                       ON i.game_id = CAST(s.game_id AS BIGINT)
                    WHERE s.has_incidents_summary = TRUE
                       AND COALESCE(i.rows_count, 0) = 0),
                    (SELECT COUNT(*) FROM schedule s
                     LEFT JOIN bets_by_game b
                       ON b.game_id = CAST(s.game_id AS BIGINT)
                     WHERE COALESCE(
                         json_size(json_parse(s.bets), '$'), 0
                     ) > 0 AND COALESCE(b.rows_count, 0) = 0),
                    (SELECT COUNT(*) FROM latest_match
                     WHERE dataset_statuses_json IS NULL
                        OR COALESCE(json_size(
                            json_parse(dataset_statuses_json), '$'
                        ), -1) <> 7
                        OR COALESCE(json_extract_scalar(
                            json_parse(dataset_statuses_json), '$.matches'
                        ), '') NOT IN ('available', 'empty', 'not_available')
                        OR COALESCE(json_extract_scalar(
                            json_parse(dataset_statuses_json), '$.events'
                        ), '') NOT IN ('available', 'empty', 'not_available')
                        OR COALESCE(json_extract_scalar(
                            json_parse(dataset_statuses_json), '$.lineups'
                        ), '') NOT IN ('available', 'empty', 'not_available')
                        OR COALESCE(json_extract_scalar(
                            json_parse(dataset_statuses_json), '$.substitutions'
                        ), '') NOT IN ('available', 'empty', 'not_available')
                        OR COALESCE(json_extract_scalar(
                            json_parse(dataset_statuses_json), '$.formations'
                        ), '') NOT IN ('available', 'empty', 'not_available')
                        OR COALESCE(json_extract_scalar(
                            json_parse(dataset_statuses_json), '$.team_match_stats'
                        ), '') NOT IN ('available', 'empty', 'not_available')
                        OR COALESCE(json_extract_scalar(
                            json_parse(dataset_statuses_json), '$.player_match_stats'
                        ), '') NOT IN ('available', 'empty', 'not_available')),
                    (SELECT COUNT(*)
                     FROM iceberg.bronze.whoscored_events_current e
                     WHERE e.league = {league_sql} AND e.season = {season_sql}
                       AND e._game_batch_id IN (
                           SELECT batch_id FROM latest_match
                       )
                       AND (
                           e.source_event_id IS NULL OR e.source_event_id <= 0
                           OR e.team_event_id IS NULL OR e.team_event_id <= 0
                           OR TRY_CAST(TRY_CAST(TRY(json_extract_scalar(
                               e.source_raw_json, '$.id'
                           )) AS DOUBLE) AS BIGINT) IS DISTINCT FROM COALESCE(
                               e.opta_event_id, e.source_event_id
                           )
                           OR TRY_CAST(TRY_CAST(TRY(json_extract_scalar(
                               e.source_raw_json, '$.eventId'
                           )) AS DOUBLE) AS BIGINT) IS DISTINCT FROM e.team_event_id
                           OR TRY_CAST(TRY_CAST(TRY(json_extract_scalar(
                               e.source_raw_json, '$.relatedEventId'
                           )) AS DOUBLE) AS BIGINT) IS DISTINCT FROM
                               e.related_team_event_id
                       )),
                    (SELECT COUNT(*) FROM (
                        SELECT game_id, source_event_id
                        FROM iceberg.bronze.whoscored_events_current
                        WHERE league = {league_sql} AND season = {season_sql}
                          AND _game_batch_id IN (
                              SELECT batch_id FROM latest_match
                          )
                        GROUP BY 1, 2 HAVING COUNT(*) > 1
                    ) duplicate_source_ids),
                    (SELECT COUNT(*) FROM (
                        SELECT game_id, team_id, team_event_id
                        FROM iceberg.bronze.whoscored_events_current
                        WHERE league = {league_sql} AND season = {season_sql}
                          AND _game_batch_id IN (
                              SELECT batch_id FROM latest_match
                          )
                        GROUP BY 1, 2, 3 HAVING COUNT(*) > 1
                    ) duplicate_team_event_ids)
                """
            )
            core_row = cur.fetchall()[0]
            # Keep this parity block separate from the already complex event
            # integrity query.  Expanding all Iceberg ``*_current`` views in
            # one statement exceeded Trino's production 150-stage limit.
            cur.execute(
                f"""
                WITH latest_match AS (
                    SELECT * FROM (
                        SELECT m.*, ROW_NUMBER() OVER (
                            PARTITION BY league, season, game_id
                            ORDER BY COALESCE(
                                completed_at, fetched_at, _ingested_at
                            ) DESC, batch_id DESC
                        ) AS rn
                        FROM iceberg.bronze.whoscored_match_ingest_manifest m
                        WHERE league = {league_sql} AND season = {season_sql}
                          AND batch_id LIKE 'ws2-%' AND raw_uri IS NOT NULL
                          AND parser_version = {_sql_string(PARSER_VERSION)}
                    ) WHERE rn = 1 AND state = 'success'
                )
                SELECT
                    (SELECT COALESCE(SUM(CAST(json_extract_scalar(
                        json_parse(entity_counts_json), '$.matches') AS BIGINT)), 0)
                     FROM latest_match),
                    (SELECT COUNT(*) FROM iceberg.bronze.whoscored_matches_current
                     WHERE league = {league_sql} AND season = {season_sql}
                       AND _game_batch_id IN (SELECT batch_id FROM latest_match)),
                    (SELECT COALESCE(SUM(CAST(json_extract_scalar(
                        json_parse(entity_counts_json), '$.substitutions') AS BIGINT)), 0)
                     FROM latest_match),
                    (SELECT COUNT(*) FROM iceberg.bronze.whoscored_substitutions_current
                     WHERE league = {league_sql} AND season = {season_sql}
                       AND _game_batch_id IN (SELECT batch_id FROM latest_match)),
                    (SELECT COALESCE(SUM(CAST(json_extract_scalar(
                        json_parse(entity_counts_json), '$.formations') AS BIGINT)), 0)
                     FROM latest_match),
                    (SELECT COUNT(*) FROM iceberg.bronze.whoscored_formations_current
                     WHERE league = {league_sql} AND season = {season_sql}
                       AND _game_batch_id IN (SELECT batch_id FROM latest_match)),
                    (SELECT COALESCE(SUM(CAST(json_extract_scalar(
                        json_parse(entity_counts_json), '$.team_match_stats') AS BIGINT)), 0)
                     FROM latest_match),
                    (SELECT COUNT(*) FROM iceberg.bronze.whoscored_team_match_stats_current
                     WHERE league = {league_sql} AND season = {season_sql}
                       AND _game_batch_id IN (SELECT batch_id FROM latest_match)),
                    (SELECT COALESCE(SUM(CAST(json_extract_scalar(
                        json_parse(entity_counts_json), '$.player_match_stats') AS BIGINT)), 0)
                     FROM latest_match),
                    (SELECT COUNT(*) FROM iceberg.bronze.whoscored_player_match_stats_current
                     WHERE league = {league_sql} AND season = {season_sql}
                       AND _game_batch_id IN (SELECT batch_id FROM latest_match))
                """
            )
            match_parity_row = cur.fetchall()[0]
            row = (*core_row[:11], *match_parity_row, *core_row[11:])
            count_sql = ",\n".join(
                f"(SELECT COUNT(*) FROM iceberg.bronze.{table}_current "
                f"WHERE league = {league_sql} AND season = {season_sql})"
                for table in SCOPE_PARITY_TABLES
            )
            cur.execute(
                f"""
                WITH latest_scope AS (
                    SELECT entity_counts_json, dataset_states_json
                    FROM iceberg.bronze.whoscored_scope_ingest_manifest
                    WHERE league = {league_sql} AND season = {season_sql}
                      AND entity_group = 'season' AND state = 'success'
                    ORDER BY completed_at DESC, _ingested_at DESC LIMIT 1
                ),
                scope_stages AS (
                    SELECT stage_id
                    FROM iceberg.bronze.whoscored_stages_current
                    WHERE league = {league_sql} AND season = {season_sql}
                      AND stage_id IS NOT NULL
                    UNION
                    SELECT stage_id
                    FROM iceberg.bronze.whoscored_schedule_current
                    WHERE league = {league_sql} AND season = {season_sql}
                      AND stage_id IS NOT NULL
                )
                SELECT (SELECT entity_counts_json FROM latest_scope),
                       (SELECT dataset_states_json FROM latest_scope),
                       (SELECT ARRAY_AGG(DISTINCT CAST(stage_id AS BIGINT))
                        FROM scope_stages),
                       {count_sql}
                """
            )
            scope_row = cur.fetchall()[0]
            scope_expected = json.loads(scope_row[0]) if scope_row[0] else {}
            scope_manifest_mismatches = sum(
                int(scope_expected.get(table, -1)) != int(actual or 0)
                for table, actual in zip(SCOPE_PARITY_TABLES, scope_row[3:])
            )
            feed_integrity = _feed_state_integrity_summary(
                scope_row[1], scope_row[2] or ()
            )
            cur.execute(
                f"""
                WITH schedule AS (
                    SELECT * FROM (
                        SELECT s.*, ROW_NUMBER() OVER (
                            PARTITION BY league, season, game_id
                            ORDER BY _ingested_at DESC
                        ) AS rn
                        FROM iceberg.bronze.whoscored_schedule_current s
                        WHERE league = {league_sql} AND season = {season_sql}
                    ) WHERE rn = 1
                ),
                latest_preview AS (
                    SELECT * FROM (
                        SELECT m.*, ROW_NUMBER() OVER (
                            PARTITION BY league, season, game_id
                            ORDER BY COALESCE(
                                completed_at, fetched_at, _ingested_at
                            ) DESC, batch_id DESC
                        ) AS rn
                        FROM iceberg.bronze.whoscored_preview_ingest_manifest m
                        WHERE league = {league_sql} AND season = {season_sql}
                    ) WHERE rn = 1
                ),
                valid_preview AS (
                    SELECT * FROM latest_preview
                    WHERE parser_version = {_sql_string(PARSER_VERSION)} AND (
                        (
                            state = 'success' AND batch_id LIKE 'wsp2-%'
                            AND raw_uri IS NOT NULL
                            AND payload_sha256 IS NOT NULL
                        ) OR (
                            state = 'not_available'
                            AND availability_version =
                                {_sql_string(MATCH_AVAILABILITY_VERSION)}
                            AND failure_code IS NOT NULL
                            AND (raw_uri IS NOT NULL OR http_status IN (404, 410))
                        )
                    )
                ),
                valid_preview_success AS (
                    SELECT * FROM valid_preview WHERE state = 'success'
                )
                SELECT
                    (SELECT COUNT(*) FROM schedule
                     WHERE has_preview = TRUE
                       AND date >= CAST(
                           CURRENT_TIMESTAMP - INTERVAL '48' HOUR AS TIMESTAMP
                       )
                       AND date <= CAST(
                           CURRENT_TIMESTAMP + INTERVAL '3' HOUR AS TIMESTAMP
                       )),
                    (SELECT COUNT(*) FROM valid_preview_success),
                    (SELECT COUNT(*) FROM schedule s
                     LEFT JOIN valid_preview p
                       ON p.game_id = CAST(s.game_id AS BIGINT)
                     WHERE s.has_preview = TRUE
                       AND s.date >= CAST(
                           CURRENT_TIMESTAMP - INTERVAL '48' HOUR AS TIMESTAMP
                       )
                       AND s.date <= CAST(
                           CURRENT_TIMESTAMP + INTERVAL '3' HOUR AS TIMESTAMP
                       ) AND p.game_id IS NULL),
                    (SELECT COALESCE(SUM(CAST(json_extract_scalar(
                        json_parse(entity_counts_json), '$.missing_players'
                    ) AS BIGINT)), 0) FROM valid_preview_success),
                    (SELECT COUNT(*)
                     FROM iceberg.bronze.whoscored_missing_players_current
                     WHERE league = {league_sql} AND season = {season_sql}
                       AND _preview_batch_id IN (
                           SELECT batch_id FROM valid_preview_success
                       )),
                    (SELECT COALESCE(SUM(CAST(json_extract_scalar(
                        json_parse(entity_counts_json), '$.preview_lineups'
                    ) AS BIGINT)), 0) FROM valid_preview_success),
                    (SELECT COUNT(*)
                     FROM iceberg.bronze.whoscored_preview_lineups_current
                     WHERE league = {league_sql} AND season = {season_sql}
                       AND _preview_batch_id IN (
                           SELECT batch_id FROM valid_preview_success
                       )),
                    (SELECT COALESCE(SUM(CAST(json_extract_scalar(
                        json_parse(entity_counts_json), '$.preview_sections'
                    ) AS BIGINT)), 0) FROM valid_preview_success),
                    (SELECT COUNT(*)
                     FROM iceberg.bronze.whoscored_preview_sections_current
                     WHERE league = {league_sql} AND season = {season_sql}
                       AND _preview_batch_id IN (
                           SELECT batch_id FROM valid_preview_success
                       )),
                    (SELECT COUNT(*) FROM valid_preview_success
                     WHERE dataset_statuses_json IS NULL
                        OR COALESCE(json_size(
                            json_parse(dataset_statuses_json), '$'
                        ), -1) <> 3
                        OR COALESCE(json_extract_scalar(
                            json_parse(dataset_statuses_json), '$.missing_players'
                        ), '') NOT IN ('available', 'empty', 'not_available')
                        OR COALESCE(json_extract_scalar(
                            json_parse(dataset_statuses_json), '$.preview_lineups'
                        ), '') NOT IN ('available', 'empty', 'not_available')
                        OR COALESCE(json_extract_scalar(
                            json_parse(dataset_statuses_json), '$.preview_sections'
                        ), '') NOT IN ('available', 'empty', 'not_available'))
                """
            )
            preview_row = cur.fetchall()[0]
        finally:
            cur.close()
    finally:
        conn.close()
    return {
        "schedule_rows": int(row[0] or 0),
        "schedule_games": int(row[1] or 0),
        "successful_matches": int(row[2] or 0),
        "manifest_event_rows": int(row[3] or 0),
        "current_event_rows": int(row[4] or 0),
        "manifest_lineup_rows": int(row[5] or 0),
        "current_lineup_rows": int(row[6] or 0),
        "completed_games": int(row[7] or 0),
        "uncovered_completed_games": int(row[8] or 0),
        "event_game_mismatches": int(row[9] or 0),
        "lineup_game_mismatches": int(row[10] or 0),
        "manifest_match_rows": int(row[11] or 0),
        "current_match_rows": int(row[12] or 0),
        "manifest_substitution_rows": int(row[13] or 0),
        "current_substitution_rows": int(row[14] or 0),
        "manifest_formation_rows": int(row[15] or 0),
        "current_formation_rows": int(row[16] or 0),
        "manifest_team_stat_rows": int(row[17] or 0),
        "current_team_stat_rows": int(row[18] or 0),
        "manifest_player_stat_rows": int(row[19] or 0),
        "current_player_stat_rows": int(row[20] or 0),
        "incomplete_final_opta_games": int(row[21] or 0),
        "uncovered_incident_summaries": int(row[22] or 0),
        "uncovered_bet_matches": int(row[23] or 0),
        "incomplete_match_snapshots": int(row[24] or 0),
        "invalid_event_identity_rows": int(row[25] or 0),
        "duplicate_source_event_ids": int(row[26] or 0),
        "duplicate_team_event_ids": int(row[27] or 0),
        "scope_manifest_mismatches": scope_manifest_mismatches,
        **feed_integrity,
        "required_previews": int(preview_row[0] or 0),
        "successful_previews": int(preview_row[1] or 0),
        "uncovered_previews": int(preview_row[2] or 0),
        "manifest_missing_player_rows": int(preview_row[3] or 0),
        "current_missing_player_rows": int(preview_row[4] or 0),
        "manifest_preview_lineup_rows": int(preview_row[5] or 0),
        "current_preview_lineup_rows": int(preview_row[6] or 0),
        "manifest_preview_section_rows": int(preview_row[7] or 0),
        "current_preview_section_rows": int(preview_row[8] or 0),
        "incomplete_preview_snapshots": int(preview_row[9] or 0),
    }


def _profile_integrity_summary(scope_specs: Sequence[str]) -> Dict[str, int]:
    """Validate the active roster in one scoped aggregate Trino query."""

    if not scope_specs:
        return {
            "roster_players": 0,
            "current_profile_manifests": 0,
            "current_profile_rows": 0,
            "uncovered_profiles": 0,
            "stale_profiles": 0,
            "not_available_profiles": 0,
            "parse_failed_profiles": 0,
            "retryable_profiles": 0,
            "terminal_profiles": 0,
            "manifest_participation_rows": 0,
            "current_participation_rows": 0,
        }
    filters = []
    for spec in scope_specs:
        league, separator, season = spec.rpartition("=")
        if not separator or not league or not season:
            raise AirflowException(f"invalid persisted scope {spec!r}")
        filters.append(
            f"(league = {_sql_string(league)} AND season = {_sql_string(season)})"
        )

    from scrapers.whoscored.parsers import MATCH_AVAILABILITY_VERSION, PARSER_VERSION
    from scrapers.whoscored.repository import PROFILE_REFRESH_DAYS
    from scrapers.base.trino_manager import get_trino_connection

    conn = get_trino_connection()
    try:
        cur = conn.cursor()
        try:
            cur.execute(
                f"""
                WITH roster AS (
                    SELECT DISTINCT CAST(player_id AS BIGINT) AS player_id
                    FROM iceberg.bronze.whoscored_player_roster
                    WHERE ({" OR ".join(filters)}) AND player_id IS NOT NULL
                ),
                latest AS (
                    SELECT * FROM (
                        SELECT m.*, ROW_NUMBER() OVER (
                            PARTITION BY CAST(m.player_id AS BIGINT)
                            ORDER BY COALESCE(
                                m.completed_at, m.fetched_at, m._ingested_at
                            ) DESC, COALESCE(m._profile_batch_id, '') DESC,
                            COALESCE(m._batch_id, '') DESC
                        ) AS rn
                        FROM iceberg.bronze.whoscored_profile_ingest_manifest m
                        JOIN roster r
                          ON r.player_id=CAST(m.player_id AS BIGINT)
                    ) WHERE rn=1
                ),
                valid AS (
                    SELECT * FROM latest
                    WHERE parser_version={_sql_string(PARSER_VERSION)} AND (
                        (state='success'
                         AND _profile_batch_id LIKE 'wspr2-%'
                         AND raw_uri IS NOT NULL
                         AND payload_sha256 IS NOT NULL)
                        OR
                        (state='not_available'
                         AND availability_version={_sql_string(MATCH_AVAILABILITY_VERSION)}
                         AND failure_code IS NOT NULL
                         AND (raw_uri IS NOT NULL OR http_status IN (404, 410)))
                    )
                ),
                valid_success AS (
                    SELECT * FROM valid WHERE state='success'
                ),
                profile_rows AS (
                    SELECT COUNT(*) AS value
                    FROM iceberg.bronze.whoscored_player_profile_versions p
                    JOIN valid_success m
                      ON CAST(m.player_id AS BIGINT)=CAST(p.player_id AS BIGINT)
                     AND m._profile_batch_id=p._profile_batch_id
                     AND m.payload_sha256=p.payload_sha256
                     AND m.parser_version=p.parser_version
                ),
                participation_rows AS (
                    SELECT COUNT(*) AS value
                    FROM iceberg.bronze.whoscored_player_stage_participations_current p
                    JOIN valid_success m
                      ON CAST(m.player_id AS BIGINT)=CAST(p.player_id AS BIGINT)
                     AND m._profile_batch_id=p._profile_batch_id
                )
                SELECT
                    (SELECT COUNT(*) FROM roster),
                    (SELECT COUNT(*) FROM valid_success),
                    (SELECT value FROM profile_rows),
                    (SELECT COUNT(*) FROM roster r LEFT JOIN valid v
                     ON CAST(v.player_id AS BIGINT)=r.player_id
                     WHERE v.player_id IS NULL),
                    (SELECT COUNT(*) FROM valid_success WHERE fetched_at IS NULL OR
                     fetched_at <= CURRENT_TIMESTAMP
                       - INTERVAL '{PROFILE_REFRESH_DAYS}' DAY),
                    (SELECT COUNT(*) FROM valid WHERE state='not_available'),
                    (SELECT COUNT(*) FROM latest WHERE state='parse_failed'),
                    (SELECT COUNT(*) FROM latest WHERE state='retryable'),
                    (SELECT COUNT(*) FROM latest WHERE state='terminal'),
                    (SELECT COALESCE(SUM(participations_count), 0)
                     FROM valid_success),
                    (SELECT value FROM participation_rows)
                """
            )
            row = cur.fetchall()[0]
        finally:
            cur.close()
    finally:
        conn.close()
    return {
        "roster_players": int(row[0] or 0),
        "current_profile_manifests": int(row[1] or 0),
        "current_profile_rows": int(row[2] or 0),
        "uncovered_profiles": int(row[3] or 0),
        "stale_profiles": int(row[4] or 0),
        "not_available_profiles": int(row[5] or 0),
        "parse_failed_profiles": int(row[6] or 0),
        "retryable_profiles": int(row[7] or 0),
        "terminal_profiles": int(row[8] or 0),
        "manifest_participation_rows": int(row[9] or 0),
        "current_participation_rows": int(row[10] or 0),
    }


def _catalog_integrity_summary(
    catalog_identity: Mapping[str, Any],
) -> Dict[str, int]:
    """Check one report-bound catalog batch, never the mutable latest batch."""

    from scrapers.base.trino_manager import get_trino_connection

    identity = _catalog_identity(catalog_identity, exact=True)
    batch_id = identity["catalog_batch_id"]
    batch_sql = _sql_string(batch_id)
    conn = get_trino_connection()
    try:
        cur = conn.cursor()
        try:
            cur.execute(
                f"""
                WITH selected_manifest AS (
                    SELECT *
                    FROM iceberg.bronze.whoscored_catalog_manifest
                    WHERE state = 'success' AND batch_id = {batch_sql}
                ),
                competitions AS (
                    SELECT * FROM iceberg.bronze.whoscored_competitions
                    WHERE _catalog_batch_id = {batch_sql}
                ),
                seasons AS (
                    SELECT * FROM iceberg.bronze.whoscored_seasons
                    WHERE _catalog_batch_id = {batch_sql}
                ),
                stages AS (
                    SELECT * FROM iceberg.bronze.whoscored_stages
                    WHERE _catalog_batch_id = {batch_sql}
                )
                SELECT
                    (SELECT COUNT(*) FROM selected_manifest),
                    (SELECT COALESCE(MAX(competitions_count), 0)
                     FROM selected_manifest),
                    (SELECT COUNT(*) FROM competitions),
                    (SELECT COUNT(DISTINCT record_key) FROM competitions),
                    (SELECT COALESCE(MAX(seasons_count), 0)
                     FROM selected_manifest),
                    (SELECT COUNT(*) FROM seasons),
                    (SELECT COUNT(DISTINCT record_key) FROM seasons),
                    (SELECT COALESCE(MAX(stages_count), 0)
                     FROM selected_manifest),
                    (SELECT COUNT(*) FROM stages),
                    (SELECT COUNT(DISTINCT record_key) FROM stages),
                    (SELECT COALESCE(MAX(quarantined_count), 0)
                     FROM selected_manifest),
                    (SELECT COUNT(*) FROM (
                        SELECT s.competition_id, s.source_season_id
                        FROM seasons s
                        JOIN competitions c
                          ON c.competition_id = s.competition_id
                        LEFT JOIN stages g
                          ON g.competition_id = s.competition_id
                         AND g.source_season_id = s.source_season_id
                        WHERE c.eligibility = 'included'
                          AND s.eligibility = 'included'
                        GROUP BY s.competition_id, s.source_season_id
                        HAVING COUNT(g.stage_id) = 0
                    ))
                """
            )
            row = cur.fetchall()[0]
            cur.execute(
                f"""
                SELECT batch_id, payload_sha256, parser_version,
                       schema_fingerprint, raw_provenance_sha256,
                       raw_inputs_json, as_of_date,
                       parent_catalog_batch_id,
                       parent_catalog_payload_sha256,
                       parent_catalog_raw_provenance_sha256
                FROM iceberg.bronze.whoscored_catalog_manifest
                WHERE state = 'success' AND batch_id = {batch_sql}
                """
            )
            identity_rows = cur.fetchall()
            identity_valid = 0
            active_scope_count = 0
            if len(identity_rows) == 1 and int(row[0] or 0) == 1:
                (
                    manifest_batch_id,
                    payload_sha256,
                    parser_version,
                    schema_fingerprint,
                    raw_provenance_sha256,
                    raw_inputs_json,
                    as_of_date,
                    parent_batch_id,
                    parent_payload_sha256,
                    parent_raw_provenance_sha256,
                ) = identity_rows[0]
                physical: dict[str, list[dict[str, Any]]] = {}
                try:
                    for kind in ("competitions", "seasons", "stages"):
                        cur.execute(
                            f"SELECT payload_json FROM "
                            f"iceberg.bronze.whoscored_{kind} "
                            "WHERE _catalog_batch_id = "
                            f"{batch_sql} ORDER BY record_key"
                        )
                        physical[kind] = [
                            json.loads(str(value)) for (value,) in cur.fetchall()
                        ]
                    from scrapers.whoscored.catalog import WhoScoredCatalog
                    from scrapers.whoscored.parsers import PARSER_VERSION
                    from scrapers.whoscored.repository import catalog_payload_sha256
                    from scrapers.whoscored.repository import (
                        _validate_catalog_raw_provenance,
                    )

                    catalog = WhoScoredCatalog.from_rows(physical)
                    physical_fingerprint = catalog_payload_sha256(catalog.to_rows())
                    descriptors = json.loads(str(raw_inputs_json or ""))
                    if not isinstance(descriptors, list):
                        raise ValueError("catalog raw input descriptors are not a list")
                    _validate_catalog_raw_provenance(
                        batch_id=batch_id,
                        raw_inputs_json=str(raw_inputs_json or ""),
                        raw_provenance_sha256=str(raw_provenance_sha256 or ""),
                        as_of_date=date.fromisoformat(identity["catalog_as_of_date"]),
                    )
                    audit_hashes = [
                        str(descriptor.get("payload_sha256") or "")
                        for descriptor in descriptors
                        if isinstance(descriptor, Mapping)
                        and descriptor.get("audit_type")
                        == "whoscored_technical_exclusion_audit"
                    ]
                    if (
                        len(audit_hashes) != 1
                        or re.fullmatch(r"[0-9a-f]{64}", audit_hashes[0]) is None
                    ):
                        raise ValueError(
                            "catalog has invalid technical audit descriptor"
                        )
                    expected_parent = (
                        identity["parent_catalog_batch_id"],
                        identity["parent_catalog_payload_sha256"],
                        identity["parent_catalog_raw_provenance_sha256"],
                    )
                    manifest_parent = (
                        None if parent_batch_id is None else str(parent_batch_id),
                        None
                        if parent_payload_sha256 is None
                        else str(parent_payload_sha256),
                        None
                        if parent_raw_provenance_sha256 is None
                        else str(parent_raw_provenance_sha256),
                    )
                    active_scope_count = len(
                        catalog.eligible_scopes(
                            active_only=True,
                            on=date.fromisoformat(identity["catalog_as_of_date"]),
                        )
                    )
                    identity_valid = int(
                        str(manifest_batch_id or "") == batch_id
                        and str(payload_sha256 or "")
                        == identity["catalog_payload_sha256"]
                        and str(payload_sha256 or "") == physical_fingerprint
                        and str(schema_fingerprint or "") == physical_fingerprint
                        and str(parser_version or "") == PARSER_VERSION
                        and str(raw_provenance_sha256 or "")
                        == identity["catalog_raw_provenance_sha256"]
                        and audit_hashes[0]
                        == identity["technical_exclusion_audit_sha256"]
                        and str(as_of_date or "") == identity["catalog_as_of_date"]
                        and manifest_parent == expected_parent
                    )
                except (RuntimeError, TypeError, ValueError):
                    identity_valid = 0
        finally:
            cur.close()
    finally:
        conn.close()
    return {
        "manifest_competitions": int(row[1] or 0),
        "physical_competitions": int(row[2] or 0),
        "distinct_competitions": int(row[3] or 0),
        "manifest_seasons": int(row[4] or 0),
        "physical_seasons": int(row[5] or 0),
        "distinct_seasons": int(row[6] or 0),
        "manifest_stages": int(row[7] or 0),
        "physical_stages": int(row[8] or 0),
        "distinct_stages": int(row[9] or 0),
        "quarantined": int(row[10] or 0),
        "eligible_seasons_without_stages": int(row[11] or 0),
        "active_scopes": active_scope_count,
        "manifest_identity_valid": identity_valid,
    }


def validate_scope_result(
    *,
    scope_spec: str,
    result_path: str,
    require_zero_paid: bool = True,
    expected_catalog_batch_id: Optional[str] = None,
    **_context: Any,
) -> Dict[str, Any]:
    """Validate this run artifact and logical Bronze parity for one scope."""
    result = _load_result(result_path)
    if result.get("status") != "success":
        raise AirflowException(
            f"WhoScored producer failed for {scope_spec}: "
            f"status={result.get('status')} errors={result.get('errors', [])}"
        )
    reported = {item.get("scope") for item in result.get("scopes", [])}
    if reported != {scope_spec}:
        raise AirflowException(
            f"WhoScored report scope mismatch: expected {scope_spec}, got {reported}"
        )
    if expected_catalog_batch_id is not None and result.get("catalog_batch_id") != str(
        expected_catalog_batch_id
    ):
        raise AirflowException(
            "WhoScored report catalog generation mismatch: "
            f"expected {expected_catalog_batch_id}, "
            f"got {result.get('catalog_batch_id')}"
        )
    paid = int(result.get("paid_proxy_bytes") or 0)
    transport = _transport_runtime(
        _context,
        task_id="ingest_active_scope",
        work_item_id=stable_scope_work_item(scope_spec),
    )
    allocation_limit = (
        transport.allocation.budget_bytes if transport.allocation is not None else 0
    )
    if not transport.is_paid and paid:
        raise AirflowException(
            f"normal daily run used paid proxy for {scope_spec}: {paid} bytes"
        )
    if paid > allocation_limit:
        raise AirflowException(
            f"paid proxy allocation exceeded: {paid}>{allocation_limit} bytes"
        )
    if require_zero_paid and paid:
        raise AirflowException(
            f"normal daily run used paid proxy for {scope_spec}: {paid} bytes"
        )

    backlog = result.get("match_candidates")
    if not isinstance(backlog, Mapping) or set(backlog) != {
        "schema_version",
        "count",
        "attempted",
        "remaining",
    }:
        raise AirflowException(
            f"WhoScored match backlog contract is missing for {scope_spec}"
        )
    count = backlog.get("count")
    attempted = backlog.get("attempted")
    remaining = backlog.get("remaining")
    if (
        backlog.get("schema_version") != 1
        or type(count) is not int
        or type(attempted) is not int
        or type(remaining) is not int
        or count < 0
        or attempted < 0
        or attempted > 100
        or remaining < 0
        or count != attempted + remaining
    ):
        raise AirflowException(
            f"WhoScored match backlog identity is invalid for {scope_spec}: {backlog}"
        )
    if remaining:
        raise AirflowException(
            f"WhoScored match backlog remains for {scope_spec}: {backlog}"
        )

    commits = _producer_commits_from_report(result)
    attempts = _producer_attempts_from_report(result)
    if (
        len(commits["scope"]) != 1
        or commits["profile"]
        or commits["profile_not_available"]
        or attempts["profile"]
        or len(attempts["match"]) != 1
        or len(attempts["preview"]) != 1
        or attempts["match"][0]["scope"] != scope_spec
        or attempts["preview"][0]["scope"] != scope_spec
    ):
        raise AirflowException(
            f"WhoScored producer commit scope is invalid for {scope_spec}"
        )
    if attempts["match"][0]["count"] != attempted:
        raise AirflowException(
            f"WhoScored match backlog/attempt identity differs for {scope_spec}"
        )
    exact = _scope_producer_integrity_summary(scope_spec, commits)
    for kind in ("scope", "match", "preview"):
        if (
            exact[f"expected_{kind}_batches"] != exact[f"exact_{kind}_manifests"]
            or exact[f"exact_{kind}_dataset_mismatches"]
        ):
            raise AirflowException(
                f"WhoScored exact {kind} commit parity failed for {scope_spec}: {exact}"
            )
    for kind in ("match", "preview"):
        if (
            exact[f"expected_{kind}_not_available_batches"]
            != exact[f"exact_{kind}_not_available_manifests"]
            or exact[f"exact_{kind}_not_available_physical_rows"]
            or exact[f"exact_{kind}_outcome_duplicates"]
            or exact[f"exact_{kind}_outcome_count"] != attempts[kind][0]["count"]
            or exact[f"exact_{kind}_outcome_payload_sha256"]
            != attempts[kind][0]["payload_sha256"]
        ):
            raise AirflowException(
                f"WhoScored exact {kind} outcome identity failed for {scope_spec}: "
                f"attempted={attempts[kind][0]} exact={exact}"
            )

    integrity = _scope_integrity_summary(scope_spec)
    if integrity["schedule_rows"] < 1:
        raise AirflowException(f"WhoScored schedule is empty for {scope_spec}")
    if integrity["schedule_rows"] != integrity["schedule_games"]:
        raise AirflowException(
            f"WhoScored schedule duplicates for {scope_spec}: {integrity}"
        )
    if integrity["manifest_event_rows"] != integrity["current_event_rows"]:
        raise AirflowException(
            f"WhoScored event manifest parity failed for {scope_spec}: {integrity}"
        )
    if integrity["manifest_lineup_rows"] != integrity["current_lineup_rows"]:
        raise AirflowException(
            f"WhoScored lineup manifest parity failed for {scope_spec}: {integrity}"
        )
    if integrity["uncovered_completed_games"]:
        raise AirflowException(
            f"WhoScored completed-match coverage failed for {scope_spec}: {integrity}"
        )
    if integrity["event_game_mismatches"] or integrity["lineup_game_mismatches"]:
        raise AirflowException(
            f"WhoScored per-game manifest parity failed for {scope_spec}: {integrity}"
        )
    if integrity["incomplete_final_opta_games"]:
        raise AirflowException(
            f"WhoScored final Opta event completeness failed for {scope_spec}: "
            f"{integrity}"
        )
    if integrity["uncovered_incident_summaries"]:
        raise AirflowException(
            f"WhoScored schedule incident coverage failed for {scope_spec}: {integrity}"
        )
    if integrity["uncovered_bet_matches"]:
        raise AirflowException(
            f"WhoScored schedule bet coverage failed for {scope_spec}: {integrity}"
        )
    if integrity["incomplete_match_snapshots"]:
        raise AirflowException(
            f"WhoScored match dataset-state contract failed for {scope_spec}: "
            f"{integrity}"
        )
    if (
        integrity["invalid_event_identity_rows"]
        or integrity["duplicate_source_event_ids"]
        or integrity["duplicate_team_event_ids"]
    ):
        raise AirflowException(
            f"WhoScored event identity contract failed for {scope_spec}: {integrity}"
        )
    for manifest_key, current_key in (
        ("manifest_match_rows", "current_match_rows"),
        ("manifest_substitution_rows", "current_substitution_rows"),
        ("manifest_formation_rows", "current_formation_rows"),
        ("manifest_team_stat_rows", "current_team_stat_rows"),
        ("manifest_player_stat_rows", "current_player_stat_rows"),
    ):
        if integrity[manifest_key] != integrity[current_key]:
            raise AirflowException(
                f"WhoScored dataset manifest parity failed for {scope_spec}: {integrity}"
            )
    if integrity["scope_manifest_mismatches"]:
        raise AirflowException(
            f"WhoScored scope manifest parity failed for {scope_spec}: {integrity}"
        )
    if (
        integrity["feed_state_stage_count"] < 1
        or integrity["missing_feed_state_count"]
        or integrity["extra_feed_state_count"]
        or integrity["malformed_feed_state_count"]
    ):
        raise AirflowException(
            f"WhoScored source feed manifest completeness failed for {scope_spec}: "
            f"{integrity}"
        )
    if integrity["uncovered_previews"] or integrity["incomplete_preview_snapshots"]:
        raise AirflowException(
            f"WhoScored preview coverage failed for {scope_spec}: {integrity}"
        )
    for manifest_key, current_key in (
        ("manifest_missing_player_rows", "current_missing_player_rows"),
        ("manifest_preview_lineup_rows", "current_preview_lineup_rows"),
        ("manifest_preview_section_rows", "current_preview_section_rows"),
    ):
        if integrity[manifest_key] != integrity[current_key]:
            raise AirflowException(
                f"WhoScored preview manifest parity failed for {scope_spec}: "
                f"{integrity}"
            )
    return {
        "scope": scope_spec,
        "paid_proxy_bytes": paid,
        **exact,
        **integrity,
    }


def validate_profile_result(
    *,
    scope_plan: Optional[Mapping[str, Any]] = None,
    capacity: Optional[Mapping[str, Any]] = None,
    **context: Any,
) -> Dict[str, Any]:
    path = _run_dir_from_context(context) / "profiles.json"
    result = _load_result(str(path))
    if result.get("status") != "success":
        raise AirflowException(
            f"WhoScored profile refresh failed: {result.get('errors', [])}"
        )
    paid = int(result.get("paid_proxy_bytes") or 0)
    if scope_plan is None:
        transport = _transport_runtime(context)
    else:
        transport = _transport_runtime(
            context,
            task_id="refresh_whoscored_profiles",
            work_item_id=stable_profiles_work_item(),
        )
    allocation_limit = (
        transport.allocation.budget_bytes if transport.allocation is not None else 0
    )
    if paid > allocation_limit:
        raise AirflowException(
            f"profile paid allocation exceeded: {paid}>{allocation_limit} bytes"
        )
    if not transport.is_paid and paid:
        raise AirflowException(f"normal profile refresh used paid proxy: {paid} bytes")
    commits = _producer_commits_from_report(result)
    attempts = _producer_attempts_from_report(result)
    if (
        commits["scope"]
        or commits["match"]
        or commits["match_not_available"]
        or commits["preview"]
        or commits["preview_not_available"]
        or attempts["match"]
        or attempts["preview"]
        or len(attempts["profile"]) != 1
    ):
        raise AirflowException("WhoScored profile report contains scoped commits")
    attempted_profile = attempts["profile"][0]
    if scope_plan is None:
        scopes = _active_scope_specs()
    else:
        batch_id, scopes = _daily_scope_plan_specs(scope_plan)
        if result.get("catalog_batch_id") != batch_id:
            raise AirflowException(
                "WhoScored profile report catalog generation mismatch: "
                f"expected {batch_id}, got {result.get('catalog_batch_id')}"
            )
        if capacity is None:
            raise AirflowException("WhoScored profile result has no frozen capacity")
        expected_count, expected_sha256 = _daily_profile_capacity_specs(
            capacity,
            expected_catalog_batch_id=batch_id,
        )
        profile_candidates = result.get("profile_candidates")
        if (
            not isinstance(profile_candidates, Mapping)
            or set(profile_candidates)
            != {"schema_version", "count", "payload_sha256", "attempted"}
            or type(profile_candidates.get("schema_version")) is not int
            or profile_candidates.get("schema_version") != 1
            or type(profile_candidates.get("count")) is not int
            or profile_candidates.get("count") != expected_count
            or type(profile_candidates.get("attempted")) is not int
            or profile_candidates.get("attempted") != expected_count
            or profile_candidates.get("payload_sha256") != expected_sha256
        ):
            raise AirflowException(
                "WhoScored profile result candidate identity mismatch: "
                f"expected_count={expected_count}, "
                f"expected_sha256={expected_sha256}, "
                f"reported={profile_candidates}"
            )
        if (
            attempted_profile["count"] != expected_count
            or attempted_profile["payload_sha256"] != expected_sha256
        ):
            raise AirflowException(
                "WhoScored profile attempted outcome identity mismatch: "
                f"expected_count={expected_count}, "
                f"expected_sha256={expected_sha256}, "
                f"reported={attempted_profile}"
            )
    exact = _profile_producer_integrity_summary(commits)
    if (
        exact["expected_profile_batches"] != exact["exact_profile_manifests"]
        or exact["exact_profile_row_mismatches"]
        or exact["exact_profile_participation_mismatches"]
        or exact["expected_profile_not_available_batches"]
        != exact["exact_profile_not_available_manifests"]
        or exact["exact_profile_not_available_physical_rows"]
        or exact["exact_profile_outcome_duplicates"]
        or exact["exact_profile_outcome_count"] != attempted_profile["count"]
        or exact["exact_profile_outcome_payload_sha256"]
        != attempted_profile["payload_sha256"]
    ):
        raise AirflowException(f"WhoScored exact profile commit parity failed: {exact}")
    integrity = _profile_integrity_summary(scopes)
    if integrity["uncovered_profiles"] or integrity["stale_profiles"]:
        raise AirflowException(
            f"WhoScored active profile coverage is incomplete: {integrity}"
        )
    if (
        integrity["current_profile_manifests"] != integrity["current_profile_rows"]
        or integrity["manifest_participation_rows"]
        != integrity["current_participation_rows"]
    ):
        raise AirflowException(f"WhoScored profile manifest parity failed: {integrity}")
    return {
        "status": "success",
        "paid_proxy_bytes": paid,
        **exact,
        **integrity,
    }


def validate_catalog_result(**context: Any) -> Dict[str, Any]:
    path = _run_dir_from_context(context) / "discovery.json"
    result = _load_result(str(path))
    if result.get("status") != "success":
        raise AirflowException(
            f"WhoScored discovery failed: {result.get('errors', [])}"
        )
    identity = _catalog_identity(result, exact=False)
    integrity = _catalog_integrity_summary(identity)
    if integrity["manifest_competitions"] < 100:
        raise AirflowException(f"WhoScored catalog is implausibly small: {integrity}")
    for manifest_key, physical_key, distinct_key in (
        ("manifest_competitions", "physical_competitions", "distinct_competitions"),
        ("manifest_seasons", "physical_seasons", "distinct_seasons"),
        ("manifest_stages", "physical_stages", "distinct_stages"),
    ):
        if not (
            integrity[manifest_key]
            == integrity[physical_key]
            == integrity[distinct_key]
        ):
            raise AirflowException(
                f"WhoScored catalog manifest parity failed: {integrity}"
            )
    if integrity["quarantined"]:
        transport = _transport_runtime(context)
        authority = (
            transport.approval.scheduled_authority
            if transport.is_paid and transport.approval is not None
            else None
        )
        if authority is not None:
            from dags.scripts import run_whoscored_scraper as runner

            repository = runner._new_repository()
            _generation, catalog = repository.load_catalog_generation_snapshot(
                batch_id=identity["catalog_batch_id"]
            )
            selected = runner._select_catalog_snapshot_scopes(
                catalog, [], active_only=True
            )
            active_scopes = {scope.spec for scope, _runtime in selected}
            missing = sorted(
                {item.scope for item in authority.scope_workloads} - active_scopes
            )
            if missing:
                audit = _write_quarantine_disappearance_audit(
                    catalog=catalog,
                    missing_scopes=missing,
                    signed_parent_batch_id=authority.catalog_batch_id,
                    candidate_identity=identity,
                    context=context,
                )
                raise AirflowException(
                    "WHOSCORED_QUARANTINE_DISAPPEARANCE: signed scopes became "
                    f"quarantined: {missing}; audit_sha256="
                    f"{audit['audit_sha256']}"
                )
    if integrity["quarantined"] or integrity["eligible_seasons_without_stages"]:
        raise AirflowException(f"WhoScored catalog is incomplete: {integrity}")
    if integrity["manifest_identity_valid"] != 1:
        raise AirflowException(
            f"WhoScored catalog manifest identity verification failed: {integrity}"
        )
    if integrity["active_scopes"] < 1:
        raise AirflowException("WhoScored catalog has no active adult men's scopes")
    return {
        "status": "success",
        "catalog_identity": identity,
        **integrity,
    }


def aggregate_traffic_reports(
    *,
    allow_empty: bool = False,
    additional_reported_paid_attribution: Optional[Mapping[str, Any]] = None,
    **context: Any,
) -> Dict[str, Any]:
    """Persist detailed traffic and return a bounded XCom summary.

    Full URL/task maps are an immutable operational S3 artifact. Returning
    them directly made one production XCom grow beyond 13 MiB and burdened the
    Airflow metadata database and UI.
    """
    run_dir = _run_dir_from_context(context)
    cleanup_stale_run_directories(current_run_dir=run_dir)
    dag_id = getattr(context.get("dag"), "dag_id", None) or context.get("dag_id")
    run_id = context.get("run_id") or getattr(context.get("dag_run"), "run_id", None)
    from dags.scripts.whoscored_ops_store import WhoScoredOpsStore

    ops_store = WhoScoredOpsStore.from_env(optional=False)
    if ops_store is None:  # pragma: no cover - guarded by optional=False
        raise AirflowException("WhoScored operational S3 store is required")
    traffic_prefix = (
        "traffic/"
        f"{_safe_token(str(dag_id or 'unknown'))}/"
        f"{_safe_token(str(run_id or 'unknown'))}"
    )
    transport = _transport_runtime(context)
    reconciliation_context = _traffic_reconciliation_context(transport)
    reconciliation_context["external_report_attribution_sha256"] = (
        hashlib.sha256(
            json.dumps(
                dict(additional_reported_paid_attribution),
                ensure_ascii=False,
                sort_keys=True,
                separators=(",", ":"),
            ).encode("utf-8")
        ).hexdigest()
        if additional_reported_paid_attribution is not None
        else None
    )
    completion_records = list(
        ops_store.iter_content_addressed_json(f"{traffic_prefix}/completion")
    )
    if completion_records:
        summaries = []
        for _key, receipt in completion_records:
            detail_artifact = receipt.get("detail_artifact")
            if (
                set(receipt)
                != {
                    "schema_version",
                    "dag_id",
                    "run_id",
                    "reconciliation_context",
                    "input_evidence_sha256",
                    "summary",
                    "detail_artifact",
                    "cleanup_files",
                }
                or receipt.get("schema_version") != 2
                or receipt.get("dag_id") != str(dag_id or "")
                or receipt.get("run_id") != str(run_id or "")
                or receipt.get("reconciliation_context") != reconciliation_context
                or not isinstance(receipt.get("summary"), Mapping)
                or not isinstance(receipt.get("cleanup_files"), list)
                or not isinstance(receipt.get("input_evidence_sha256"), str)
                or re.fullmatch(r"[0-9a-f]{64}", receipt["input_evidence_sha256"])
                is None
                or not isinstance(detail_artifact, Mapping)
                or set(detail_artifact) != {"uri", "key", "sha256", "bytes"}
            ):
                raise AirflowException("invalid durable WhoScored traffic completion")
            detail_key = detail_artifact.get("key")
            detail_sha = detail_artifact.get("sha256")
            detail_bytes = detail_artifact.get("bytes")
            if (
                not isinstance(detail_key, str)
                or not detail_key.startswith(f"{traffic_prefix}/detail/")
                or not isinstance(detail_sha, str)
                or re.fullmatch(r"[0-9a-f]{64}", detail_sha) is None
                or type(detail_bytes) is not int
                or detail_bytes <= 0
            ):
                raise AirflowException(
                    "invalid durable WhoScored traffic detail reference"
                )
            detail = ops_store.read_content_addressed_json(
                detail_key,
                expected_sha256=detail_sha,
                expected_bytes=detail_bytes,
            )
            summary = receipt["summary"]
            input_evidence = detail.get("input_evidence")
            input_evidence_sha256 = hashlib.sha256(
                json.dumps(
                    input_evidence,
                    ensure_ascii=False,
                    sort_keys=True,
                    separators=(",", ":"),
                ).encode("utf-8")
            ).hexdigest()
            if (
                detail.get("schema_version") != 2
                or detail.get("reconciliation_context") != reconciliation_context
                or not isinstance(input_evidence, Mapping)
                or set(input_evidence)
                != {
                    "schema_version",
                    "reports",
                    "request_ledgers",
                    "proxy_event_count",
                    "proxy_events_sha256",
                    "campaign_snapshot_sha256",
                }
                or input_evidence.get("schema_version") != 1
                or input_evidence_sha256 != receipt["input_evidence_sha256"]
                or input_evidence.get("campaign_snapshot_sha256")
                != _current_campaign_snapshot_sha256(transport)
                or summary.get("artifact_uri") != detail_artifact.get("uri")
                or summary.get("artifact_sha256") != detail_sha
                or summary.get("artifact_bytes") != detail_bytes
            ):
                raise AirflowException(
                    "WhoScored traffic completion/detail identity mismatch"
                )
            _assert_staging_input_files(
                run_dir,
                input_evidence,
                require_all=False,
            )
            durable_expected = sorted(
                (
                    dict(item)
                    for item in input_evidence["request_ledgers"]
                    if item.get("durable") is True
                ),
                key=lambda item: item["evidence_key"],
            )
            _durable_sources, _durable_events, durable_current = (
                _validated_durable_request_evidence(
                    ops_store,
                    prefix=f"{traffic_prefix}/request-ledgers",
                    dag_id=str(dag_id or ""),
                    run_id=str(run_id or ""),
                )
            )
            if durable_current != durable_expected:
                raise AirflowException(
                    "WhoScored durable request evidence changed after reconciliation"
                )
            current_paid_events: list[Mapping[str, Any]] = []
            paid_ledger = Path(PAID_LEDGER_PATH)
            if paid_ledger.exists():
                current_paid_events = [
                    event
                    for event in _iter_jsonl_events(paid_ledger, label="paid ledger")
                    if str(event.get("dag_id") or "") == str(dag_id or "")
                    and str(event.get("run_id") or "") == str(run_id or "")
                ]
            if input_evidence.get("proxy_event_count") != len(
                current_paid_events
            ) or input_evidence.get("proxy_events_sha256") != _proxy_events_sha256(
                current_paid_events
            ):
                raise AirflowException(
                    "WhoScored durable traffic proxy evidence changed"
                )
            for name in receipt["cleanup_files"]:
                if not isinstance(name, str) or Path(name).name != name:
                    raise AirflowException(
                        "unsafe file in durable WhoScored traffic completion"
                    )
                try:
                    (run_dir / name).unlink()
                except FileNotFoundError:
                    pass
                except OSError as exc:
                    raise AirflowException(
                        f"cannot resume WhoScored traffic cleanup for {name}: {exc}"
                    ) from exc
            summaries.append(dict(summary))
        canonical = {
            json.dumps(item, separators=(",", ":"), sort_keys=True)
            for item in summaries
        }
        if len(canonical) != 1:
            raise AirflowException(
                "conflicting durable WhoScored traffic completions for one DagRun"
            )
        return summaries[0]

    reports: list[tuple[str, dict[str, Any]]] = []
    stable_reports: dict[str, dict[str, Any]] = {}
    attempt_reports: dict[str, list[tuple[str, dict[str, Any], bytes]]] = {}
    attempt_identities: set[str] = set()
    report_cleanup_paths: list[Path] = []
    for path in sorted(run_dir.glob("*.json")):
        match = _ATTEMPT_REPORT_RE.fullmatch(path.name)
        if ".attempt-" in path.name and match is None:
            raise AirflowException(
                f"malformed immutable WhoScored attempt report {path.name}"
            )
        if match is None:
            try:
                result = _load_result(str(path))
            except AirflowException:
                continue
            stable_reports[path.name] = result
            report_cleanup_paths.append(path)
            continue
        try:
            payload = path.read_bytes()
            result = json.loads(payload.decode("utf-8"))
        except (OSError, UnicodeDecodeError, ValueError) as exc:
            raise AirflowException(
                f"immutable WhoScored attempt report {path.name} is corrupt: {exc}"
            ) from exc
        if not isinstance(result, dict) or result.get("schema_version") != 3:
            raise AirflowException(
                f"immutable WhoScored attempt report {path.name} is not schema v3"
            )
        content_sha256 = hashlib.sha256(payload).hexdigest()
        identity_sha256 = _attempt_report_identity_sha256(match.group("output"), result)
        airflow = result["airflow"]
        if (
            content_sha256 != match.group("content")
            or identity_sha256 != match.group("identity")
            or airflow.get("dag_id") != str(dag_id or "")
            or airflow.get("dag_run_id") != str(run_id or "")
            or identity_sha256 in attempt_identities
        ):
            raise AirflowException(
                f"immutable WhoScored attempt report {path.name} failed identity verification"
            )
        attempt_identities.add(identity_sha256)
        attempt_reports.setdefault(match.group("output"), []).append(
            (path.name, result, payload)
        )
        report_cleanup_paths.append(path)
    for output_name, values in attempt_reports.items():
        stable = stable_reports.pop(output_name, None)
        if stable is not None:
            stable_payload = json.dumps(
                stable,
                ensure_ascii=False,
                sort_keys=True,
                separators=(",", ":"),
            ).encode("utf-8")
            if all(stable_payload != payload for _name, _result, payload in values):
                raise AirflowException(
                    f"WhoScored stable report {output_name} has no immutable attempt"
                )
        reports.extend((name, result) for name, result, _payload in values)
    reports.extend(sorted(stable_reports.items()))
    if not reports and not allow_empty:
        raise AirflowException(f"no WhoScored reports found in {run_dir}")

    reported_paid = 0
    reported_paid_by_url: dict[str, int] = {}
    reported_paid_by_task: dict[str, int] = {}
    reported_paid_by_task_try: dict[str, int] = {}
    for name, result in reports:
        _validate_paid_report_identity(
            transport,
            result,
            dag_id=str(dag_id or ""),
            run_id=str(run_id or ""),
        )
        raw_report_paid = result.get("paid_proxy_bytes", 0)
        if type(raw_report_paid) is not int or raw_report_paid < 0:
            raise AirflowException(
                f"invalid paid_proxy_bytes in WhoScored report {name}"
            )
        report_paid = raw_report_paid
        reported_paid += report_paid
        airflow_identity = result.get("airflow")
        if not isinstance(airflow_identity, Mapping):
            airflow_identity = {}
        _counter_add(
            reported_paid_by_task,
            _traffic_task_key(airflow_identity),
            report_paid,
        )
        _counter_add(
            reported_paid_by_task_try,
            _traffic_task_key(airflow_identity, include_try=True),
            report_paid,
        )
        traffic = result.get("traffic")
        paid_urls = (
            traffic.get("paid_proxy_bytes_by_url")
            if isinstance(traffic, Mapping)
            else None
        )
        if isinstance(paid_urls, Mapping):
            for raw_url, raw_count in paid_urls.items():
                url = _canonical_traffic_url(raw_url)
                if type(raw_count) is not int or raw_count < 0:
                    raise AirflowException(
                        f"invalid paid URL bytes in WhoScored report {name}"
                    )
                count = raw_count
                if url and count:
                    _counter_add(reported_paid_by_url, url, count)
    if additional_reported_paid_attribution is not None:
        external = additional_reported_paid_attribution
        expected_fields = {
            "schema_version",
            "evidence_type",
            "dag_id",
            "run_id",
            "task_id",
            "map_index",
            "try_number",
            "paid_proxy_bytes",
            "paid_proxy_bytes_by_url",
            "paid_proxy_bytes_by_task",
            "paid_proxy_bytes_by_task_try",
        }
        external_paid = external.get("paid_proxy_bytes")
        external_urls = external.get("paid_proxy_bytes_by_url")
        external_tasks = external.get("paid_proxy_bytes_by_task")
        external_task_tries = external.get("paid_proxy_bytes_by_task_try")
        if (
            set(external) != expected_fields
            or external.get("schema_version") != 1
            or external.get("evidence_type") != "whoscored_report_paid_attribution"
            or external.get("dag_id") != str(dag_id or "")
            or external.get("run_id") != str(run_id or "")
            or type(external.get("map_index")) is not int
            or type(external.get("try_number")) is not int
            or int(external.get("map_index")) < -1
            or int(external.get("try_number")) < 1
            or type(external_paid) is not int
            or external_paid < 0
            or not isinstance(external_urls, Mapping)
            or not isinstance(external_tasks, Mapping)
            or not isinstance(external_task_tries, Mapping)
        ):
            raise AirflowException(
                "invalid external WhoScored report attribution evidence"
            )
        normalized_external_urls: dict[str, int] = {}
        for raw_url, raw_count in external_urls.items():
            url = _canonical_traffic_url(raw_url)
            if (
                not url
                or type(raw_count) is not int
                or raw_count <= 0
                or url in normalized_external_urls
            ):
                raise AirflowException(
                    "invalid external WhoScored report URL attribution"
                )
            normalized_external_urls[url] = raw_count
        expected_task = _traffic_task_key(external)
        expected_task_try = _traffic_task_key(external, include_try=True)
        expected_tasks = {expected_task: external_paid} if external_paid else {}
        expected_task_tries = (
            {expected_task_try: external_paid} if external_paid else {}
        )
        if (
            sum(normalized_external_urls.values()) != external_paid
            or dict(external_tasks) != expected_tasks
            or dict(external_task_tries) != expected_task_tries
        ):
            raise AirflowException(
                "external WhoScored report attribution counters differ"
            )
        reported_paid += external_paid
        for url, count in normalized_external_urls.items():
            _counter_add(reported_paid_by_url, url, count)
        for task, count in expected_tasks.items():
            _counter_add(reported_paid_by_task, task, count)
        for task_try, count in expected_task_tries.items():
            _counter_add(reported_paid_by_task_try, task_try, count)

    route_requests: dict[str, int] = {}
    route_bytes: dict[str, int] = {}
    requests_by_url: dict[str, int] = {}
    bytes_by_url: dict[str, int] = {}
    requests_by_task: dict[str, int] = {}
    bytes_by_task: dict[str, int] = {}
    request_ledger_paid = 0
    request_ledger_paid_by_url: dict[str, int] = {}
    request_ledger_paid_by_task: dict[str, int] = {}
    request_ledger_paid_by_task_try: dict[str, int] = {}
    request_paid_events: list[Mapping[str, Any]] = []
    request_ledger_paths = sorted(run_dir.glob("requests_*.jsonl"))
    (
        durable_by_source,
        durable_events,
        durable_request_records,
    ) = _validated_durable_request_evidence(
        ops_store,
        prefix=f"{traffic_prefix}/request-ledgers",
        dag_id=str(dag_id or ""),
        run_id=str(run_id or ""),
    )

    def consume_request_event(event: Mapping[str, Any], *, label: str) -> None:
        nonlocal request_ledger_paid
        counters = {
            field: event.get(field, 0)
            for field in ("request_bytes", "response_bytes", "paid_proxy_bytes")
        }
        if any(type(value) is not int or value < 0 for value in counters.values()):
            raise AirflowException(f"invalid byte counter in {label}")
        request_bytes = counters["request_bytes"]
        response_bytes = counters["response_bytes"]
        accounted_paid = counters["paid_proxy_bytes"]
        url = _canonical_traffic_url(event.get("url")) or "unknown"
        task_key = _traffic_task_key(event)
        task_try_key = _traffic_task_key(event, include_try=True)
        # Lease accounting is a byte-delta summary for the same logical
        # fetch, not a second HTTP request. Keep it as an independent
        # retry-safe paid source and exclude it from request counters.
        if str(event.get("status") or "") == "accounted":
            request_paid_events.append(event)
            request_ledger_paid += accounted_paid
            _counter_add(request_ledger_paid_by_url, url, accounted_paid)
            _counter_add(request_ledger_paid_by_task, task_key, accounted_paid)
            _counter_add(
                request_ledger_paid_by_task_try,
                task_try_key,
                accounted_paid,
            )
            return
        route = str(event.get("route") or "unknown")
        transferred = request_bytes + response_bytes
        _counter_add(route_requests, route, 1)
        _counter_add(route_bytes, route, transferred)
        _counter_add(requests_by_url, url, 1)
        _counter_add(bytes_by_url, url, transferred)
        _counter_add(requests_by_task, task_key, 1)
        _counter_add(bytes_by_task, task_key, transferred)

    for label, event in durable_events:
        consume_request_event(event, label=label)
    for path in request_ledger_paths:
        durable_identity = durable_by_source.get(path.name)
        if durable_identity is not None:
            local_identity = _file_sha256_and_bytes(path)
            if local_identity != durable_identity:
                raise AirflowException(
                    f"local/durable WhoScored request ledger mismatch for {path.name}"
                )
            # The immutable evidence is the same exact byte stream. Counting
            # both would duplicate discovery traffic.
            continue
        for event in _iter_jsonl_events(path, label="request ledger"):
            consume_request_event(event, label=f"request ledger {path}")

    durable_paid = 0
    durable_paid_by_url: dict[str, int] = {}
    durable_paid_by_task: dict[str, int] = {}
    durable_paid_by_task_try: dict[str, int] = {}
    paid_events: list[Mapping[str, Any]] = []
    paid_ledger = Path(PAID_LEDGER_PATH)
    if paid_ledger.exists():
        for event in _iter_jsonl_events(paid_ledger, label="paid ledger"):
            if str(event.get("dag_id") or "") == str(dag_id or "") and str(
                event.get("run_id") or ""
            ) == str(run_id or ""):
                paid_events.append(event)
            if (
                event.get("event_type") == "bytes"
                and str(event.get("dag_id") or "") == str(dag_id or "")
                and str(event.get("run_id") or "") == str(run_id or "")
            ):
                count = event.get("bytes", 0)
                if type(count) is not int or count <= 0:
                    raise AirflowException(
                        f"invalid non-positive byte event in paid ledger {paid_ledger}"
                    )
                url = _canonical_traffic_url(event.get("canonical_url"))
                if not url:
                    raise AirflowException(
                        f"paid ledger event for {dag_id}/{run_id} has no canonical URL"
                    )
                durable_paid += count
                _counter_add(durable_paid_by_url, url, count)
                _counter_add(
                    durable_paid_by_task,
                    _traffic_task_key(event),
                    count,
                )
                _counter_add(
                    durable_paid_by_task_try,
                    _traffic_task_key(event, include_try=True),
                    count,
                )

    if not transport.is_paid and (
        any((reported_paid, request_ledger_paid, durable_paid)) or paid_events
    ):
        raise AirflowException(
            "normal WhoScored DagRun used paid proxy: "
            f"report={reported_paid}, request_ledger={request_ledger_paid}, "
            f"proxy_ledger={durable_paid}"
        )
    if len({reported_paid, request_ledger_paid, durable_paid}) != 1:
        raise AirflowException(
            "WhoScored paid accounting mismatch: "
            f"report={reported_paid}, request_ledger={request_ledger_paid}, "
            f"proxy_ledger={durable_paid}"
        )
    paid = reported_paid
    campaign_evidence: dict[str, Any] = {}
    campaign_paid = _campaign_ledger_paid_bytes(
        transport,
        paid_events,
        request_paid_events,
        dag_id=str(dag_id or ""),
        run_id=str(run_id or ""),
        reconciliation_evidence=campaign_evidence,
    )
    campaign_snapshot_sha256 = campaign_evidence.get("campaign_snapshot_sha256")
    if campaign_paid != paid:
        raise AirflowException(
            "WhoScored paid accounting mismatch: "
            f"report={reported_paid}, request_ledger={request_ledger_paid}, "
            f"proxy_ledger={durable_paid}, campaign_ledger={campaign_paid}"
        )
    limit_bytes = transport.dagrun_limit_bytes
    if paid > limit_bytes:
        raise AirflowException(
            f"WhoScored DagRun signed paid proxy budget exceeded: "
            f"{paid}>{limit_bytes} bytes"
        )
    attribution_sources = (
        (
            durable_paid,
            durable_paid_by_url,
            durable_paid_by_task,
            durable_paid_by_task_try,
        ),
        (
            request_ledger_paid,
            request_ledger_paid_by_url,
            request_ledger_paid_by_task,
            request_ledger_paid_by_task_try,
        ),
        (
            reported_paid,
            reported_paid_by_url,
            reported_paid_by_task,
            reported_paid_by_task_try,
        ),
    )
    exact_attribution: list[tuple[dict[str, int], dict[str, int], dict[str, int]]] = []
    for (
        source_total,
        source_urls,
        source_tasks,
        source_task_tries,
    ) in attribution_sources:
        normalized = (
            dict(sorted((key, value) for key, value in source_urls.items() if value)),
            dict(sorted((key, value) for key, value in source_tasks.items() if value)),
            dict(
                sorted(
                    (key, value) for key, value in source_task_tries.items() if value
                )
            ),
        )
        if source_total != paid or any(
            sum(values.values()) != paid for values in normalized
        ):
            raise AirflowException(
                "WhoScored paid bytes cannot be attributed exactly by URL/task/try"
            )
        exact_attribution.append(normalized)
    if any(item != exact_attribution[0] for item in exact_attribution[1:]):
        raise AirflowException(
            "WhoScored paid attribution maps differ between report/request/proxy ledgers"
        )
    paid_by_url, paid_by_task, paid_by_task_try = exact_attribution[0]
    if paid and not paid_by_url:
        raise AirflowException(
            "WhoScored paid bytes cannot be attributed exactly to full URLs"
        )
    report_inputs = []
    for path in sorted(set(report_cleanup_paths)):
        digest, size = _file_sha256_and_bytes(path)
        report_inputs.append(
            {"source_name": path.name, "source_sha256": digest, "source_bytes": size}
        )
    request_inputs = [dict(item) for item in durable_request_records]
    for path in request_ledger_paths:
        if path.name in durable_by_source:
            continue
        digest, size = _file_sha256_and_bytes(path)
        request_inputs.append(
            {
                "source_name": path.name,
                "source_sha256": digest,
                "source_bytes": size,
                "durable": False,
            }
        )
    request_inputs.sort(key=lambda item: item["source_name"])
    input_evidence = {
        "schema_version": 1,
        "reports": report_inputs,
        "request_ledgers": request_inputs,
        "proxy_event_count": len(paid_events),
        "proxy_events_sha256": _proxy_events_sha256(paid_events),
        "campaign_snapshot_sha256": campaign_snapshot_sha256,
    }
    input_evidence_sha256 = hashlib.sha256(
        json.dumps(
            input_evidence,
            ensure_ascii=False,
            sort_keys=True,
            separators=(",", ":"),
        ).encode("utf-8")
    ).hexdigest()
    detail = {
        "schema_version": 2,
        "dag_id": str(dag_id or ""),
        "run_id": str(run_id or ""),
        "reconciliation_context": reconciliation_context,
        "input_evidence": input_evidence,
        "reports": len(reports),
        "paid_proxy_bytes": paid,
        "paid_proxy_mb": round(paid / 1_000_000, 6),
        "limit_bytes": limit_bytes,
        "per_url_limit_bytes": None,
        "reported_paid_proxy_bytes": reported_paid,
        "request_ledger_paid_proxy_bytes": request_ledger_paid,
        "durable_paid_proxy_bytes": durable_paid,
        "campaign_paid_proxy_bytes": campaign_paid,
        "paid_proxy_bytes_by_url": paid_by_url,
        "paid_proxy_bytes_by_task": paid_by_task,
        "paid_proxy_bytes_by_task_try": paid_by_task_try,
        "route_requests": route_requests,
        "route_bytes": route_bytes,
        "requests_by_url": dict(sorted(requests_by_url.items())),
        "bytes_by_url": dict(sorted(bytes_by_url.items())),
        "requests_by_task": dict(sorted(requests_by_task.items())),
        "bytes_by_task": dict(sorted(bytes_by_task.items())),
        "durable_request_ledgers": len(durable_request_records),
    }
    artifact = ops_store.put_content_addressed_json(
        f"{traffic_prefix}/detail",
        detail,
    )
    # Detailed reports and request ledgers are transient staging files. Delete
    # them only after every DQ consumer has finished (enforced by DAG topology)
    # and the immutable S3 artifact has passed read-back verification.
    cleanup_paths = report_cleanup_paths
    cleanup_paths.extend(request_ledger_paths)
    cleaned_bytes = sum(path.stat().st_size for path in cleanup_paths if path.exists())
    summary = {
        "schema_version": 1,
        "reports": len(reports),
        "paid_proxy_bytes": paid,
        "paid_proxy_mb": round(paid / 1_000_000, 6),
        "limit_bytes": limit_bytes,
        "per_url_limit_bytes": None,
        "reported_paid_proxy_bytes": reported_paid,
        "request_ledger_paid_proxy_bytes": request_ledger_paid,
        "durable_paid_proxy_bytes": durable_paid,
        "campaign_paid_proxy_bytes": campaign_paid,
        "request_count": sum(route_requests.values()),
        "wire_bytes": sum(route_bytes.values()),
        "routes": len(route_requests),
        "urls": len(requests_by_url),
        "tasks": len(requests_by_task),
        "paid_urls": len(paid_by_url),
        "durable_request_ledgers": len(durable_request_records),
        "artifact_uri": artifact["uri"],
        "artifact_sha256": artifact["sha256"],
        "artifact_bytes": artifact["bytes"],
        "cleaned_local_files": len(cleanup_paths),
        "cleaned_local_bytes": cleaned_bytes,
    }
    encoded_summary = json.dumps(summary, separators=(",", ":"), sort_keys=True).encode(
        "utf-8"
    )
    if len(encoded_summary) >= MAX_TRAFFIC_XCOM_BYTES:
        raise AirflowException(
            "WhoScored traffic summary exceeds the XCom contract: "
            f"{len(encoded_summary)} bytes"
        )
    _assert_staging_input_files(run_dir, input_evidence, require_all=True)
    durable_expected = sorted(
        (dict(item) for item in request_inputs if item.get("durable") is True),
        key=lambda item: item["evidence_key"],
    )
    _durable_sources, _durable_events, durable_current = (
        _validated_durable_request_evidence(
            ops_store,
            prefix=f"{traffic_prefix}/request-ledgers",
            dag_id=str(dag_id or ""),
            run_id=str(run_id or ""),
        )
    )
    refreshed_paid_events: list[Mapping[str, Any]] = []
    if Path(PAID_LEDGER_PATH).exists():
        refreshed_paid_events = [
            event
            for event in _iter_jsonl_events(Path(PAID_LEDGER_PATH), label="paid ledger")
            if str(event.get("dag_id") or "") == str(dag_id or "")
            and str(event.get("run_id") or "") == str(run_id or "")
        ]
    if (
        durable_current != durable_expected
        or len(refreshed_paid_events) != input_evidence["proxy_event_count"]
        or _proxy_events_sha256(refreshed_paid_events)
        != input_evidence["proxy_events_sha256"]
        or _current_campaign_snapshot_sha256(transport)
        != input_evidence["campaign_snapshot_sha256"]
    ):
        raise AirflowException(
            "WhoScored reconciliation inputs changed before durable completion"
        )
    # The completion receipt is durable before cleanup. If the task process
    # dies between these steps, its retry consumes this receipt and finishes
    # the same safe local deletions without requiring the original reports.
    ops_store.put_content_addressed_json(
        f"{traffic_prefix}/completion",
        {
            "schema_version": 2,
            "dag_id": str(dag_id or ""),
            "run_id": str(run_id or ""),
            "reconciliation_context": reconciliation_context,
            "input_evidence_sha256": input_evidence_sha256,
            "summary": summary,
            "detail_artifact": artifact,
            "cleanup_files": [path.name for path in cleanup_paths],
        },
    )
    for path in cleanup_paths:
        try:
            path.unlink()
        except FileNotFoundError:
            continue
        except OSError as exc:
            raise AirflowException(
                f"cannot remove durable WhoScored traffic staging file {path}: {exc}"
            ) from exc
    return summary


def enforce_terminal_gate(**context: Any) -> Dict[str, Any]:
    """Prevent ``all_done`` diagnostics from turning a failed run green."""
    dag_run = context.get("dag_run")
    current_ti = context.get("ti")
    if dag_run is None:
        raise AirflowException("terminal gate requires dag_run context")
    current_task_id = getattr(current_ti, "task_id", "final_success_gate")
    failures: list[str] = []
    for task_instance in dag_run.get_task_instances():
        if task_instance.task_id == current_task_id:
            continue
        state = str(task_instance.state or "none").lower().split(".")[-1]
        if state != "success":
            suffix = (
                f"[{task_instance.map_index}]" if task_instance.map_index >= 0 else ""
            )
            failures.append(f"{task_instance.task_id}{suffix}={state}")
    if failures:
        raise AirflowException(
            "WhoScored upstream/DQ tasks were not successful: " + ", ".join(failures)
        )
    return {
        "status": "success",
        "checked_task_instances": len(dag_run.get_task_instances()) - 1,
    }


with DAG(
    dag_id="dag_ingest_whoscored",
    default_args=WHOSCORED_ARGS,
    description="Discover and incrementally ingest all active senior-men WhoScored scopes",
    schedule=SCHEDULES.get("dag_ingest_whoscored", "0 10 * * *"),
    start_date=datetime(2024, 1, 1),
    catchup=False,
    is_paused_upon_creation=True,
    max_active_runs=1,
    dagrun_timeout=timedelta(hours=COLD_DAGRUN_HARD_LIMIT_HOURS),
    # Task-level callbacks on a mapped fan-out produce one page per failed
    # map index.  A DAG-level callback reports the failed DagRun exactly once.
    on_failure_callback=SCRAPER_ARGS.get("on_failure_callback"),
    params={
        "transport_policy": "direct_only",
        "paid_approval_id": "",
        "paid_approval_sha256": "",
        "require_zero_paid": True,
        "direct_only": True,
    },
    user_defined_filters={"stable_safe_token": stable_safe_token},
    tags=DAG_TAGS.get("whoscored", ["scraping", "whoscored", "bronze"]),
    doc_md="""
    Runtime discovery publishes the complete source catalog. Active eligible
    scopes are then dynamically mapped; a missing/quarantined catalog fails at
    runtime without breaking DAG import. Normal runs require zero paid bytes,
    rolling scheduled-run p95 <=4h, and a hard cold-bootstrap ceiling of 6h.
    """,
) as dag:
    runtime_preflight = PythonOperator(
        task_id="validate_whoscored_runtime",
        python_callable=validate_whoscored_runtime,
        execution_timeout=timedelta(minutes=5),
    )
    paid_alert_preflight = PythonOperator(
        task_id="validate_whoscored_paid_alert_delivery",
        python_callable=validate_transport_alert_delivery,
        retries=0,
        execution_timeout=timedelta(minutes=1),
    )
    build_discovery = PythonOperator(
        task_id="build_whoscored_discovery_command",
        python_callable=build_daily_discovery_command,
        op_kwargs={"alert_metadata": paid_alert_preflight.output},
        execution_timeout=timedelta(minutes=5),
    )
    discover_catalog = BashOperator(
        task_id="discover_whoscored_catalog",
        bash_command="{{ ti.xcom_pull(task_ids='build_whoscored_discovery_command') }}",
        env=_TASK_ENV,
        append_env=True,
        pool=DIRECT_POOL,
        priority_weight=100,
        # Keep one task report per source attempt. Transport-level retries and
        # the raw cache own recovery; an Airflow retry would overwrite the
        # report while the request/proxy ledgers correctly retain paid bytes.
        retries=0,
        do_xcom_push=False,
        # A cold source crawl can legitimately exceed the incremental two-hour
        # envelope; a later DagRun resumes from the append-only raw cache.
        execution_timeout=timedelta(hours=4),
    )

    catalog_dq = PythonOperator(
        task_id="validate_whoscored_catalog",
        python_callable=validate_catalog_result,
        trigger_rule="all_done",
        execution_timeout=timedelta(minutes=5),
    )

    initialize_schema = PythonOperator(
        task_id="initialize_whoscored_schema",
        python_callable=initialize_whoscored_schema,
        pool=DIRECT_POOL,
        retries=1,
        retry_delay=timedelta(minutes=5),
        execution_timeout=timedelta(minutes=15),
    )

    freeze_scope_plan = PythonOperator(
        task_id="freeze_daily_scope_plan",
        python_callable=freeze_daily_scope_plan,
        op_kwargs={"validated_catalog": catalog_dq.output},
        pool=DQ_POOL,
        execution_timeout=timedelta(minutes=10),
    )

    if not callable(getattr(BashOperator, "partial", None)):
        raise RuntimeError("WhoScored daily ingest requires Airflow dynamic mapping")
    build_commands = PythonOperator(
        task_id="build_active_scope_commands",
        python_callable=build_daily_commands,
        op_kwargs={
            "scope_plan": freeze_scope_plan.output,
            "alert_metadata": paid_alert_preflight.output,
        },
    )
    build_dq = PythonOperator(
        task_id="build_scope_dq_inputs",
        python_callable=build_scope_validation_kwargs,
        op_kwargs={"scope_plan": freeze_scope_plan.output},
    )
    ingest_scopes = BashOperator.partial(
        task_id="ingest_active_scope",
        env=_TASK_ENV,
        append_env=True,
        pool=DIRECT_POOL,
        priority_weight=100,
        # Entity retry_after timestamps are durable in the manifests.
        # An earlier Airflow retry would see no due candidates and could
        # overwrite the failed report with a false-green empty run.
        retries=0,
        # World Cup qualification exposes many active stages and is the
        # measured worst case: bounded browser batches plus atomic Iceberg
        # staging can exceed 45 minutes on a cold scope. Completed table
        # batches are resumable, but the task needs enough time to publish
        # the final scope manifest in one attempt.
        execution_timeout=timedelta(minutes=75),
        do_xcom_push=False,
    ).expand(bash_command=build_commands.output)
    scope_dq = PythonOperator.partial(
        task_id="validate_active_scope",
        python_callable=validate_scope_result,
        trigger_rule="all_done",
        pool=DQ_POOL,
        execution_timeout=timedelta(minutes=10),
    ).expand(op_kwargs=build_dq.output)
    freeze_scope_plan >> [build_commands, build_dq]
    build_commands >> ingest_scopes
    [ingest_scopes, build_dq] >> scope_dq
    profile_capacity = PythonOperator(
        task_id="plan_daily_profile_capacity",
        python_callable=plan_daily_profile_capacity,
        op_kwargs={"scope_plan": freeze_scope_plan.output},
        pool=DQ_POOL,
        execution_timeout=timedelta(minutes=10),
    )
    profile_command = PythonOperator(
        task_id="build_daily_profile_command",
        python_callable=build_daily_profile_command,
        op_kwargs={
            "scope_plan": freeze_scope_plan.output,
            "capacity": profile_capacity.output,
            "alert_metadata": paid_alert_preflight.output,
        },
        execution_timeout=timedelta(minutes=5),
    )
    profile_task = BashOperator(
        task_id="refresh_whoscored_profiles",
        bash_command="{{ ti.xcom_pull(task_ids='build_daily_profile_command') }}",
        env=_TASK_ENV,
        append_env=True,
        pool=DIRECT_POOL,
        priority_weight=100,
        # Profile retry_after is 24 hours; let the next scheduled DagRun own
        # recovery so a premature retry cannot hide a persisted failure.
        retries=0,
        execution_timeout=timedelta(hours=2),
        trigger_rule="all_success",
        do_xcom_push=False,
    )
    profile_dq = PythonOperator(
        task_id="validate_profile_refresh",
        python_callable=validate_profile_result,
        op_kwargs={
            "scope_plan": freeze_scope_plan.output,
            "capacity": profile_capacity.output,
        },
        trigger_rule="all_done",
        pool=DQ_POOL,
    )
    traffic_dq = PythonOperator(
        task_id="report_whoscored_traffic",
        python_callable=aggregate_traffic_reports,
        trigger_rule="all_done",
        pool=DQ_POOL,
    )
    daily_slo = PythonOperator(
        task_id="validate_whoscored_daily_slo",
        python_callable=validate_whoscored_daily_slo,
        trigger_rule="all_done",
        pool=DQ_POOL,
        execution_timeout=timedelta(minutes=5),
    )
    final_gate = PythonOperator(
        task_id="final_success_gate",
        python_callable=enforce_terminal_gate,
        trigger_rule="all_done",
        execution_timeout=timedelta(minutes=5),
    )

    runtime_preflight >> paid_alert_preflight >> initialize_schema >> build_discovery
    build_discovery >> discover_catalog >> catalog_dq
    catalog_dq >> freeze_scope_plan
    freeze_scope_plan >> profile_capacity >> profile_command
    ingest_scopes >> profile_task
    profile_command >> profile_task >> profile_dq
    [catalog_dq, scope_dq, profile_dq, traffic_dq] >> daily_slo
    [catalog_dq, scope_dq, profile_dq, traffic_dq, daily_slo] >> final_gate
    # Traffic upload/cleanup runs only after result-consuming DQ tasks, so
    # deleting the large local JSON reports cannot race their readers.
    [catalog_dq, scope_dq, profile_dq] >> traffic_dq
