"""Daily production ingestion for the persisted WhoScored catalog.

Catalog access happens only in runtime tasks.  DAG parsing therefore remains
safe while Trino is restarting or before the additive catalog migration has
run, and there is no static fallback to the historical six competitions.
"""

from __future__ import annotations

import hashlib
import json
import math
import os
import re
import shlex
import shutil
from datetime import datetime, timedelta
from pathlib import Path
from typing import Any, Dict, Mapping, Optional, Sequence
from urllib.parse import parse_qsl, urlencode, urlsplit, urlunsplit

from airflow import DAG
from airflow.exceptions import AirflowException
from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator

from dags.scripts.whoscored_identity import stable_safe_token

from utils.config import DAG_TAGS, SCHEDULES
from utils.default_args import SCRAPER_ARGS


RUN_ROOT = "/opt/airflow/logs/whoscored_runs"
_RUN_DIR_TEMPLATE = (
    RUN_ROOT + "/{{ dag.dag_id | stable_safe_token }}/"
    "{{ run_id | stable_safe_token }}"
)
PAID_DAGRUN_LIMIT_BYTES = 8_000_000
PAID_URL_LIMIT_BYTES = 2_000_000
PAID_LEDGER_PATH = os.environ.get(
    "PROXY_FILTER_LEDGER_PATH",
    "/opt/airflow/logs/proxy_filter/paid_requests.jsonl",
)
MAX_LEDGER_EVENT_BYTES = 256 * 1024
MAX_TRAFFIC_XCOM_BYTES = 256 * 1024
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
        if key not in {"pool", "retries", "retry_delay", "execution_timeout"}
    },
    "retries": 0,
    "execution_timeout": timedelta(minutes=20),
}

_TASK_ENV = {
    "PYTHONPATH": "/opt/airflow:/opt/airflow/dags",
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
    root = Path(RUN_ROOT)
    if not root.exists():
        return {"removed_directories": 0, "removed_bytes": 0}
    cutoff = (now or datetime.now()).timestamp() - retention_days * 86400
    current = current_run_dir.resolve() if current_run_dir is not None else None
    removed_directories = 0
    removed_bytes = 0
    for dag_dir in root.iterdir():
        if not dag_dir.is_dir():
            continue
        for run_dir in dag_dir.iterdir():
            if not run_dir.is_dir() or (
                current is not None and run_dir.resolve() == current
            ):
                continue
            try:
                if run_dir.stat().st_mtime >= cutoff:
                    continue
                removed_bytes += sum(
                    path.stat().st_size for path in run_dir.rglob("*") if path.is_file()
                )
                shutil.rmtree(run_dir)
            except FileNotFoundError:
                continue
            except OSError as exc:
                raise AirflowException(
                    f"cannot remove stale WhoScored run directory {run_dir}: {exc}"
                ) from exc
            removed_directories += 1
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


def freeze_daily_scope_plan() -> Dict[str, Any]:
    """Bind one exact catalog generation for every task in this DagRun."""

    from dags.scripts import run_whoscored_scraper as runner

    repository = runner._new_repository()
    generation, catalog = repository.load_catalog_generation_snapshot()
    selected = runner._select_catalog_snapshot_scopes(
        catalog,
        [],
        active_only=True,
    )
    scopes = sorted(scope.spec for scope, _runtime in selected)
    if not scopes or len(scopes) != len(set(scopes)):
        raise AirflowException("frozen WhoScored catalog has invalid active scopes")
    batch_id = str(generation.get("catalog_batch_id") or "")
    payload_sha256 = str(generation.get("catalog_payload_sha256") or "")
    if not re.fullmatch(r"[A-Za-z0-9_.-]{1,120}", batch_id):
        raise AirflowException("frozen WhoScored catalog has invalid batch id")
    if re.fullmatch(r"[0-9a-f]{64}", payload_sha256) is None:
        raise AirflowException("frozen WhoScored catalog has invalid payload hash")
    scopes_sha256 = hashlib.sha256(
        ("\n".join(scopes) + "\n").encode("utf-8")
    ).hexdigest()
    return {
        "schema_version": 1,
        "catalog_batch_id": batch_id,
        "catalog_payload_sha256": payload_sha256,
        "active_scopes": scopes,
        "active_scope_count": len(scopes),
        "active_scopes_sha256": scopes_sha256,
    }


def _daily_scope_plan_specs(scope_plan: Mapping[str, Any]) -> tuple[str, list[str]]:
    if set(scope_plan) != {
        "schema_version",
        "catalog_batch_id",
        "catalog_payload_sha256",
        "active_scopes",
        "active_scope_count",
        "active_scopes_sha256",
    }:
        raise AirflowException("invalid frozen WhoScored daily scope plan schema")
    batch_id = str(scope_plan.get("catalog_batch_id") or "")
    payload_sha256 = str(scope_plan.get("catalog_payload_sha256") or "")
    scopes = scope_plan.get("active_scopes")
    if (
        scope_plan.get("schema_version") != 1
        or not re.fullmatch(r"[A-Za-z0-9_.-]{1,120}", batch_id)
        or re.fullmatch(r"[0-9a-f]{64}", payload_sha256) is None
        or not isinstance(scopes, list)
        or not scopes
        or any(not isinstance(value, str) or not value for value in scopes)
        or scopes != sorted(set(scopes))
        or scope_plan.get("active_scope_count") != len(scopes)
        or scope_plan.get("active_scopes_sha256")
        != hashlib.sha256(("\n".join(scopes) + "\n").encode("utf-8")).hexdigest()
    ):
        raise AirflowException("invalid frozen WhoScored daily scope plan identity")
    return batch_id, list(scopes)


def initialize_whoscored_schema() -> Dict[str, Any]:
    """Run additive schema evolution once before any dynamically mapped writer."""
    from scrapers.whoscored.repository import WhoScoredRepository

    WhoScoredRepository().ensure_schema(create_views=True)
    return {"status": "success"}


def validate_whoscored_runtime(**context: Any) -> Dict[str, Any]:
    """Fail unless this release runs direct-only on one LocalExecutor host."""

    executor = os.environ.get("AIRFLOW__CORE__EXECUTOR", "").strip()
    if not executor.endswith("LocalExecutor"):
        raise AirflowException(
            "WhoScored production requires AIRFLOW__CORE__EXECUTOR=LocalExecutor; "
            f"got {executor or 'unset'}"
        )
    params = context.get("params", {})
    if not bool(params.get("direct_only", True)):
        raise AirflowException(
            "dag_ingest_whoscored is permanently direct-only; paid transport is "
            "available only through an explicitly approved manual replay CLI"
        )
    if not bool(params.get("require_zero_paid", True)):
        raise AirflowException("daily WhoScored must enforce zero paid proxy bytes")
    return {"status": "success", "executor": executor, "direct_only": True}


def build_daily_commands(
    *,
    scope_plan: Optional[Mapping[str, Any]] = None,
    **context: Any,
) -> list[str]:
    """Return one safely quoted CLI command per active persisted scope."""
    run_dir = _run_dir_from_context(context)
    cleanup_stale_run_directories(current_run_dir=run_dir)
    direct_flag = " --direct-only"
    commands: list[str] = []
    if scope_plan is None:
        batch_id = None
        scopes = _active_scope_specs()
    else:
        batch_id, scopes = _daily_scope_plan_specs(scope_plan)
    for scope in scopes:
        output = _scope_result_path(run_dir, scope)
        catalog_arg = f" --catalog-batch-id {shlex.quote(batch_id)}" if batch_id else ""
        commands.append(
            "cd /opt/airflow && "
            "python dags/scripts/run_whoscored_scraper.py daily "
            f"--scope {shlex.quote(scope)} --skip-profiles "
            f"--output {shlex.quote(str(output))}{catalog_arg}{direct_flag}"
        )
    return commands


def build_scope_validation_kwargs(
    *,
    scope_plan: Optional[Mapping[str, Any]] = None,
    **context: Any,
) -> list[dict[str, Any]]:
    run_dir = _run_dir_from_context(context)
    require_zero = bool(context.get("params", {}).get("require_zero_paid", True))
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
    return (
        "cd /opt/airflow && "
        "python dags/scripts/run_whoscored_scraper.py daily --profiles-only "
        f"{scope_args} --catalog-batch-id {shlex.quote(batch_id)} "
        f"--profiles-limit {selected_limit} --output {shlex.quote(str(output))} "
        f"--expected-profile-candidate-count {selected_limit} "
        f"--expected-profile-candidate-sha256 {candidate_sha256} "
        "--direct-only"
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


def _scope_integrity_summary(scope_spec: str) -> Dict[str, int]:
    competition_id, separator, season_id = scope_spec.rpartition("=")
    if not separator or not competition_id or not season_id:
        raise AirflowException(f"invalid persisted scope {scope_spec!r}")

    from scrapers.whoscored.parsers import MATCH_AVAILABILITY_VERSION, PARSER_VERSION
    from utils.silver_tasks import _get_trino_connection

    league_sql = _sql_string(competition_id)
    season_sql = _sql_string(season_id)
    conn = _get_trino_connection()
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
    from utils.silver_tasks import _get_trino_connection

    conn = _get_trino_connection()
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


def _catalog_integrity_summary() -> Dict[str, int]:
    """Check that the current catalog is complete and physically atomic."""

    from utils.silver_tasks import _get_trino_connection

    conn = _get_trino_connection()
    try:
        cur = conn.cursor()
        try:
            cur.execute(
                """
                WITH latest AS (
                    SELECT * FROM iceberg.bronze.whoscored_catalog_manifest
                    WHERE state = 'success'
                    ORDER BY completed_at DESC, _ingested_at DESC, batch_id DESC
                    LIMIT 1
                ),
                competitions AS (
                    SELECT c.* FROM iceberg.bronze.whoscored_competitions c
                    JOIN latest m ON m.batch_id = c._catalog_batch_id
                ),
                seasons AS (
                    SELECT s.* FROM iceberg.bronze.whoscored_seasons s
                    JOIN latest m ON m.batch_id = s._catalog_batch_id
                ),
                stages AS (
                    SELECT s.* FROM iceberg.bronze.whoscored_stages s
                    JOIN latest m ON m.batch_id = s._catalog_batch_id
                )
                SELECT
                    (SELECT competitions_count FROM latest),
                    (SELECT COUNT(*) FROM competitions),
                    (SELECT COUNT(DISTINCT record_key) FROM competitions),
                    (SELECT seasons_count FROM latest),
                    (SELECT COUNT(*) FROM seasons),
                    (SELECT COUNT(DISTINCT record_key) FROM seasons),
                    (SELECT stages_count FROM latest),
                    (SELECT COUNT(*) FROM stages),
                    (SELECT COUNT(DISTINCT record_key) FROM stages),
                    (SELECT quarantined_count FROM latest),
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
                """
                SELECT batch_id, payload_sha256, parser_version, schema_fingerprint
                FROM iceberg.bronze.whoscored_catalog_manifest
                WHERE state = 'success'
                ORDER BY completed_at DESC, _ingested_at DESC, batch_id DESC
                LIMIT 1
                """
            )
            identity_rows = cur.fetchall()
            identity_valid = 0
            if identity_rows:
                batch_id, payload_sha256, parser_version, schema_fingerprint = (
                    identity_rows[0]
                )
                physical: dict[str, list[dict[str, Any]]] = {}
                try:
                    for kind in ("competitions", "seasons", "stages"):
                        cur.execute(
                            f"SELECT payload_json FROM "
                            f"iceberg.bronze.whoscored_{kind} "
                            "WHERE _catalog_batch_id = "
                            f"{_sql_string(str(batch_id))} ORDER BY record_key"
                        )
                        physical[kind] = [
                            json.loads(str(value)) for (value,) in cur.fetchall()
                        ]
                    from scrapers.whoscored.catalog import WhoScoredCatalog
                    from scrapers.whoscored.parsers import PARSER_VERSION
                    from scrapers.whoscored.repository import catalog_payload_sha256

                    catalog = WhoScoredCatalog.from_rows(physical)
                    physical_fingerprint = catalog_payload_sha256(catalog.to_rows())
                    identity_valid = int(
                        str(payload_sha256 or "") == physical_fingerprint
                        and str(schema_fingerprint or "") == physical_fingerprint
                        and str(parser_version or "") == PARSER_VERSION
                    )
                except (TypeError, ValueError):
                    identity_valid = 0
        finally:
            cur.close()
    finally:
        conn.close()
    return {
        "manifest_competitions": int(row[0] or 0),
        "physical_competitions": int(row[1] or 0),
        "distinct_competitions": int(row[2] or 0),
        "manifest_seasons": int(row[3] or 0),
        "physical_seasons": int(row[4] or 0),
        "distinct_seasons": int(row[5] or 0),
        "manifest_stages": int(row[6] or 0),
        "physical_stages": int(row[7] or 0),
        "distinct_stages": int(row[8] or 0),
        "quarantined": int(row[9] or 0),
        "eligible_seasons_without_stages": int(row[10] or 0),
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
    if paid > PAID_DAGRUN_LIMIT_BYTES:
        raise AirflowException(f"paid proxy budget exceeded: {paid} bytes")
    if require_zero_paid and paid:
        raise AirflowException(
            f"normal daily run used paid proxy for {scope_spec}: {paid} bytes"
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
    return {"scope": scope_spec, "paid_proxy_bytes": paid, **integrity}


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
    if paid > PAID_DAGRUN_LIMIT_BYTES:
        raise AirflowException(f"profile paid budget exceeded: {paid} bytes")
    if context.get("params", {}).get("require_zero_paid", True) and paid:
        raise AirflowException(f"normal profile refresh used paid proxy: {paid} bytes")
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
    return {"status": "success", "paid_proxy_bytes": paid, **integrity}


def validate_catalog_result(**context: Any) -> Dict[str, Any]:
    path = _run_dir_from_context(context) / "discovery.json"
    result = _load_result(str(path))
    if result.get("status") != "success":
        raise AirflowException(
            f"WhoScored discovery failed: {result.get('errors', [])}"
        )
    integrity = _catalog_integrity_summary()
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
    if integrity["quarantined"] or integrity["eligible_seasons_without_stages"]:
        raise AirflowException(f"WhoScored catalog is incomplete: {integrity}")
    if integrity["manifest_identity_valid"] != 1:
        raise AirflowException(
            f"WhoScored catalog manifest identity verification failed: {integrity}"
        )
    scopes = _active_scope_specs()
    if not scopes:
        raise AirflowException("WhoScored catalog has no active adult men's scopes")
    return {"status": "success", "active_scopes": len(scopes), **integrity}


def aggregate_traffic_reports(
    *,
    allow_empty: bool = False,
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
    completion_records = list(
        ops_store.iter_content_addressed_json(f"{traffic_prefix}/completion")
    )
    if completion_records:
        summaries = []
        for _key, receipt in completion_records:
            detail_artifact = receipt.get("detail_artifact")
            if (
                receipt.get("schema_version") != 1
                or receipt.get("dag_id") != str(dag_id or "")
                or receipt.get("run_id") != str(run_id or "")
                or not isinstance(receipt.get("summary"), Mapping)
                or not isinstance(receipt.get("cleanup_files"), list)
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
            ops_store.read_content_addressed_json(
                detail_key,
                expected_sha256=detail_sha,
                expected_bytes=detail_bytes,
            )
            summary = receipt["summary"]
            if (
                summary.get("artifact_uri") != detail_artifact.get("uri")
                or summary.get("artifact_sha256") != detail_sha
                or summary.get("artifact_bytes") != detail_bytes
            ):
                raise AirflowException(
                    "WhoScored traffic completion/detail identity mismatch"
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

    reports = []
    for path in sorted(run_dir.glob("*.json")):
        try:
            result = _load_result(str(path))
        except AirflowException:
            continue
        reports.append((path.name, result))
    if not reports and not allow_empty:
        raise AirflowException(f"no WhoScored reports found in {run_dir}")

    reported_paid = 0
    reported_paid_by_url: dict[str, int] = {}
    reported_paid_by_task: dict[str, int] = {}
    reported_paid_by_task_try: dict[str, int] = {}
    for name, result in reports:
        try:
            report_paid = max(0, int(result.get("paid_proxy_bytes") or 0))
        except (TypeError, ValueError) as exc:
            raise AirflowException(
                f"invalid paid_proxy_bytes in WhoScored report {name}"
            ) from exc
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
                try:
                    count = max(0, int(raw_count or 0))
                except (TypeError, ValueError) as exc:
                    raise AirflowException(
                        f"invalid paid URL bytes in WhoScored report {name}"
                    ) from exc
                if url and count:
                    _counter_add(reported_paid_by_url, url, count)

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
    request_ledger_paths = sorted(run_dir.glob("requests_*.jsonl"))
    durable_request_ledgers = list(
        ops_store.iter_content_addressed_json(
            f"{traffic_prefix}/request-ledgers"
        )
    )
    durable_by_source: dict[str, tuple[str, int]] = {}
    durable_events: list[tuple[str, Mapping[str, Any]]] = []
    durable_event_ids: set[str] = set()
    for key, evidence in durable_request_ledgers:
        source_name = evidence.get("source_name")
        source_sha256 = evidence.get("source_sha256")
        source_bytes = evidence.get("source_bytes")
        events = evidence.get("events")
        if (
            evidence.get("schema_version") != 1
            or evidence.get("evidence_type") != "whoscored_request_ledger"
            or evidence.get("dag_id") != str(dag_id or "")
            or evidence.get("run_id") != str(run_id or "")
            or not isinstance(source_name, str)
            or Path(source_name).name != source_name
            or not source_name.startswith("requests_")
            or not source_name.endswith(".jsonl")
            or not isinstance(source_sha256, str)
            or re.fullmatch(r"[0-9a-f]{64}", source_sha256) is None
            or type(source_bytes) is not int
            or source_bytes < 0
            or not isinstance(events, list)
            or evidence.get("event_count") != len(events)
        ):
            raise AirflowException(
                f"invalid durable WhoScored request ledger evidence {key}"
            )
        source_identity = (source_sha256, source_bytes)
        if source_name in durable_by_source:
            if durable_by_source[source_name] != source_identity:
                raise AirflowException(
                    "conflicting durable WhoScored request ledgers for "
                    f"{source_name}"
                )
            raise AirflowException(
                f"duplicate durable WhoScored request ledger {source_name}"
            )
        durable_by_source[source_name] = source_identity
        expected_identity = {
            "dag_id": evidence.get("dag_id"),
            "run_id": evidence.get("run_id"),
            "task_id": evidence.get("task_id"),
            "map_index": evidence.get("map_index"),
            "try_number": evidence.get("try_number"),
        }
        reconstructed_digest = hashlib.sha256()
        reconstructed_bytes = 0
        reconstructed_requests = 0
        reconstructed_wire_bytes = 0
        reconstructed_paid_bytes = 0
        for event in events:
            event_id = event.get("event_id") if isinstance(event, Mapping) else None
            if (
                not isinstance(event, Mapping)
                or event.get("event_version") != "whoscored-request-v1"
                or not isinstance(event_id, str)
                or re.fullmatch(r"[0-9a-f]{32}", event_id) is None
                or any(event.get(field) != value for field, value in expected_identity.items())
                or event_id in durable_event_ids
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
            counters: dict[str, int] = {}
            for field in ("request_bytes", "response_bytes", "paid_proxy_bytes"):
                value = event.get(field, 0)
                if type(value) is not int or value < 0:
                    raise AirflowException(
                        f"invalid {field} in durable WhoScored request ledger {key}"
                    )
                counters[field] = value
            if str(event.get("status") or "") == "accounted":
                reconstructed_paid_bytes += counters["paid_proxy_bytes"]
            else:
                if counters["paid_proxy_bytes"]:
                    raise AirflowException(
                        "unaccounted paid bytes in durable WhoScored request "
                        f"ledger {key}"
                    )
                reconstructed_requests += 1
                reconstructed_wire_bytes += (
                    counters["request_bytes"] + counters["response_bytes"]
                )
            durable_event_ids.add(event_id)
            durable_events.append((f"durable request ledger {key}", event))
        if (
            reconstructed_digest.hexdigest() != source_sha256
            or reconstructed_bytes != source_bytes
            or evidence.get("request_count") != reconstructed_requests
            or evidence.get("wire_bytes") != reconstructed_wire_bytes
            or evidence.get("paid_proxy_bytes") != reconstructed_paid_bytes
        ):
            raise AirflowException(
                f"durable WhoScored request ledger summary mismatch {key}"
            )

    def consume_request_event(event: Mapping[str, Any], *, label: str) -> None:
        nonlocal request_ledger_paid
        try:
            request_bytes = max(0, int(event.get("request_bytes") or 0))
            response_bytes = max(0, int(event.get("response_bytes") or 0))
            accounted_paid = max(0, int(event.get("paid_proxy_bytes") or 0))
        except (TypeError, ValueError) as exc:
            raise AirflowException(f"invalid byte counter in {label}") from exc
        url = _canonical_traffic_url(event.get("url")) or "unknown"
        task_key = _traffic_task_key(event)
        task_try_key = _traffic_task_key(event, include_try=True)
        # Lease accounting is a byte-delta summary for the same logical
        # fetch, not a second HTTP request. Keep it as an independent
        # retry-safe paid source and exclude it from request counters.
        if str(event.get("status") or "") == "accounted":
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
                    "local/durable WhoScored request ledger mismatch for "
                    f"{path.name}"
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
    paid_ledger = Path(PAID_LEDGER_PATH)
    if paid_ledger.exists():
        for event in _iter_jsonl_events(paid_ledger, label="paid ledger"):
            if (
                event.get("event_type") == "bytes"
                and str(event.get("dag_id") or "") == str(dag_id or "")
                and str(event.get("run_id") or "") == str(run_id or "")
            ):
                try:
                    count = int(event.get("bytes") or 0)
                except (TypeError, ValueError) as exc:
                    raise AirflowException(
                        f"invalid byte counter in paid ledger {paid_ledger}"
                    ) from exc
                if count <= 0:
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

    paid = max(reported_paid, request_ledger_paid, durable_paid)
    if paid > PAID_DAGRUN_LIMIT_BYTES:
        raise AirflowException(
            f"WhoScored DagRun paid proxy budget exceeded: {paid} bytes"
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
    paid_by_url: dict[str, int] = {}
    paid_by_task: dict[str, int] = {}
    paid_by_task_try: dict[str, int] = {}
    for (
        source_total,
        source_urls,
        source_tasks,
        source_task_tries,
    ) in attribution_sources:
        if source_total == paid and sum(source_urls.values()) == paid:
            paid_by_url = dict(sorted(source_urls.items()))
            paid_by_task = dict(sorted(source_tasks.items()))
            paid_by_task_try = dict(sorted(source_task_tries.items()))
            break
    if paid and not paid_by_url:
        raise AirflowException(
            "WhoScored paid bytes cannot be attributed exactly to full URLs"
        )
    for url, count in paid_by_url.items():
        if count > PAID_URL_LIMIT_BYTES:
            raise AirflowException(
                f"WhoScored paid proxy URL budget exceeded for {url}: {count} bytes"
            )
    require_zero = bool(context.get("params", {}).get("require_zero_paid", True))
    if require_zero and paid:
        raise AirflowException(f"normal WhoScored DagRun used paid proxy: {paid} bytes")
    detail = {
        "schema_version": 1,
        "dag_id": str(dag_id or ""),
        "run_id": str(run_id or ""),
        "reports": len(reports),
        "paid_proxy_bytes": paid,
        "paid_proxy_mb": round(paid / 1_000_000, 6),
        "limit_bytes": PAID_DAGRUN_LIMIT_BYTES,
        "per_url_limit_bytes": PAID_URL_LIMIT_BYTES,
        "reported_paid_proxy_bytes": reported_paid,
        "request_ledger_paid_proxy_bytes": request_ledger_paid,
        "durable_paid_proxy_bytes": durable_paid,
        "paid_proxy_bytes_by_url": paid_by_url,
        "paid_proxy_bytes_by_task": paid_by_task,
        "paid_proxy_bytes_by_task_try": paid_by_task_try,
        "route_requests": route_requests,
        "route_bytes": route_bytes,
        "requests_by_url": dict(sorted(requests_by_url.items())),
        "bytes_by_url": dict(sorted(bytes_by_url.items())),
        "requests_by_task": dict(sorted(requests_by_task.items())),
        "bytes_by_task": dict(sorted(bytes_by_task.items())),
        "durable_request_ledgers": len(durable_request_ledgers),
    }
    artifact = ops_store.put_content_addressed_json(
        f"{traffic_prefix}/detail",
        detail,
    )
    # Detailed reports and request ledgers are transient staging files. Delete
    # them only after every DQ consumer has finished (enforced by DAG topology)
    # and the immutable S3 artifact has passed read-back verification.
    cleanup_paths = [run_dir / name for name, _result in reports]
    cleanup_paths.extend(request_ledger_paths)
    cleaned_bytes = sum(path.stat().st_size for path in cleanup_paths if path.exists())
    summary = {
        "schema_version": 1,
        "reports": len(reports),
        "paid_proxy_bytes": paid,
        "paid_proxy_mb": round(paid / 1_000_000, 6),
        "limit_bytes": PAID_DAGRUN_LIMIT_BYTES,
        "per_url_limit_bytes": PAID_URL_LIMIT_BYTES,
        "reported_paid_proxy_bytes": reported_paid,
        "request_ledger_paid_proxy_bytes": request_ledger_paid,
        "durable_paid_proxy_bytes": durable_paid,
        "request_count": sum(route_requests.values()),
        "wire_bytes": sum(route_bytes.values()),
        "routes": len(route_requests),
        "urls": len(requests_by_url),
        "tasks": len(requests_by_task),
        "paid_urls": len(paid_by_url),
        "durable_request_ledgers": len(durable_request_ledgers),
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
    # The completion receipt is durable before cleanup. If the task process
    # dies between these steps, its retry consumes this receipt and finishes
    # the same safe local deletions without requiring the original reports.
    ops_store.put_content_addressed_json(
        f"{traffic_prefix}/completion",
        {
            "schema_version": 1,
            "dag_id": str(dag_id or ""),
            "run_id": str(run_id or ""),
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
    max_active_runs=1,
    dagrun_timeout=timedelta(hours=COLD_DAGRUN_HARD_LIMIT_HOURS),
    params={"require_zero_paid": True, "direct_only": True},
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
    discover_catalog = BashOperator(
        task_id="discover_whoscored_catalog",
        bash_command=(
            "cd /opt/airflow && "
            "python dags/scripts/run_whoscored_scraper.py discover "
            f"--output {_RUN_DIR_TEMPLATE}/discovery.json --direct-only"
        ),
        env=_TASK_ENV,
        append_env=True,
        pool=DIRECT_POOL,
        priority_weight=100,
        retries=2,
        retry_delay=timedelta(minutes=5),
        do_xcom_push=False,
        # Initial deployment automatically performs the only full-history
        # catalog snapshot. Raw-backed retries resume it, but a cold source
        # crawl can legitimately exceed the incremental two-hour envelope.
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
        pool=DQ_POOL,
        execution_timeout=timedelta(minutes=10),
    )

    dynamic_mapping_available = callable(getattr(BashOperator, "partial", None))
    if dynamic_mapping_available:
        build_commands = PythonOperator(
            task_id="build_active_scope_commands",
            python_callable=build_daily_commands,
            op_kwargs={"scope_plan": freeze_scope_plan.output},
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
    else:
        # Lightweight host test stubs do not implement dynamic mapping.  This
        # task exercises the same runtime catalog path without resolving any
        # scope while the module is imported.
        ingest_scopes = BashOperator(
            task_id="ingest_active_scopes",
            bash_command=(
                "cd /opt/airflow && "
                "python dags/scripts/run_whoscored_scraper.py daily --skip-profiles "
                f"--output {_RUN_DIR_TEMPLATE}/active_scopes.json --direct-only"
            ),
            env=_TASK_ENV,
            append_env=True,
            pool=DIRECT_POOL,
            priority_weight=100,
            retries=0,
            execution_timeout=timedelta(hours=2),
            do_xcom_push=False,
        )
        scope_dq = PythonOperator(
            task_id="validate_active_scopes",
            python_callable=aggregate_traffic_reports,
            trigger_rule="all_done",
            pool=DQ_POOL,
        )
        catalog_dq >> ingest_scopes >> scope_dq

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

    runtime_preflight >> initialize_schema >> discover_catalog >> catalog_dq
    catalog_dq >> freeze_scope_plan
    ingest_scopes >> profile_capacity >> profile_command >> profile_task >> profile_dq
    [catalog_dq, scope_dq, profile_dq, traffic_dq] >> daily_slo
    [catalog_dq, scope_dq, profile_dq, traffic_dq, daily_slo] >> final_gate
    # Traffic upload/cleanup runs only after result-consuming DQ tasks, so
    # deleting the large local JSON reports cannot race their readers.
    [catalog_dq, scope_dq, profile_dq] >> traffic_dq
