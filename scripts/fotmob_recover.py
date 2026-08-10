#!/usr/bin/env python3
"""Recover one exact automatic FotMob publication without guessing.

The automatic owner deliberately retains the singleton publication lock when
an Airflow trigger fails ambiguously.  This coordinator is the only production
path that may release such a lock.  It first re-attests both live runtimes,
pauses the exact six isolated DAGs, proves their metadata is idle, and checks
the deterministic owner -> ingest -> Silver lineage.

Two recoveries are intentionally narrow:

* a terminal isolated producer in ``writing``/retained ``failed`` is marked
  failed with ``safe_to_release=True`` while its scheduler cursor remains
  unchanged.  A terminal generation is never reopened: daily becomes eligible
  at the next calendar 14:00 UTC boundary, while refresh/backfill keeps the
  same lane for the next eligible five-minute owner interval.  Both use fresh
  generation and child-run identities;
* an exact terminal SofaScore wait against ``ready`` and unclaimed evidence is
  abandoned.

``consuming`` is never released here.  Partial shared writes need a separate,
reviewed data repair and the command emits a durable blocked report instead.
All successful recoveries leave the six isolated DAGs paused; production is
resumed only through a new automatic rollout ceremony.
"""

from __future__ import annotations

import argparse
import json
import os
import re
import stat
import subprocess
import sys
import tempfile
import uuid
from datetime import datetime, time, timedelta, timezone
from pathlib import Path
from typing import Any, Callable, Mapping, Sequence

sys.dont_write_bytecode = True

try:  # package import in tests / ``python -m``
    from scripts import fotmob_runtime as runtime_binding
except ModuleNotFoundError:  # direct ``python scripts/fotmob_recover.py``
    import fotmob_runtime as runtime_binding


SCHEMA_VERSION = "fotmob-automatic-recovery-v1"
ISOLATED_SNAPSHOT_SCHEMA = "fotmob-automatic-recovery-isolated-v1"
SHARED_SNAPSHOT_SCHEMA = "fotmob-automatic-recovery-shared-v1"
CURSOR_TRANSITION_SCHEMA = "fotmob-automatic-recovery-cursor-v1"
SHARED_PAUSE_SCHEMA = "fotmob-automatic-recovery-shared-pause-v1"
CONFIRM_RECOVERY = "RECOVER_FOTMOB_AUTOMATIC_PUBLICATION"

OWNER_DAG_ID = "dag_orchestrate_fotmob"
INGEST_DAG_ID = "dag_ingest_fotmob"
SILVER_DAG_ID = "dag_transform_fotmob_silver"
SHARED_CONSUMER_DAG_ID = "dag_sofascore_pipeline"
PUBLICATION_CONF_KEY = "fotmob_publication"
SCHEDULER_STATE_VARIABLE = "fotmob.scheduler.state.v1"

ISOLATED_DAGS = (
    OWNER_DAG_ID,
    INGEST_DAG_ID,
    SILVER_DAG_ID,
    "dag_trigger_fotmob_daily",
    "dag_refresh_fotmob",
    "dag_backfill_fotmob",
)
LEGACY_DAGS = frozenset(ISOLATED_DAGS[3:])
ACTIVE_PAUSE_STATES = {dag_id: dag_id in LEGACY_DAGS for dag_id in ISOLATED_DAGS}
ALL_PAUSED_STATES = {dag_id: True for dag_id in ISOLATED_DAGS}
SHARED_DOWNSTREAM_DAGS = (
    "dag_transform_xref",
    "dag_transform_e3",
    "dag_transform_e4",
)
EXPECTED_TERMINAL_CONSUMER_TASKS = {
    "wait_for_fotmob_publication": "failed",
    "trigger_xref_transforms": "upstream_failed",
    "trigger_e3_transforms": "upstream_failed",
    "trigger_e4_transforms": "upstream_failed",
    "finalize_fotmob_publication": "failed",
}
TERMINAL_RUN_STATES = frozenset({"success", "failed"})
ACTIVE_TASK_INSTANCE_STATES = (
    "queued",
    "running",
    "scheduled",
    "deferred",
    "up_for_retry",
    "up_for_reschedule",
    "restarting",
)
_FULL_SHA_RE = re.compile(r"[0-9a-f]{40}")
_UUID_RE = re.compile(r"[0-9a-f]{8}-(?:[0-9a-f]{4}-){3}[0-9a-f]{12}")


class RecoveryError(RuntimeError):
    pass


def _now() -> str:
    return datetime.now(timezone.utc).isoformat().replace("+00:00", "Z")


def _instant(value: Any, *, label: str) -> datetime:
    try:
        parsed = datetime.fromisoformat(str(value).replace("Z", "+00:00"))
    except (TypeError, ValueError) as exc:
        raise RecoveryError(f"{label} is not an ISO-8601 instant") from exc
    if parsed.tzinfo is None or parsed.utcoffset() is None:
        raise RecoveryError(f"{label} has no timezone")
    return parsed.astimezone(timezone.utc)


def _canonical_instant(value: Any, *, label: str) -> str:
    return _instant(value, label=label).isoformat(timespec="microseconds")


def _canonical_binding(value: Any) -> dict[str, str]:
    if not isinstance(value, Mapping):
        raise RecoveryError("publication binding is missing")
    fingerprint = str(value.get("runtime_fingerprint") or "").casefold()
    if _FULL_SHA_RE.fullmatch(fingerprint) is None:
        raise RecoveryError("publication runtime fingerprint is invalid")
    binding = {
        "schema": "fotmob-publication-v1",
        "source": "fotmob",
        "owner": "isolated",
        "data_interval_start": _canonical_instant(
            value.get("data_interval_start"), label="publication interval start"
        ),
        "data_interval_end": _canonical_instant(
            value.get("data_interval_end"), label="publication interval end"
        ),
        "runtime_fingerprint": fingerprint,
    }
    if dict(value) != binding:
        raise RecoveryError("publication binding is not canonical isolated evidence")
    if _instant(
        binding["data_interval_end"], label="publication interval end"
    ) <= _instant(binding["data_interval_start"], label="publication interval start"):
        raise RecoveryError("publication interval is empty")
    return binding


def make_generation_id(binding: Mapping[str, Any]) -> str:
    """Mirror the runtime's retry-safe UUIDv5 generation contract."""

    normalized = _canonical_binding(binding)
    payload = json.dumps(normalized, sort_keys=True, separators=(",", ":"))
    return str(uuid.uuid5(uuid.NAMESPACE_URL, f"fotmob-publication:{payload}"))


def _scheduled_run_id(logical_date: Any) -> str:
    instant = _instant(logical_date, label="consumer logical date")
    return f"scheduled__{instant.isoformat()}"


def _normalize_state(value: Any) -> str:
    return str(value or "").casefold().split(".")[-1]


def _publication(generation_id: str, binding: Mapping[str, Any]) -> dict[str, Any]:
    return {"generation_id": generation_id, "binding": dict(binding)}


def _next_eligible_boundary(
    decision: Mapping[str, Any],
    binding: Mapping[str, Any],
    *,
    generation_id: str,
    observed_at: Any,
) -> dict[str, Any]:
    """Describe the first safe retry identity without reopening a terminal run."""

    normalized = _canonical_binding(binding)
    if make_generation_id(normalized) != generation_id:
        raise RecoveryError("terminal generation differs from its exact binding")
    # This validates the complete persisted scheduler decision without changing
    # it.  The returned advanced state is deliberately ignored for a failed
    # producer: only a successful publication may move that cursor.
    expected_advanced_state(
        decision,
        recovered_at=_canonical_instant(
            observed_at, label="retry boundary observation"
        ),
    )
    lane = str(decision.get("lane") or "").casefold()
    common = {
        "policy": "next_eligible_boundary",
        "lane": lane,
    }
    if lane in {"refresh", "backfill"}:
        start = _instant(
            normalized["data_interval_start"],
            label="terminal background interval start",
        )
        end = _instant(
            normalized["data_interval_end"],
            label="terminal background interval end",
        )
        if end - start != timedelta(minutes=5):
            raise RecoveryError("terminal background binding is not one owner interval")
        return {
            **common,
            "timing": "next_eligible_owner_5_minute_interval",
            "identity_source": "next_scheduled_owner_data_interval",
            "requires_new_generation_id": True,
            "requires_new_child_run_ids": True,
        }

    selected_date = str(decision.get("selected_date") or "")
    try:
        failed_date = datetime.fromisoformat(selected_date).date()
    except ValueError as exc:  # already guarded above; keep this helper total
        raise RecoveryError("automatic owner selected date is invalid") from exc
    old_end = _instant(
        normalized["data_interval_end"], label="terminal daily interval end"
    )
    old_start = _instant(
        normalized["data_interval_start"], label="terminal daily interval start"
    )
    if (
        old_end - old_start != timedelta(days=1)
        or old_end.date() != failed_date
        or old_end.time() != time(14, 0)
    ):
        raise RecoveryError("terminal daily binding differs from selected boundary")

    observed = _instant(observed_at, label="retry boundary observation")
    next_date = failed_date + timedelta(days=1)
    boundary = datetime.combine(next_date, time(14, 0), tzinfo=timezone.utc)
    while boundary <= observed:
        boundary += timedelta(days=1)
    next_binding = {
        **normalized,
        "data_interval_start": (boundary - timedelta(days=1)).isoformat(
            timespec="microseconds"
        ),
        "data_interval_end": boundary.isoformat(timespec="microseconds"),
    }
    next_generation_id = make_generation_id(next_binding)
    if next_generation_id == generation_id:  # pragma: no cover - UUID input differs
        raise RecoveryError("next daily boundary reused terminal generation")
    return {
        **common,
        "timing": "next_calendar_daily_1400_utc",
        "same_day_retry_allowed": False,
        "earliest_at": boundary.isoformat(timespec="microseconds"),
        "next_data_interval_start": next_binding["data_interval_start"],
        "next_data_interval_end": next_binding["data_interval_end"],
        "identity_source": "next_daily_binding_and_rollout_runtime_fingerprint",
        "requires_new_generation_id": True,
        "requires_new_child_run_ids": True,
    }


def expected_advanced_state(
    decision: Mapping[str, Any], *, recovered_at: str
) -> dict[str, Any]:
    selected = decision.get("state")
    if not isinstance(selected, Mapping):
        raise RecoveryError("automatic owner selected scheduler state is missing")
    expected_keys = {
        "next_background_lane",
        "daily_date",
        "generation",
        "updated_at",
    }
    if set(selected) != expected_keys:
        raise RecoveryError("automatic owner selected scheduler state is malformed")
    generation = selected.get("generation")
    if (
        isinstance(generation, bool)
        or not isinstance(generation, int)
        or generation < 0
    ):
        raise RecoveryError("automatic owner scheduler generation is invalid")
    if decision.get("state_generation") != generation:
        raise RecoveryError("automatic owner scheduler generation differs")
    lane = str(decision.get("lane") or "").casefold()
    if lane not in {"daily", "refresh", "backfill"}:
        raise RecoveryError("automatic owner lane is invalid")
    next_lane = str(selected.get("next_background_lane") or "").casefold()
    if next_lane not in {"refresh", "backfill"}:
        raise RecoveryError("automatic owner background cursor is invalid")
    daily_date = selected.get("daily_date")
    if daily_date is not None:
        try:
            if datetime.fromisoformat(str(daily_date)).date().isoformat() != daily_date:
                raise ValueError("non-canonical date")
        except (TypeError, ValueError) as exc:
            raise RecoveryError("automatic owner daily cursor is invalid") from exc
    _instant(selected.get("updated_at"), label="automatic owner cursor timestamp")
    selected_date = str(decision.get("selected_date") or "")
    try:
        if datetime.fromisoformat(selected_date).date().isoformat() != selected_date:
            raise ValueError("non-canonical date")
    except ValueError as exc:
        raise RecoveryError("automatic owner selected date is invalid") from exc
    if lane == "daily":
        daily_date = selected_date
    elif lane == "refresh":
        if next_lane != "refresh":
            raise RecoveryError("automatic refresh differs from scheduler cursor")
        next_lane = "backfill"
    else:
        if next_lane != "backfill":
            raise RecoveryError("automatic backfill differs from scheduler cursor")
        next_lane = "refresh"
    return {
        "next_background_lane": next_lane,
        "daily_date": daily_date,
        "generation": generation + 1,
        "updated_at": _canonical_instant(
            recovered_at, label="automatic recovery timestamp"
        ),
    }


def _state_matches_advanced(current: Any, expected: Mapping[str, Any]) -> bool:
    if not isinstance(current, Mapping) or set(current) != set(expected):
        return False
    if any(
        current.get(key) != expected.get(key)
        for key in ("next_background_lane", "daily_date", "generation")
    ):
        return False
    try:
        _instant(current.get("updated_at"), label="current scheduler timestamp")
    except RecoveryError:
        return False
    return True


def _atomic_json(path: Path, payload: Mapping[str, Any]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    temporary: Path | None = None
    try:
        with tempfile.NamedTemporaryFile(
            mode="w", encoding="utf-8", dir=path.parent, delete=False
        ) as stream:
            os.chmod(stream.name, 0o600)
            json.dump(payload, stream, ensure_ascii=False, indent=2, default=str)
            stream.write("\n")
            stream.flush()
            os.fsync(stream.fileno())
            temporary = Path(stream.name)
        temporary.replace(path)
        directory_fd = os.open(path.parent, os.O_RDONLY | getattr(os, "O_DIRECTORY", 0))
        try:
            os.fsync(directory_fd)
        finally:
            os.close(directory_fd)
    finally:
        if temporary is not None and temporary.exists():
            temporary.unlink()


def _validate_output_path(context: Mapping[str, Any], path: Path) -> Path:
    evidence = Path(str(context.get("evidence_dir") or ""))
    if not path.is_absolute():
        raise RecoveryError("recovery output must be an absolute protected path")
    try:
        evidence = evidence.resolve(strict=True)
        resolved = path.resolve(strict=False)
        resolved.relative_to(evidence)
    except (OSError, ValueError) as exc:
        raise RecoveryError(
            "recovery output must stay inside the deployment evidence directory"
        ) from exc
    if resolved == Path(str(context.get("deployment_report") or "")).resolve():
        raise RecoveryError("recovery output cannot replace deployment admission")
    try:
        mode = path.lstat().st_mode
    except FileNotFoundError:
        pass
    else:
        if stat.S_ISLNK(mode) or not stat.S_ISREG(mode):
            raise RecoveryError("recovery output must be a regular non-symlink file")
    return path


def _deployment_context(args: argparse.Namespace) -> dict[str, Any]:
    try:
        context = runtime_binding.load_deployment_context(
            args.deployment_report,
            project=args.project,
            compose_file=args.compose_file,
        )
    except runtime_binding.RuntimeBindingError as exc:
        raise RecoveryError(str(exc)) from exc
    if (
        context.get("activation_state") != "active"
        or not isinstance(context.get("automatic_rollout_summary"), Mapping)
        or context["automatic_rollout_summary"].get("phase") != "active"
        or context["automatic_rollout_summary"].get("passed") is not True
    ):
        raise RecoveryError("recovery requires the exact active automatic deployment")
    context["deployment_report"] = str(args.deployment_report.resolve())
    return context


def _compose_base(args: argparse.Namespace) -> tuple[str, ...]:
    try:
        return runtime_binding.compose_base(
            project=args.project,
            compose_file=args.compose_file,
            env_file=args.env_file,
        )
    except runtime_binding.RuntimeBindingError as exc:
        raise RecoveryError(str(exc)) from exc


def _compose_environment(context: Mapping[str, Any]) -> dict[str, str]:
    return runtime_binding.compose_environment(context)


def validate_live_runtimes(
    args: argparse.Namespace,
    context: Mapping[str, Any],
    *,
    run: Callable[..., subprocess.CompletedProcess[str]] = subprocess.run,
) -> dict[str, Any]:
    try:
        isolated = runtime_binding.validate_live_deployment(
            context,
            project=args.project,
            compose_file=args.compose_file,
            env_file=args.env_file,
            require_running=True,
            run=run,
        )
        shared = runtime_binding.validate_live_shared_runtime(context, run=run)
    except runtime_binding.RuntimeBindingError as exc:
        raise RecoveryError(str(exc)) from exc
    return {"isolated": isolated, "shared": shared}


def _revalidate_mutation_boundary(
    args: argparse.Namespace,
    expected: Mapping[str, Any],
    *,
    run: Callable[..., subprocess.CompletedProcess[str]],
) -> tuple[dict[str, Any], dict[str, Any]]:
    current = _deployment_context(args)
    identity_fields = (
        "deployment_id",
        "git_sha",
        "scheduler_container_id",
        "metadb_container_id",
        "resolved_image_id",
        "resolved_postgres_image_id",
        "evidence_dir",
        "automatic_rollout_summary",
    )
    expected_handoff = expected.get("shared_handoff_final")
    current_handoff = current.get("shared_handoff_final")
    expected_shared = (
        expected_handoff.get("shared_scheduler_container")
        if isinstance(expected_handoff, Mapping)
        else None
    )
    current_shared = (
        current_handoff.get("shared_scheduler_container")
        if isinstance(current_handoff, Mapping)
        else None
    )
    if (
        any(current.get(field) != expected.get(field) for field in identity_fields)
        or current_shared != expected_shared
    ):
        raise RecoveryError("active automatic deployment changed during recovery")
    return current, validate_live_runtimes(args, current, run=run)


def _parse_marker(output: str, marker: str) -> dict[str, Any]:
    for line in reversed(output.splitlines()):
        if line.startswith(marker):
            try:
                value = json.loads(line.removeprefix(marker))
            except json.JSONDecodeError as exc:
                raise RecoveryError("recovery runtime returned invalid JSON") from exc
            if isinstance(value, dict):
                return value
    raise RecoveryError("recovery runtime returned no protected evidence")


def _isolated_python_json(
    args: argparse.Namespace,
    context: Mapping[str, Any],
    *,
    code: str,
    marker: str,
    run: Callable[..., subprocess.CompletedProcess[str]],
) -> dict[str, Any]:
    del args
    container_id = str(context.get("scheduler_container_id") or "")
    if re.fullmatch(r"[0-9a-f]{64}", container_id) is None:
        raise RecoveryError("recovery has no exact isolated scheduler identity")
    result = run(
        (
            "docker",
            "exec",
            container_id,
            "python",
            "-c",
            code,
        ),
        check=True,
        capture_output=True,
        text=True,
    )
    return _parse_marker(result.stdout, marker)


def _shared_python_json(
    context: Mapping[str, Any],
    *,
    code: str,
    marker: str,
    run: Callable[..., subprocess.CompletedProcess[str]],
) -> dict[str, Any]:
    handoff = context.get("shared_handoff_final")
    container_id = (
        str(handoff.get("shared_scheduler_container") or "")
        if isinstance(handoff, Mapping)
        else ""
    )
    result = run(
        ("docker", "exec", container_id, "python", "-c", code),
        check=True,
        capture_output=True,
        text=True,
    )
    return _parse_marker(result.stdout, marker)


def _get_control_state(
    args: argparse.Namespace,
    context: Mapping[str, Any],
    generation_id: str,
    *,
    run: Callable[..., subprocess.CompletedProcess[str]] = subprocess.run,
) -> dict[str, Any]:
    marker = "FOTMOB_AUTOMATIC_RECOVERY_CONTROL_JSON="
    code = (
        "import json\n"
        "from scrapers.fbref.control import ControlStore\n"
        f"value=ControlStore.from_env().get_publication_generation({generation_id!r},source='fotmob')\n"
        f"print({marker!r}+json.dumps(value,default=str,sort_keys=True))\n"
    )
    value = _isolated_python_json(args, context, code=code, marker=marker, run=run)
    return value


def _isolated_snapshot_code(generation_id: str, *, pause: bool) -> tuple[str, str]:
    marker = "FOTMOB_AUTOMATIC_RECOVERY_ISOLATED_JSON="
    code = f"""
import json
from collections.abc import Mapping
from datetime import datetime, timezone
from airflow.models import DagModel, DagRun, TaskInstance, Variable, XCom
from airflow.settings import Session
from sqlalchemy import text

DAGS={list(ISOLATED_DAGS)!r}
ACTIVE_TI_STATES={ACTIVE_TASK_INSTANCE_STATES!r}
OWNER={OWNER_DAG_ID!r}
INGEST={INGEST_DAG_ID!r}
SILVER={SILVER_DAG_ID!r}
GENERATION={generation_id!r}
PAUSE={pause!r}

def state(value):
    return str(getattr(value, 'value', value) or '').lower().split('.')[-1]

def instant(value):
    return None if value is None else value.isoformat(timespec='microseconds')

def task_states(session, dag_id, run_id):
    rows=session.query(TaskInstance.task_id,TaskInstance.state).filter(
        TaskInstance.dag_id==dag_id,TaskInstance.run_id==run_id).all()
    return {{str(task_id):state(task_state) or None for task_id,task_state in rows}}

def dag_run(session, dag_id, run_id):
    row=session.query(DagRun).filter(
        DagRun.dag_id==dag_id,DagRun.run_id==run_id).one_or_none()
    if row is None:
        return None
    conf=row.conf if isinstance(row.conf,Mapping) else row.conf
    return {{
        'dag_id':str(row.dag_id),'run_id':str(row.run_id),
        'run_type':state(row.run_type),'state':state(row.state),
        'logical_date':instant(row.logical_date),
        'data_interval_start':instant(row.data_interval_start),
        'data_interval_end':instant(row.data_interval_end),
        'conf':dict(conf) if isinstance(conf,Mapping) else conf,
        'task_states':task_states(session,dag_id,run_id),
    }}

session=Session()
try:
    if PAUSE:
        session.execute(text('SET TRANSACTION ISOLATION LEVEL SERIALIZABLE'))
    models=session.query(DagModel).filter(DagModel.dag_id.in_(DAGS)).with_for_update().all()
    if {{str(row.dag_id) for row in models}} != set(DAGS):
        raise RuntimeError('exact six isolated DagModels are not present')
    pause_before={{str(row.dag_id):bool(row.is_paused) for row in models}}
    if PAUSE:
        for row in models:
            row.is_paused=True
        session.flush()
    locked_active_rows=session.query(DagRun.dag_id,DagRun.run_id,DagRun.state).filter(
        DagRun.dag_id.in_(DAGS),DagRun.state.in_(('queued','running'))).with_for_update().all()
    locked_active=[{{'dag_id':str(d),'run_id':str(r),'state':state(s)}} for d,r,s in locked_active_rows]
    locked_task_rows=session.query(
        TaskInstance.dag_id,TaskInstance.run_id,TaskInstance.task_id,TaskInstance.state
    ).filter(
        TaskInstance.dag_id.in_(DAGS),TaskInstance.state.in_(ACTIVE_TI_STATES)
    ).with_for_update().all()
    locked_tasks=[{{'dag_id':str(d),'run_id':str(r),'task_id':str(t),'state':state(s)}} for d,r,t,s in locked_task_rows]
    session.commit()
finally:
    session.close()

session=Session()
try:
    models=session.query(DagModel).filter(DagModel.dag_id.in_(DAGS)).all()
    pause_after={{str(row.dag_id):bool(row.is_paused) for row in models}}
    active_rows=session.query(DagRun.dag_id,DagRun.run_id,DagRun.state).filter(
        DagRun.dag_id.in_(DAGS),DagRun.state.in_(('queued','running'))).all()
    current_active=[{{'dag_id':str(d),'run_id':str(r),'state':state(s)}} for d,r,s in active_rows]
    active=[]
    for item in locked_active+current_active:
        if item not in active:
            active.append(item)
    current_task_rows=session.query(
        TaskInstance.dag_id,TaskInstance.run_id,TaskInstance.task_id,TaskInstance.state
    ).filter(
        TaskInstance.dag_id.in_(DAGS),TaskInstance.state.in_(ACTIVE_TI_STATES)
    ).all()
    current_tasks=[{{'dag_id':str(d),'run_id':str(r),'task_id':str(t),'state':state(s)}} for d,r,t,s in current_task_rows]
    active_tasks=[]
    for item in locked_tasks+current_tasks:
        if item not in active_tasks:
            active_tasks.append(item)

    initializers=session.query(XCom).filter(
        XCom.dag_id==OWNER,
        XCom.task_id=='initialize_fotmob_publication',
        XCom.key=='return_value').order_by(XCom.timestamp.desc()).limit(100).all()
    owner_matches=[]
    for row in initializers:
        value=XCom.deserialize_value(row)
        if not isinstance(value,Mapping) or value.get('generation_id') != GENERATION:
            continue
        owner=dag_run(session,OWNER,str(row.run_id))
        decision_row=session.query(XCom).filter(
            XCom.dag_id==OWNER,XCom.run_id==str(row.run_id),
            XCom.task_id=='choose_fotmob_lane',XCom.key=='return_value').order_by(
                XCom.timestamp.desc()).first()
        decision=XCom.deserialize_value(decision_row) if decision_row is not None else None
        payload=owner or {{'dag_id':OWNER,'run_id':str(row.run_id)}}
        payload['initializer']=dict(value)
        payload['decision']=dict(decision) if isinstance(decision,Mapping) else decision
        owner_matches.append(payload)

    ingest=dag_run(session,INGEST,'fotmob_orchestrated__'+GENERATION)
    silver=dag_run(session,SILVER,'fotmob_silver__'+GENERATION)
    default=json.dumps({{'next_background_lane':'refresh','daily_date':None,'generation':0,'updated_at':'1970-01-01T00:00:00+00:00'}},sort_keys=True,separators=(',',':'))
    scheduler=Variable.get({SCHEDULER_STATE_VARIABLE!r},default_var=default,deserialize_json=True)
    if isinstance(scheduler,str):
        scheduler=json.loads(scheduler)
finally:
    session.close()

result={{
    'schema_version':{ISOLATED_SNAPSHOT_SCHEMA!r},
    'observed_at':datetime.now(timezone.utc).isoformat(),
    'pause_states_before':pause_before,
    'pause_states_after':pause_after,
    'active_runs':active,
    'active_task_instances':active_tasks,
    'owner_matches':owner_matches,
    'ingest':ingest,
    'silver':silver,
    'scheduler_state':scheduler,
    'atomic_metadata_transaction':True,
}}
print({marker!r}+json.dumps(result,default=str,sort_keys=True))
"""
    return code, marker


def _observe_isolated(
    args: argparse.Namespace,
    context: Mapping[str, Any],
    generation_id: str,
    *,
    pause: bool,
    run: Callable[..., subprocess.CompletedProcess[str]] = subprocess.run,
) -> dict[str, Any]:
    code, marker = _isolated_snapshot_code(generation_id, pause=pause)
    return _isolated_python_json(args, context, code=code, marker=marker, run=run)


def _observe_shared(
    context: Mapping[str, Any],
    generation_id: str,
    binding: Mapping[str, Any],
    *,
    run: Callable[..., subprocess.CompletedProcess[str]] = subprocess.run,
) -> dict[str, Any]:
    marker = "FOTMOB_AUTOMATIC_RECOVERY_SHARED_JSON="
    publication = _publication(generation_id, binding)
    consumer_run_id = _scheduled_run_id(binding["data_interval_start"])
    code = f"""
import json
from collections.abc import Mapping
from datetime import datetime, timedelta, timezone
from airflow.models import DagRun, TaskInstance
from airflow.settings import Session

CONSUMER={SHARED_CONSUMER_DAG_ID!r}
RUN_ID={consumer_run_id!r}
DOWNSTREAM={list(SHARED_DOWNSTREAM_DAGS)!r}
ACTIVE_TI_STATES={ACTIVE_TASK_INSTANCE_STATES!r}
PUBLICATION=json.loads({json.dumps(publication, sort_keys=True)!r})
INTERVAL_START=datetime.fromisoformat({binding["data_interval_start"]!r})

def state(value):
    return str(getattr(value,'value',value) or '').lower().split('.')[-1]

def instant(value):
    return None if value is None else value.isoformat(timespec='microseconds')

session=Session()
try:
    row=session.query(DagRun).filter(
        DagRun.dag_id==CONSUMER,DagRun.run_id==RUN_ID).one_or_none()
    consumer=None
    if row is not None:
        tasks=session.query(TaskInstance.task_id,TaskInstance.state).filter(
            TaskInstance.dag_id==CONSUMER,TaskInstance.run_id==RUN_ID).all()
        consumer={{
            'dag_id':str(row.dag_id),'run_id':str(row.run_id),
            'run_type':state(row.run_type),'state':state(row.state),
            'logical_date':instant(row.logical_date),
            'data_interval_start':instant(row.data_interval_start),
            'data_interval_end':instant(row.data_interval_end),
            'task_states':{{str(task_id):state(task_state) or None for task_id,task_state in tasks}},
        }}
    active_target_tasks=[]
    if row is not None:
        active_target_tasks=[
            {{'dag_id':CONSUMER,'run_id':RUN_ID,'task_id':str(task_id),'state':state(task_state)}}
            for task_id,task_state in tasks if state(task_state) in ACTIVE_TI_STATES
        ]
    rows=session.query(DagRun).filter(
        DagRun.dag_id.in_(DOWNSTREAM),
        DagRun.execution_date >= INTERVAL_START-timedelta(days=1)).all()
    active=[]
    active_tasks=list(active_target_tasks)
    for child in rows:
        conf=child.conf if isinstance(child.conf,Mapping) else {{}}
        if conf.get({PUBLICATION_CONF_KEY!r}) == PUBLICATION:
            if state(child.state) in ('queued','running'):
                active.append({{'dag_id':str(child.dag_id),'run_id':str(child.run_id),'state':state(child.state)}})
            child_tasks=session.query(
                TaskInstance.task_id,TaskInstance.state
            ).filter(
                TaskInstance.dag_id==child.dag_id,
                TaskInstance.run_id==child.run_id,
                TaskInstance.state.in_(ACTIVE_TI_STATES)).all()
            active_tasks.extend([
                {{'dag_id':str(child.dag_id),'run_id':str(child.run_id),'task_id':str(task_id),'state':state(task_state)}}
                for task_id,task_state in child_tasks
            ])
finally:
    session.close()
result={{
    'schema_version':{SHARED_SNAPSHOT_SCHEMA!r},
    'observed_at':datetime.now(timezone.utc).isoformat(),
    'consumer_run':consumer,
    'active_bound_downstream_runs':active,
    'active_bound_task_instances':active_tasks,
}}
print({marker!r}+json.dumps(result,default=str,sort_keys=True))
"""
    return _shared_python_json(context, code=code, marker=marker, run=run)


def _pause_shared_for_rollout(
    context: Mapping[str, Any],
    *,
    run: Callable[..., subprocess.CompletedProcess[str]] = subprocess.run,
) -> dict[str, Any]:
    """Pause future Sofa schedules and report whether shared state is idle."""

    marker = "FOTMOB_AUTOMATIC_RECOVERY_SHARED_PAUSE_JSON="
    shared_state_dags = tuple(sorted(runtime_binding.SHARED_STATE_DAGS))
    code = f"""
import json
from datetime import datetime, timezone
from airflow.models import DagModel, DagRun, TaskInstance, Variable
from airflow.settings import Session
from sqlalchemy import text

CONSUMER={SHARED_CONSUMER_DAG_ID!r}
STATE_DAGS={shared_state_dags!r}
ACTIVE_TI_STATES={ACTIVE_TASK_INSTANCE_STATES!r}
session=Session()
try:
    session.execute(text('SET TRANSACTION ISOLATION LEVEL SERIALIZABLE'))
    model=session.query(DagModel).filter(
        DagModel.dag_id==CONSUMER).with_for_update().one_or_none()
    owner=session.query(Variable).filter(
        Variable.key=='fotmob_schedule_owner').with_for_update().one_or_none()
    if model is None or owner is None or str(owner.val).strip().lower() != 'isolated':
        raise RuntimeError('shared automatic schedule ownership differs')
    before=bool(model.is_paused)
    model.is_paused=True
    session.flush()
    rows=session.query(DagRun.dag_id,DagRun.run_id,DagRun.state).filter(
        DagRun.dag_id.in_(STATE_DAGS),
        DagRun.state.in_(('queued','running'))).with_for_update().all()
    active=[{{'dag_id':str(d),'run_id':str(r),'state':str(getattr(s,'value',s)).lower()}} for d,r,s in rows]
    task_rows=session.query(
        TaskInstance.dag_id,TaskInstance.run_id,TaskInstance.task_id,TaskInstance.state
    ).filter(
        TaskInstance.dag_id.in_(STATE_DAGS),
        TaskInstance.state.in_(ACTIVE_TI_STATES)).with_for_update().all()
    active_tasks=[{{
        'dag_id':str(d),'run_id':str(r),'task_id':str(t),
        'state':str(getattr(s,'value',s)).lower()
    }} for d,r,t,s in task_rows]
    session.commit()
finally:
    session.close()
result={{
    'schema_version':{SHARED_PAUSE_SCHEMA!r},
    'observed_at':datetime.now(timezone.utc).isoformat(),
    'schedule_owner':'isolated',
    'consumer_pause_before':before,
    'consumer_pause_after':True,
    'active_runs':active,
    'active_task_instances':active_tasks,
    'rollout_ready':not active and not active_tasks,
    'atomic_metadata_transaction':True,
}}
print({marker!r}+json.dumps(result,sort_keys=True))
"""
    payload = _shared_python_json(context, code=code, marker=marker, run=run)
    if (
        payload.get("schema_version") != SHARED_PAUSE_SCHEMA
        or payload.get("schedule_owner") != "isolated"
        or payload.get("consumer_pause_after") is not True
        or not isinstance(payload.get("active_runs"), list)
        or not isinstance(payload.get("active_task_instances"), list)
        or payload.get("rollout_ready")
        != (payload["active_runs"] == [] and payload["active_task_instances"] == [])
        or payload.get("atomic_metadata_transaction") is not True
    ):
        raise RecoveryError("shared consumer pause/readback is incomplete")
    _instant(payload.get("observed_at"), label="shared pause observation")
    return payload


def _advance_scheduler_cursor(
    args: argparse.Namespace,
    context: Mapping[str, Any],
    decision: Mapping[str, Any],
    *,
    recovered_at: str,
    run: Callable[..., subprocess.CompletedProcess[str]] = subprocess.run,
) -> dict[str, Any]:
    source = dict(decision["state"])
    target = expected_advanced_state(decision, recovered_at=recovered_at)
    marker = "FOTMOB_AUTOMATIC_RECOVERY_CURSOR_JSON="
    code = f"""
import json
from datetime import datetime
from airflow.models import Variable
from airflow.settings import Session

KEY={SCHEDULER_STATE_VARIABLE!r}
SOURCE=json.loads({json.dumps(source, sort_keys=True)!r})
TARGET=json.loads({json.dumps(target, sort_keys=True)!r})
DEFAULT={{'next_background_lane':'refresh','daily_date':None,'generation':0,'updated_at':'1970-01-01T00:00:00+00:00'}}

def parsed(value):
    if isinstance(value,str):
        return json.loads(value)
    return value

def already(value):
    if not isinstance(value,dict) or set(value) != set(TARGET):
        return False
    if any(value.get(k) != TARGET.get(k) for k in ('next_background_lane','daily_date','generation')):
        return False
    timestamp=datetime.fromisoformat(str(value.get('updated_at')).replace('Z','+00:00'))
    return timestamp.tzinfo is not None and timestamp.utcoffset() is not None

session=Session()
try:
    row=session.query(Variable).filter(Variable.key==KEY).with_for_update().one_or_none()
    before=parsed(row.val) if row is not None else dict(DEFAULT)
    if before == SOURCE:
        if row is None:
            row=Variable(key=KEY,val=json.dumps(TARGET,sort_keys=True,separators=(',',':')))
            session.add(row)
        else:
            row.val=json.dumps(TARGET,sort_keys=True,separators=(',',':'))
        after=dict(TARGET)
        idempotent=False
    elif already(before):
        after=dict(before)
        idempotent=True
    else:
        raise RuntimeError('automatic scheduler cursor changed outside exact lineage')
    session.commit()
finally:
    session.close()
result={{'schema_version':{CURSOR_TRANSITION_SCHEMA!r},'before':before,'after':after,'idempotent':idempotent}}
print({marker!r}+json.dumps(result,sort_keys=True))
"""
    return _isolated_python_json(args, context, code=code, marker=marker, run=run)


def _control_transition(
    args: argparse.Namespace,
    context: Mapping[str, Any],
    *,
    expression: str,
    marker: str,
    run: Callable[..., subprocess.CompletedProcess[str]],
) -> dict[str, Any]:
    code = (
        "import json\n"
        "from scrapers.fbref.control import ControlStore\n"
        "store=ControlStore.from_env()\n"
        f"value={expression}\n"
        f"print({marker!r}+json.dumps(value,default=str,sort_keys=True))\n"
    )
    return _isolated_python_json(args, context, code=code, marker=marker, run=run)


def _fail_and_release(
    args: argparse.Namespace,
    context: Mapping[str, Any],
    generation_id: str,
    *,
    run: Callable[..., subprocess.CompletedProcess[str]] = subprocess.run,
) -> dict[str, Any]:
    return _control_transition(
        args,
        context,
        expression=(
            "store.fail_publication_generation("
            f"{generation_id!r},safe_to_release=True,source='fotmob')"
        ),
        marker="FOTMOB_AUTOMATIC_RECOVERY_RELEASE_JSON=",
        run=run,
    )


def _abandon_unclaimed(
    args: argparse.Namespace,
    context: Mapping[str, Any],
    generation_id: str,
    *,
    run: Callable[..., subprocess.CompletedProcess[str]] = subprocess.run,
) -> dict[str, Any]:
    return _control_transition(
        args,
        context,
        expression=(
            "store.complete_publication_generation("
            f"{generation_id!r},consumer=None,published=False,source='fotmob')"
        ),
        marker="FOTMOB_AUTOMATIC_RECOVERY_ABANDON_JSON=",
        run=run,
    )


def _validate_control_identity(
    state: Any,
    *,
    generation_id: str,
    git_sha: str,
) -> tuple[dict[str, Any], dict[str, str]]:
    if not isinstance(state, Mapping):
        raise RecoveryError("exact ControlStore generation is absent")
    binding = _canonical_binding(state.get("binding"))
    if (
        state.get("generation_id") != generation_id
        or make_generation_id(binding) != generation_id
        or binding["runtime_fingerprint"] != git_sha
        or state.get("source") != "fotmob"
    ):
        raise RecoveryError("ControlStore generation identity differs from deployment")
    if state.get("consumer") is not None and state.get("phase") != "consuming":
        raise RecoveryError("ControlStore has an unexpected consumer identity")
    return dict(state), binding


def _validate_isolated_quiescence(snapshot: Any) -> dict[str, Any]:
    if not isinstance(snapshot, Mapping):
        raise RecoveryError("isolated recovery snapshot is missing")
    if snapshot.get("schema_version") != ISOLATED_SNAPSHOT_SCHEMA:
        raise RecoveryError("isolated recovery snapshot schema differs")
    if snapshot.get("atomic_metadata_transaction") is not True:
        raise RecoveryError("isolated recovery snapshot is not one metadata fence")
    _instant(snapshot.get("observed_at"), label="isolated recovery observation")
    before = snapshot.get("pause_states_before")
    after = snapshot.get("pause_states_after")
    if before not in (ACTIVE_PAUSE_STATES, ALL_PAUSED_STATES):
        raise RecoveryError("isolated pause state differs from automatic deployment")
    if after != ALL_PAUSED_STATES:
        raise RecoveryError("recovery did not pause the exact six isolated DAGs")
    active = snapshot.get("active_runs")
    if not isinstance(active, list):
        raise RecoveryError("isolated active-run evidence is malformed")
    if active:
        raise RecoveryError("active isolated runs keep the publication lock retained")
    active_tasks = snapshot.get("active_task_instances")
    if not isinstance(active_tasks, list):
        raise RecoveryError("isolated active-task evidence is malformed")
    if active_tasks:
        raise RecoveryError(
            "active isolated task instances keep the publication lock retained"
        )
    return dict(snapshot)


def _validate_owner_lineage(
    snapshot: Mapping[str, Any],
    *,
    generation_id: str,
    binding: Mapping[str, Any],
    producer_failed: bool,
) -> tuple[dict[str, Any], dict[str, Any]]:
    expected_publication = _publication(generation_id, binding)
    matches = snapshot.get("owner_matches")
    if not isinstance(matches, list) or len(matches) != 1:
        raise RecoveryError("exact automatic owner initializer is not unique")
    owner = matches[0]
    if not isinstance(owner, Mapping):
        raise RecoveryError("automatic owner lineage is malformed")
    initializer = owner.get("initializer")
    if (
        not isinstance(initializer, Mapping)
        or initializer.get("generation_id") != generation_id
        or initializer.get("binding") != dict(binding)
    ):
        raise RecoveryError("automatic owner initializer differs from generation")
    decision = owner.get("decision")
    tasks = owner.get("task_states")
    if (
        owner.get("dag_id", OWNER_DAG_ID) != OWNER_DAG_ID
        or owner.get("run_type") != "scheduled"
        or not isinstance(decision, Mapping)
        or not isinstance(tasks, Mapping)
        or not isinstance(decision.get("conf"), Mapping)
        or decision["conf"].get("mode") != decision.get("lane")
    ):
        raise RecoveryError("automatic owner scheduled lineage differs")
    expected_advanced_state(decision, recovered_at=snapshot["observed_at"])
    owner_state = _normalize_state(owner.get("state"))
    if producer_failed:
        if owner_state != "failed":
            raise RecoveryError("isolated producer recovery requires failed owner")
        if tasks.get("advance_fotmob_scheduler_state") == "success":
            raise RecoveryError(
                "failed owner already claims a successful cursor advance"
            )
    else:
        completed = (
            owner_state == "success"
            and tasks.get("advance_fotmob_scheduler_state") == "success"
        )
        cursor_failed = (
            owner_state == "failed"
            and tasks.get("trigger_fotmob_ingest") == "success"
            and tasks.get("finalize_fotmob_publication") == "success"
            and tasks.get("advance_fotmob_scheduler_state") == "failed"
        )
        finalizer_failed = (
            owner_state == "failed"
            and tasks.get("trigger_fotmob_ingest") == "success"
            and tasks.get("finalize_fotmob_publication") == "failed"
            and tasks.get("advance_fotmob_scheduler_state") in {None, "upstream_failed"}
        )
        if not (completed or cursor_failed or finalizer_failed):
            raise RecoveryError("ready generation has no successful owner completion")

    ingest = snapshot.get("ingest")
    trigger_state = _normalize_state(tasks.get("trigger_fotmob_ingest"))
    if ingest is None:
        if trigger_state == "success":
            raise RecoveryError("successful owner trigger has no deterministic ingest")
        if snapshot.get("silver") is not None:
            raise RecoveryError("Silver exists without its deterministic ingest")
        return dict(owner), dict(decision)
    if not isinstance(ingest, Mapping):
        raise RecoveryError("deterministic ingest lineage is malformed")
    expected_ingest = f"fotmob_orchestrated__{generation_id}"
    if (
        ingest.get("dag_id") != INGEST_DAG_ID
        or ingest.get("run_id") != expected_ingest
        or _normalize_state(ingest.get("state")) not in TERMINAL_RUN_STATES
        or not isinstance(ingest.get("conf"), Mapping)
        or ingest["conf"].get(PUBLICATION_CONF_KEY) != expected_publication
    ):
        raise RecoveryError("deterministic ingest lineage differs")
    if not producer_failed and _normalize_state(ingest.get("state")) != "success":
        raise RecoveryError("ready generation has no successful deterministic ingest")

    silver = snapshot.get("silver")
    if silver is not None:
        if (
            not isinstance(silver, Mapping)
            or silver.get("dag_id") != SILVER_DAG_ID
            or silver.get("run_id") != f"fotmob_silver__{generation_id}"
            or _normalize_state(silver.get("state")) not in TERMINAL_RUN_STATES
            or not isinstance(silver.get("conf"), Mapping)
            or silver["conf"].get(PUBLICATION_CONF_KEY) != expected_publication
        ):
            raise RecoveryError("deterministic Silver lineage differs")
        if not producer_failed and _normalize_state(silver.get("state")) != "success":
            raise RecoveryError(
                "ready generation has no successful deterministic Silver"
            )
    return dict(owner), dict(decision)


def _validate_producer_failure(
    snapshot: Any,
    control: Any,
    *,
    generation_id: str,
    git_sha: str,
) -> tuple[dict[str, Any], dict[str, Any], dict[str, str]]:
    isolated = _validate_isolated_quiescence(snapshot)
    state, binding = _validate_control_identity(
        control, generation_id=generation_id, git_sha=git_sha
    )
    active_retained = state.get("active") is True and state.get("lock_active") is True
    released_retry = (
        state.get("phase") == "failed"
        and state.get("active") is False
        and state.get("lock_active") is False
        and bool(state.get("released_at"))
    )
    if (
        (state.get("phase"), state.get("status"))
        not in {("writing", "running"), ("failed", "failed")}
        or state.get("consumer") is not None
        or state.get("owner_dag_id") != OWNER_DAG_ID
        or not (active_retained or released_retry)
    ):
        raise RecoveryError("producer generation is not one retained writing failure")
    owner, decision = _validate_owner_lineage(
        isolated,
        generation_id=generation_id,
        binding=binding,
        producer_failed=True,
    )
    if isolated.get("scheduler_state") != decision.get("state"):
        raise RecoveryError("failed producer scheduler cursor did not stay unchanged")
    return owner, decision, binding


def _validate_shared_terminal(
    snapshot: Any,
    *,
    generation_id: str,
    binding: Mapping[str, Any],
) -> dict[str, Any]:
    if not isinstance(snapshot, Mapping):
        raise RecoveryError("shared terminal recovery snapshot is missing")
    if snapshot.get("schema_version") != SHARED_SNAPSHOT_SCHEMA:
        raise RecoveryError("shared terminal recovery snapshot schema differs")
    _instant(snapshot.get("observed_at"), label="shared recovery observation")
    consumer = snapshot.get("consumer_run")
    if not isinstance(consumer, Mapping):
        raise RecoveryError("exact scheduled Sofa consumer is absent")
    expected_run_id = _scheduled_run_id(binding["data_interval_start"])
    expected_interval = {
        "logical_date": binding["data_interval_start"],
        "data_interval_start": binding["data_interval_start"],
        "data_interval_end": binding["data_interval_end"],
    }
    tasks = consumer.get("task_states")
    if (
        consumer.get("dag_id") != SHARED_CONSUMER_DAG_ID
        or consumer.get("run_id") != expected_run_id
        or consumer.get("run_type") != "scheduled"
        or consumer.get("state") != "failed"
        or any(consumer.get(key) != value for key, value in expected_interval.items())
        or not isinstance(tasks, Mapping)
        or any(
            tasks.get(task_id) != state
            for task_id, state in EXPECTED_TERMINAL_CONSUMER_TASKS.items()
        )
    ):
        raise RecoveryError("shared consumer has no exact terminal task proof")
    if snapshot.get("active_bound_downstream_runs") != []:
        raise RecoveryError("shared publication still has an active downstream run")
    if snapshot.get("active_bound_task_instances") != []:
        raise RecoveryError(
            "shared publication still has an active consumer/downstream task"
        )
    return dict(snapshot)


def _validate_ready_failure(
    isolated_snapshot: Any,
    shared_snapshot: Any,
    control: Any,
    *,
    generation_id: str,
    git_sha: str,
) -> tuple[
    dict[str, Any],
    dict[str, str],
    dict[str, Any],
    dict[str, Any],
    bool,
]:
    isolated = _validate_isolated_quiescence(isolated_snapshot)
    state, binding = _validate_control_identity(
        control, generation_id=generation_id, git_sha=git_sha
    )
    if (
        (state.get("phase"), state.get("status"))
        not in {("ready", "succeeded"), ("abandoned", "succeeded")}
        or state.get("consumer") is not None
        or (
            state.get("phase") == "ready"
            and (
                state.get("owner_dag_id") != OWNER_DAG_ID
                or state.get("active") is not True
                or state.get("lock_active") is not True
            )
        )
        or (
            state.get("phase") == "abandoned"
            and (
                state.get("active") is not False
                or state.get("lock_active") is not False
                or not state.get("released_at")
            )
        )
    ):
        raise RecoveryError("shared recovery requires exact ready unclaimed generation")
    owner, decision = _validate_owner_lineage(
        isolated,
        generation_id=generation_id,
        binding=binding,
        producer_failed=False,
    )
    cursor_required = _normalize_state(owner.get("state")) == "failed"
    if cursor_required:
        current = isolated.get("scheduler_state")
        target = expected_advanced_state(decision, recovered_at=isolated["observed_at"])
        if current != decision.get("state") and not _state_matches_advanced(
            current, target
        ):
            raise RecoveryError(
                "automatic scheduler cursor changed outside ready lineage"
            )
    shared = _validate_shared_terminal(
        shared_snapshot, generation_id=generation_id, binding=binding
    )
    return shared, binding, owner, decision, cursor_required


def _owner_lane_hint(snapshot: Any) -> str:
    matches = snapshot.get("owner_matches") if isinstance(snapshot, Mapping) else None
    if not isinstance(matches, list) or len(matches) != 1:
        raise RecoveryError("exact automatic owner initializer is not unique")
    owner = matches[0]
    decision = owner.get("decision") if isinstance(owner, Mapping) else None
    lane = str(decision.get("lane") or "") if isinstance(decision, Mapping) else ""
    if lane not in {"daily", "refresh", "backfill"}:
        raise RecoveryError("automatic owner lane is invalid")
    return lane


def _validate_background_failure(
    isolated_snapshot: Any,
    control: Any,
    *,
    generation_id: str,
    git_sha: str,
) -> tuple[dict[str, Any], dict[str, Any], dict[str, str]]:
    isolated = _validate_isolated_quiescence(isolated_snapshot)
    state, binding = _validate_control_identity(
        control, generation_id=generation_id, git_sha=git_sha
    )
    ready = state.get("phase") == "ready"
    abandoned = state.get("phase") == "abandoned"
    if (
        state.get("status") != "succeeded"
        or state.get("consumer") is not None
        or state.get("owner_dag_id") != OWNER_DAG_ID
        or (
            ready
            and (
                state.get("active") is not True or state.get("lock_active") is not True
            )
        )
        or (
            abandoned
            and (
                state.get("active") is not False
                or state.get("lock_active") is not False
                or not state.get("released_at")
            )
        )
        or not (ready or abandoned)
    ):
        raise RecoveryError(
            "background recovery requires exact ready/abandoned unclaimed generation"
        )
    owner, decision = _validate_owner_lineage(
        isolated,
        generation_id=generation_id,
        binding=binding,
        producer_failed=False,
    )
    lane = str(decision.get("lane") or "")
    if (
        lane not in {"refresh", "backfill"}
        or _normalize_state(owner.get("state")) != "failed"
    ):
        raise RecoveryError("normal completed background generation needs no recovery")
    start = _instant(
        binding["data_interval_start"], label="background publication start"
    )
    end = _instant(binding["data_interval_end"], label="background publication end")
    if (
        end - start != timedelta(minutes=5)
        or _canonical_instant(
            owner.get("data_interval_start"), label="background owner interval start"
        )
        != binding["data_interval_start"]
        or _canonical_instant(
            owner.get("data_interval_end"), label="background owner interval end"
        )
        != binding["data_interval_end"]
    ):
        raise RecoveryError("background publication interval differs from owner")
    target = expected_advanced_state(decision, recovered_at=isolated["observed_at"])
    current = isolated.get("scheduler_state")
    if current != decision.get("state") and not _state_matches_advanced(
        current, target
    ):
        raise RecoveryError(
            "automatic scheduler cursor changed outside background lineage"
        )
    return owner, decision, binding


def _same_control_state(before: Mapping[str, Any], current: Mapping[str, Any]) -> bool:
    fields = (
        "generation_id",
        "source",
        "status",
        "phase",
        "binding",
        "candidate",
        "consumer",
        "owner_dag_id",
        "active",
        "lock_active",
    )
    return all(before.get(field) == current.get(field) for field in fields)


def _validate_release_result(
    transition: Any,
    readback: Any,
    *,
    generation_id: str,
    binding: Mapping[str, Any],
) -> None:
    if (
        not isinstance(transition, Mapping)
        or transition.get("generation_id") != generation_id
        or transition.get("binding") != dict(binding)
        or transition.get("status") != "failed"
        or transition.get("phase") != "failed"
        or transition.get("active") is not False
        or transition.get("released") is not True
        or transition.get("safe_to_release") is not True
        or transition.get("consumer") is not None
        or not isinstance(readback, Mapping)
        or readback.get("generation_id") != generation_id
        or readback.get("binding") != dict(binding)
        or readback.get("status") != "failed"
        or readback.get("phase") != "failed"
        or readback.get("active") is not False
        or readback.get("lock_active") is not False
        or not readback.get("released_at")
    ):
        raise RecoveryError("failed generation release did not prove terminal readback")


def _validate_abandon_result(
    transition: Any,
    readback: Any,
    *,
    generation_id: str,
    binding: Mapping[str, Any],
) -> None:
    if (
        not isinstance(transition, Mapping)
        or transition.get("generation_id") != generation_id
        or transition.get("binding") != dict(binding)
        or transition.get("status") != "succeeded"
        or transition.get("phase") != "abandoned"
        or transition.get("active") is not False
        or transition.get("released") is not True
        or transition.get("published") is not False
        or transition.get("consumer") is not None
        or not isinstance(readback, Mapping)
        or readback.get("generation_id") != generation_id
        or readback.get("binding") != dict(binding)
        or readback.get("status") != "succeeded"
        or readback.get("phase") != "abandoned"
        or readback.get("active") is not False
        or readback.get("lock_active") is not False
    ):
        raise RecoveryError("ready generation abandon did not prove terminal readback")


def _report_base(
    context: Mapping[str, Any],
    generation_id: str,
    live: Mapping[str, Any],
) -> dict[str, Any]:
    return {
        "schema_version": SCHEMA_VERSION,
        "generated_at": _now(),
        "deployment_id": context.get("deployment_id"),
        "git_sha": context.get("git_sha"),
        "scheduler_container_id": context.get("scheduler_container_id"),
        "generation_id": generation_id,
        "live_runtime": dict(live),
    }


def recover_automatic_failure(
    args: argparse.Namespace,
    *,
    run: Callable[..., subprocess.CompletedProcess[str]] = subprocess.run,
) -> dict[str, Any]:
    if not args.execute or args.confirm != CONFIRM_RECOVERY:
        raise RecoveryError(
            f"nothing changed; pass --execute --confirm {CONFIRM_RECOVERY}"
        )
    generation_id = str(args.generation_id or "").strip().casefold()
    if _UUID_RE.fullmatch(generation_id) is None:
        raise RecoveryError("--generation-id must be one canonical UUID")

    context = _deployment_context(args)
    output = _validate_output_path(context, args.output)
    live = validate_live_runtimes(args, context, run=run)
    initial_control, binding = _validate_control_identity(
        _get_control_state(args, context, generation_id, run=run),
        generation_id=generation_id,
        git_sha=str(context.get("git_sha") or ""),
    )
    base = _report_base(context, generation_id, live)

    if initial_control.get("phase") == "consuming":
        report = {
            **base,
            "passed": False,
            "phase": "blocked_consuming",
            "recovery_required": True,
            "publication_state": initial_control,
            "safe_action": "retain_lock_and_inspect_exact_consumer",
            "operator_action": (
                "Do not release this lock. Preserve the report and inspect the "
                "exact Sofa/xref/E3/E4 lineage before a reviewed data repair."
            ),
        }
        _atomic_json(output, report)
        return report

    phase = str(initial_control.get("phase") or "")
    if phase not in {"writing", "failed", "ready", "abandoned"}:
        raise RecoveryError(
            f"publication phase {phase!r} has no automatic safe recovery"
        )

    isolated = _observe_isolated(args, context, generation_id, pause=True, run=run)
    if phase in {"writing", "failed"}:
        owner, decision, binding = _validate_producer_failure(
            isolated,
            initial_control,
            generation_id=generation_id,
            git_sha=str(context["git_sha"]),
        )
        second_isolated = _observe_isolated(
            args, context, generation_id, pause=False, run=run
        )
        _validate_producer_failure(
            second_isolated,
            initial_control,
            generation_id=generation_id,
            git_sha=str(context["git_sha"]),
        )
        mutation_context, final_live = _revalidate_mutation_boundary(
            args, context, run=run
        )
        current_control = _get_control_state(
            args, mutation_context, generation_id, run=run
        )
        if not _same_control_state(initial_control, current_control):
            raise RecoveryError("ControlStore generation changed during recovery proof")
        transition = _fail_and_release(args, mutation_context, generation_id, run=run)
        readback = _get_control_state(args, mutation_context, generation_id, run=run)
        _validate_release_result(
            transition,
            readback,
            generation_id=generation_id,
            binding=binding,
        )
        post_release_isolated = _observe_isolated(
            args, mutation_context, generation_id, pause=False, run=run
        )
        post_owner, post_decision, _post_binding = _validate_producer_failure(
            post_release_isolated,
            readback,
            generation_id=generation_id,
            git_sha=str(mutation_context["git_sha"]),
        )
        if post_owner.get("run_id") != owner.get("run_id") or post_decision != decision:
            raise RecoveryError("failed producer lineage changed after release")
        shared_pause = _pause_shared_for_rollout(mutation_context, run=run)
        _post_context, post_live = _revalidate_mutation_boundary(
            args, mutation_context, run=run
        )
        report = {
            **base,
            "passed": True,
            "phase": "failed_generation_released",
            "recovery_required": False,
            "isolated_writers_paused": True,
            "isolated_terminal_proof": post_release_isolated,
            "owner_run_id": owner.get("run_id"),
            "publication_before": initial_control,
            "publication_transition": transition,
            "publication_readback": readback,
            "live_runtime_at_mutation": final_live,
            "live_runtime_final": post_live,
            "shared_rollout_pause": shared_pause,
            "rollout_ready": shared_pause["rollout_ready"],
            "roll_forward": {
                "cursor_transition": None,
                "scheduler_state_unchanged": dict(decision["state"]),
                "next_eligible_boundary": _next_eligible_boundary(
                    decision,
                    binding,
                    generation_id=generation_id,
                    observed_at=post_release_isolated["observed_at"],
                ),
                "terminal_generation_reopen_allowed": False,
                "resume": "new_pristine_automatic_rollout_required",
            },
        }
    elif phase in {"ready", "abandoned"} and _owner_lane_hint(isolated) in {
        "refresh",
        "backfill",
    }:
        owner, decision, binding = _validate_background_failure(
            isolated,
            initial_control,
            generation_id=generation_id,
            git_sha=str(context["git_sha"]),
        )
        second_isolated = _observe_isolated(
            args, context, generation_id, pause=False, run=run
        )
        second_owner, second_decision, _second_binding = _validate_background_failure(
            second_isolated,
            initial_control,
            generation_id=generation_id,
            git_sha=str(context["git_sha"]),
        )
        if (
            second_owner.get("run_id") != owner.get("run_id")
            or second_decision != decision
        ):
            raise RecoveryError(
                "background owner lineage changed during recovery proof"
            )
        mutation_context, final_live = _revalidate_mutation_boundary(
            args, context, run=run
        )
        current_control = _get_control_state(
            args, mutation_context, generation_id, run=run
        )
        if not _same_control_state(initial_control, current_control):
            raise RecoveryError("ControlStore generation changed during recovery proof")
        recovered_at = _now()
        cursor = _advance_scheduler_cursor(
            args,
            mutation_context,
            decision,
            recovered_at=recovered_at,
            run=run,
        )
        target = expected_advanced_state(decision, recovered_at=recovered_at)
        if (
            not isinstance(cursor, Mapping)
            or cursor.get("schema_version") != CURSOR_TRANSITION_SCHEMA
            or not _state_matches_advanced(cursor.get("after"), target)
        ):
            raise RecoveryError("automatic background cursor did not roll forward")
        transition = _abandon_unclaimed(args, mutation_context, generation_id, run=run)
        readback = _get_control_state(args, mutation_context, generation_id, run=run)
        _validate_abandon_result(
            transition,
            readback,
            generation_id=generation_id,
            binding=binding,
        )
        shared_pause = _pause_shared_for_rollout(mutation_context, run=run)
        _post_context, post_live = _revalidate_mutation_boundary(
            args, mutation_context, run=run
        )
        report = {
            **base,
            "passed": True,
            "phase": "background_abandoned_cursor_recovered",
            "lane": decision["lane"],
            "recovery_required": False,
            "isolated_writers_paused": True,
            "isolated_terminal_proof": second_isolated,
            "owner_run_id": owner.get("run_id"),
            "publication_before": initial_control,
            "publication_transition": transition,
            "publication_readback": readback,
            "live_runtime_at_mutation": final_live,
            "live_runtime_final": post_live,
            "shared_rollout_pause": shared_pause,
            "rollout_ready": shared_pause["rollout_ready"],
            "roll_forward": {
                "cursor_transition": cursor,
                "terminal_generation_reopen_allowed": False,
                "resume": "new_pristine_automatic_rollout_required",
            },
        }
    elif phase in {"ready", "abandoned"}:
        shared = _observe_shared(context, generation_id, binding, run=run)
        shared_proof, binding, owner, decision, cursor_required = (
            _validate_ready_failure(
                isolated,
                shared,
                initial_control,
                generation_id=generation_id,
                git_sha=str(context["git_sha"]),
            )
        )
        second_isolated = _observe_isolated(
            args, context, generation_id, pause=False, run=run
        )
        second_shared = _observe_shared(context, generation_id, binding, run=run)
        (
            _second_shared,
            _second_binding,
            second_owner,
            second_decision,
            second_cursor_required,
        ) = _validate_ready_failure(
            second_isolated,
            second_shared,
            initial_control,
            generation_id=generation_id,
            git_sha=str(context["git_sha"]),
        )
        if (
            second_owner.get("run_id") != owner.get("run_id")
            or second_decision != decision
            or second_cursor_required is not cursor_required
        ):
            raise RecoveryError("ready owner lineage changed during recovery proof")
        mutation_context, final_live = _revalidate_mutation_boundary(
            args, context, run=run
        )
        current_control = _get_control_state(
            args, mutation_context, generation_id, run=run
        )
        if not _same_control_state(initial_control, current_control):
            raise RecoveryError("ControlStore generation changed during recovery proof")
        cursor = None
        if cursor_required:
            recovered_at = _now()
            cursor = _advance_scheduler_cursor(
                args,
                mutation_context,
                decision,
                recovered_at=recovered_at,
                run=run,
            )
            target = expected_advanced_state(decision, recovered_at=recovered_at)
            if (
                not isinstance(cursor, Mapping)
                or cursor.get("schema_version") != CURSOR_TRANSITION_SCHEMA
                or not _state_matches_advanced(cursor.get("after"), target)
            ):
                raise RecoveryError("automatic ready cursor did not roll forward")
        transition = _abandon_unclaimed(args, mutation_context, generation_id, run=run)
        readback = _get_control_state(args, mutation_context, generation_id, run=run)
        _validate_abandon_result(
            transition,
            readback,
            generation_id=generation_id,
            binding=binding,
        )
        shared_pause = _pause_shared_for_rollout(mutation_context, run=run)
        _post_context, post_live = _revalidate_mutation_boundary(
            args, mutation_context, run=run
        )
        report = {
            **base,
            "passed": True,
            "phase": "unclaimed_ready_abandoned",
            "recovery_required": False,
            "isolated_writers_paused": True,
            "isolated_terminal_proof": second_isolated,
            "shared_terminal_proof": shared_proof,
            "publication_before": initial_control,
            "publication_transition": transition,
            "publication_readback": readback,
            "live_runtime_at_mutation": final_live,
            "live_runtime_final": post_live,
            "shared_rollout_pause": shared_pause,
            "rollout_ready": shared_pause["rollout_ready"],
            "roll_forward": {
                "cursor_transition": cursor,
                "terminal_generation_reopen_allowed": False,
                "resume": "new_pristine_automatic_rollout_required",
            },
        }
    _atomic_json(output, report)
    return report


def _parser(argv: Sequence[str] | None = None) -> argparse.Namespace:
    repository = Path(__file__).resolve().parents[1]
    parser = argparse.ArgumentParser(
        description="Safely recover one terminal automatic FotMob generation"
    )
    parser.add_argument("--project", default="fotmob-airflow")
    parser.add_argument(
        "--compose-file",
        type=Path,
        default=repository / "deploy/fotmob/airflow.compose.yaml",
    )
    parser.add_argument("--env-file", type=Path, required=True)
    parser.add_argument("--deployment-report", type=Path, required=True)
    parser.add_argument("--generation-id", required=True)
    parser.add_argument("--output", type=Path, required=True)
    parser.add_argument("--execute", action="store_true")
    parser.add_argument("--confirm")
    return parser.parse_args(argv)


def _write_blocked_report(args: argparse.Namespace, payload: Mapping[str, Any]) -> None:
    """Write an error only after re-proving its protected evidence path."""

    try:
        context = _deployment_context(args)
        output = _validate_output_path(context, args.output)
        enriched = {
            **dict(payload),
            "deployment_id": context.get("deployment_id"),
            "git_sha": context.get("git_sha"),
            "scheduler_container_id": context.get("scheduler_container_id"),
        }
        _atomic_json(output, enriched)
    except (RecoveryError, OSError):
        # Invalid admission/output is precisely when writing would be unsafe.
        return


def main(argv: Sequence[str] | None = None) -> int:
    args = _parser(argv)
    try:
        report = recover_automatic_failure(args)
    except (RecoveryError, subprocess.CalledProcessError, OSError) as exc:
        error = {
            "schema_version": SCHEMA_VERSION,
            "generated_at": _now(),
            "passed": False,
            "phase": "blocked",
            "recovery_required": True,
            "generation_id": str(args.generation_id or ""),
            "safe_action": "retain_lock",
            "error": str(exc),
        }
        _write_blocked_report(args, error)
        print(json.dumps(error, indent=2), file=sys.stderr)
        return 2
    print(json.dumps(report, indent=2, default=str))
    return 0 if report.get("passed") is True else 2


if __name__ == "__main__":  # pragma: no cover
    raise SystemExit(main())
