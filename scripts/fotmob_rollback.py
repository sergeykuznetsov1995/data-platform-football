#!/usr/bin/env python3
"""Fail-closed rollback coordinator for FotMob consumers.

Rollback is a four-step operation, intentionally split across invocations:

1. ``plan`` records the exact procedure without changing anything.
2. ``pause --execute`` pauses all FotMob DAGs and proves no writer is running.
3. After an operator deploys the reviewed consumer revert, ``run-silver``
   creates a synthetic publication fence, runs exact Silver/DQ and abandons
   the candidate. ``recover-publication`` handles an ambiguous interrupted run
   without guessing whether its lock is safe to release.
4. ``validate`` proves the deployed revision, exact fenced Silver/DQ run,
   abandoned candidate and availability of every frozen legacy Bronze table.

The command never drops, truncates or otherwise mutates native Bronze objects.
"""

from __future__ import annotations

import argparse
import hashlib
import json
import os
import re
import subprocess
import sys
import tempfile
import time
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import Any, Callable, Mapping, Sequence

sys.dont_write_bytecode = True

try:  # package import in tests / ``python -m``
    from scripts.fotmob_acceptance import QueryClient, connect_from_env
    from scripts import fotmob_runtime as runtime_binding
except ModuleNotFoundError:  # direct ``python scripts/fotmob_rollback.py``
    from fotmob_acceptance import QueryClient, connect_from_env
    import fotmob_runtime as runtime_binding


DAGS = (
    "dag_orchestrate_fotmob",
    "dag_ingest_fotmob",
    "dag_transform_fotmob_silver",
    "dag_trigger_fotmob_daily",
    "dag_refresh_fotmob",
    "dag_backfill_fotmob",
)
PRESERVED_DYNAMIC_CATALOG_EVIDENCE = frozenset(
    {
        "fotmob_competitions",
        "fotmob_competitions_current",
        "fotmob_competition_scope_observations",
        "fotmob_competition_scope_observations_current",
    }
)
LEGACY_TABLES = (
    "fotmob_match_details",
    "fotmob_player_details",
    "fotmob_player_stats",
    "fotmob_schedule",
    "fotmob_team_leaderboards",
    "fotmob_team_profile",
    "fotmob_team_squad",
    "fotmob_team_stats",
    "fotmob_transfers",
)
CONFIRM_PAUSE = "PAUSE_FOTMOB_WRITERS"
CONFIRM_RUN_SILVER = "RUN_FOTMOB_ROLLBACK_VALIDATION_SILVER"
CONFIRM_RECOVER_PUBLICATION = "RECOVER_FOTMOB_ROLLBACK_PUBLICATION"
CONFIRM_RESTORE_MAINTENANCE = "RESTORE_FOTMOB_MAINTENANCE"
ROLLBACK_PUBLICATION_SCHEMA = "fotmob-rollback-publication-v1"
PUBLICATION_TTL_SECONDS = 14 * 24 * 60 * 60
SILVER_DAG_ID = "dag_transform_fotmob_silver"
SHARED_CONSUMER_DAG_ID = "dag_sofascore_pipeline"
ACTIVE_TASK_INSTANCE_STATES = (
    "queued",
    "running",
    "scheduled",
    "deferred",
    "up_for_retry",
    "up_for_reschedule",
    "restarting",
)


class RollbackError(RuntimeError):
    pass


def _now() -> str:
    return datetime.now(timezone.utc).isoformat().replace("+00:00", "Z")


def _atomic_json(path: Path, payload: Mapping[str, Any]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    temporary: Path | None = None
    try:
        with tempfile.NamedTemporaryFile(
            mode="w", encoding="utf-8", dir=path.parent, delete=False
        ) as stream:
            json.dump(payload, stream, ensure_ascii=False, indent=2)
            stream.write("\n")
            stream.flush()
            os.fsync(stream.fileno())
            temporary = Path(stream.name)
        temporary.replace(path)
        directory_fd = os.open(
            path.parent, os.O_RDONLY | getattr(os, "O_DIRECTORY", 0)
        )
        try:
            os.fsync(directory_fd)
        finally:
            os.close(directory_fd)
    finally:
        if temporary is not None and temporary.exists():
            temporary.unlink()


def _parse_json_array(output: str) -> list[dict[str, Any]]:
    decoder = json.JSONDecoder()
    for index, character in enumerate(output):
        if character != "[":
            continue
        try:
            value, _ = decoder.raw_decode(output[index:])
        except json.JSONDecodeError:
            continue
        if isinstance(value, list) and all(isinstance(row, dict) for row in value):
            return value
    raise RollbackError("Airflow command did not return a JSON array of objects")


def _timestamp(value: Any) -> datetime:
    try:
        parsed = datetime.fromisoformat(str(value).replace("Z", "+00:00"))
    except (TypeError, ValueError) as exc:
        raise RollbackError(f"invalid evidence timestamp: {value!r}") from exc
    if parsed.tzinfo is None:
        raise RollbackError("evidence timestamp must include a timezone")
    return parsed.astimezone(timezone.utc)


def _deployment_context(args: argparse.Namespace) -> Mapping[str, Any]:
    cached = getattr(args, "_deployment_context_cache", None)
    if isinstance(cached, Mapping):
        return cached
    try:
        payload = runtime_binding.load_deployment_context(
            args.deployment_report,
            project=args.project,
            compose_file=args.compose_file,
        )
    except runtime_binding.RuntimeBindingError as exc:
        raise RollbackError(str(exc)) from exc
    setattr(args, "_deployment_context_cache", payload)
    return payload


def _compose_environment(args: argparse.Namespace) -> dict[str, str]:
    return runtime_binding.compose_environment(_deployment_context(args))


def _compose_base(args: argparse.Namespace) -> tuple[str, ...]:
    try:
        return runtime_binding.compose_base(
            project=args.project,
            compose_file=args.compose_file,
            env_file=args.env_file,
        )
    except runtime_binding.RuntimeBindingError as exc:
        raise RollbackError(str(exc)) from exc


def _airflow(
    args: argparse.Namespace,
    *command: str,
    run: Callable[..., subprocess.CompletedProcess[str]] = subprocess.run,
) -> str:
    result = run(
        (
            *_compose_base(args),
            "exec",
            "-T",
            "airflow-scheduler",
            "airflow",
            *command,
        ),
        check=True,
        capture_output=True,
        text=True,
        env=_compose_environment(args),
    )
    return result.stdout


def _container_deploy_sha(
    args: argparse.Namespace,
    *,
    run: Callable[..., subprocess.CompletedProcess[str]],
) -> str:
    return run(
        (
            *_compose_base(args),
            "exec",
            "-T",
            "airflow-scheduler",
            "sh",
            "-c",
            "printenv FOTMOB_DEPLOY_GIT_SHA",
        ),
        check=True,
        capture_output=True,
        text=True,
        env=_compose_environment(args),
    ).stdout.strip()


def validate_live_deployment(
    args: argparse.Namespace,
    *,
    require_running: bool,
    run: Callable[..., subprocess.CompletedProcess[str]] = subprocess.run,
) -> dict[str, Any]:
    try:
        return runtime_binding.validate_live_deployment(
            _deployment_context(args),
            project=args.project,
            compose_file=args.compose_file,
            env_file=args.env_file,
            require_running=require_running,
            run=run,
        )
    except runtime_binding.RuntimeBindingError as exc:
        raise RollbackError(str(exc)) from exc


def validate_live_shared_runtime(
    args: argparse.Namespace,
    *,
    run: Callable[..., subprocess.CompletedProcess[str]] = subprocess.run,
) -> dict[str, Any]:
    try:
        return runtime_binding.validate_live_shared_runtime(
            _deployment_context(args), run=run
        )
    except runtime_binding.RuntimeBindingError as exc:
        raise RollbackError(str(exc)) from exc


def bind_admitted_trino(
    args: argparse.Namespace,
    *,
    require_running: bool,
    run: Callable[..., subprocess.CompletedProcess[str]] = subprocess.run,
) -> dict[str, Any]:
    try:
        return runtime_binding.bind_admitted_trino(
            _deployment_context(args),
            project=args.project,
            compose_file=args.compose_file,
            env_file=args.env_file,
            require_running=require_running,
            run=run,
        )
    except runtime_binding.RuntimeBindingError as exc:
        raise RollbackError(str(exc)) from exc


def require_no_active_fotmob_publication(
    args: argparse.Namespace,
    *,
    run: Callable[..., subprocess.CompletedProcess[str]] = subprocess.run,
) -> dict[str, Any]:
    try:
        return runtime_binding.assert_no_active_fotmob_publication(
            _deployment_context(args), run=run
        )
    except runtime_binding.RuntimeBindingError as exc:
        raise RollbackError(str(exc)) from exc


def _container_python_json(
    args: argparse.Namespace,
    *,
    code: str,
    marker: str,
    run: Callable[..., subprocess.CompletedProcess[str]],
    allow_null: bool = False,
) -> dict[str, Any] | None:
    output = run(
        (
            *_compose_base(args),
            "exec",
            "-T",
            "airflow-scheduler",
            "python",
            "-c",
            code,
        ),
        check=True,
        capture_output=True,
        text=True,
        env=_compose_environment(args),
    ).stdout
    for line in reversed(output.splitlines()):
        if not line.startswith(marker):
            continue
        try:
            payload = json.loads(line.removeprefix(marker))
        except json.JSONDecodeError as exc:
            raise RollbackError(f"invalid {marker} evidence") from exc
        if isinstance(payload, dict) or (allow_null and payload is None):
            return payload
        break
    raise RollbackError(f"container did not emit {marker} evidence")


def _rollback_publication_envelope(
    args: argparse.Namespace,
    *,
    run: Callable[..., subprocess.CompletedProcess[str]],
) -> dict[str, Any]:
    context = _deployment_context(args)
    attempt = int(args.publication_attempt)
    if attempt <= 0:
        raise RollbackError("--publication-attempt must be a positive integer")
    start = _timestamp(context["generated_at"]) + timedelta(seconds=attempt)
    end = start + timedelta(seconds=1)
    expected_start = start.isoformat(timespec="microseconds")
    expected_end = end.isoformat(timespec="microseconds")
    marker = "FOTMOB_ROLLBACK_PUBLICATION_BINDING_JSON="
    code = (
        "import json,sys; sys.path.insert(0,'/opt/airflow/dags'); "
        "from utils.fotmob_publication import make_publication_binding,make_generation_id; "
        f"b=make_publication_binding(owner='isolated',data_interval_start={start.isoformat()!r},"
        f"data_interval_end={end.isoformat()!r},fingerprint={context['git_sha']!r}); "
        f"print('{marker}'+json.dumps({{'generation_id':make_generation_id(b),'binding':b}},"
        "sort_keys=True))"
    )
    payload = _container_python_json(
        args, code=code, marker=marker, run=run
    )
    binding = payload.get("binding")
    generation_id = str(payload.get("generation_id", ""))
    if not isinstance(binding, Mapping):
        raise RollbackError("rollback publication binding is absent")
    if (
        binding.get("schema") != "fotmob-publication-v1"
        or binding.get("source") != "fotmob"
        or binding.get("owner") != "isolated"
        or binding.get("runtime_fingerprint") != context["git_sha"]
        or binding.get("data_interval_start") != expected_start
        or binding.get("data_interval_end") != expected_end
        or _timestamp(binding.get("data_interval_end"))
        - _timestamp(binding.get("data_interval_start"))
        != timedelta(seconds=1)
        or not re.fullmatch(
            r"[0-9a-f]{8}-[0-9a-f]{4}-[1-5][0-9a-f]{3}-[89ab][0-9a-f]{3}-[0-9a-f]{12}",
            generation_id,
        )
    ):
        raise RollbackError("rollback publication binding is not exact and synthetic")
    return {"generation_id": generation_id, "binding": dict(binding)}


def _initialize_rollback_publication(
    args: argparse.Namespace,
    publication: Mapping[str, Any],
    *,
    run: Callable[..., subprocess.CompletedProcess[str]],
) -> dict[str, Any]:
    marker = "FOTMOB_ROLLBACK_PUBLICATION_STATE_JSON="
    binding_json = json.dumps(publication["binding"], sort_keys=True)
    code = (
        "import json; from scrapers.fbref.control import ControlStore; "
        f"b=json.loads({binding_json!r}); "
        "r=ControlStore.from_env().initialize_publication_generation("
        f"{str(publication['generation_id'])!r},dag_id='fotmob_rollback_validation',"
        f"binding=b,source='fotmob',ttl_seconds={PUBLICATION_TTL_SECONDS}); "
        f"print('{marker}'+json.dumps(r,default=str,sort_keys=True))"
    )
    state = _container_python_json(args, code=code, marker=marker, run=run)
    if (
        state.get("generation_id") != publication["generation_id"]
        or state.get("binding") != publication["binding"]
        or state.get("status") != "running"
        or state.get("phase") != "writing"
        or state.get("active") is not True
        or state.get("owner_dag_id") != "fotmob_rollback_validation"
    ):
        raise RollbackError("rollback publication generation was not acquired exactly")
    return state


def _get_rollback_publication(
    args: argparse.Namespace,
    generation_id: str,
    *,
    run: Callable[..., subprocess.CompletedProcess[str]],
) -> dict[str, Any] | None:
    marker = "FOTMOB_ROLLBACK_PUBLICATION_STATE_JSON="
    code = (
        "import json; from scrapers.fbref.control import ControlStore; "
        "r=ControlStore.from_env().get_publication_generation("
        f"{generation_id!r},source='fotmob'); "
        f"print('{marker}'+json.dumps(r,default=str,sort_keys=True))"
    )
    return _container_python_json(
        args, code=code, marker=marker, run=run, allow_null=True
    )


def _transition_rollback_publication(
    args: argparse.Namespace,
    generation_id: str,
    *,
    action: str,
    run: Callable[..., subprocess.CompletedProcess[str]],
) -> dict[str, Any]:
    marker = "FOTMOB_ROLLBACK_PUBLICATION_STATE_JSON="
    if action == "seal":
        expression = (
            "s.seal_publication_generation("
            f"{generation_id!r},source='fotmob',ttl_seconds={PUBLICATION_TTL_SECONDS})"
        )
    elif action == "abandon":
        expression = (
            "s.complete_publication_generation("
            f"{generation_id!r},consumer=None,published=False,source='fotmob')"
        )
    elif action in {"fail_retain", "fail_release"}:
        safe = action == "fail_release"
        expression = (
            "s.fail_publication_generation("
            f"{generation_id!r},safe_to_release={safe!r},source='fotmob')"
        )
    else:  # pragma: no cover - internal callers use constants above
        raise AssertionError(action)
    code = (
        "import json; from scrapers.fbref.control import ControlStore; "
        f"s=ControlStore.from_env(); r={expression}; "
        f"print('{marker}'+json.dumps(r,default=str,sort_keys=True))"
    )
    return _container_python_json(args, code=code, marker=marker, run=run)


def _publication_state_summary(state: Mapping[str, Any]) -> dict[str, Any]:
    candidate = state.get("candidate")
    candidate_summary = None
    if isinstance(candidate, Mapping):
        candidate_summary = {
            "generation_id": candidate.get("generation_id"),
            "digest": candidate.get("digest"),
            "transform_task_ids": candidate.get("transform_task_ids"),
        }
    return {
        "generation_id": state.get("generation_id"),
        "status": state.get("status"),
        "phase": state.get("phase"),
        "active": state.get("active"),
        "released": state.get("released"),
        "published": state.get("published"),
        "candidate": candidate_summary,
    }


def inspect_writer_state(
    args: argparse.Namespace,
    *,
    run: Callable[..., subprocess.CompletedProcess[str]] = subprocess.run,
) -> dict[str, Any]:
    # One repeatable-read transaction covers pause flags plus DagRun and
    # TaskInstance activity. A terminal DagRun with a still-running task is not
    # quiescent and must fail cleanup/rollback just like a running DagRun.
    marker = "FOTMOB_WRITER_STATE_JSON="
    code = f"""
import json
from airflow.models import DagModel, DagRun, TaskInstance
from airflow.settings import Session
from sqlalchemy import text
ids = {tuple(DAGS)!r}
task_states = {ACTIVE_TASK_INSTANCE_STATES!r}
s = Session()
try:
    s.execute(text('SET TRANSACTION ISOLATION LEVEL REPEATABLE READ, READ ONLY'))
    models = s.query(DagModel.dag_id, DagModel.is_paused).filter(
        DagModel.dag_id.in_(ids)
    ).all()
    runs = s.query(DagRun.dag_id, DagRun.run_id, DagRun.state).filter(
        DagRun.dag_id.in_(ids), DagRun.state.in_(('running', 'queued'))
    ).all()
    tasks = s.query(
        TaskInstance.dag_id, TaskInstance.run_id,
        TaskInstance.task_id, TaskInstance.state
    ).filter(
        TaskInstance.dag_id.in_(ids), TaskInstance.state.in_(task_states)
    ).all()
    s.commit()
except Exception:
    s.rollback()
    raise
finally:
    s.close()
def state(value):
    return str(getattr(value, 'value', value) or '').lower().split('.')[-1]
payload = {{
    'pause_states': {{str(dag_id): bool(paused) for dag_id, paused in models}},
    'active_runs': [
        {{'dag_id': str(dag_id), 'run_id': str(run_id), 'state': state(run_state)}}
        for dag_id, run_id, run_state in runs
    ],
    'active_task_instances': [
        {{'dag_id': str(dag_id), 'run_id': str(run_id), 'task_id': str(task_id),
          'state': state(task_state)}}
        for dag_id, run_id, task_id, task_state in tasks
    ],
    'atomic_metadata_snapshot': True,
}}
print({marker!r} + json.dumps(payload, sort_keys=True))
"""
    result = run(
        (
            *_compose_base(args),
            "exec",
            "-T",
            "airflow-scheduler",
            "python",
            "-c",
            code,
        ),
        check=True,
        capture_output=True,
        text=True,
        env=_compose_environment(args),
    ).stdout
    payload = None
    for line in reversed(result.splitlines()):
        if line.startswith(marker):
            try:
                payload = json.loads(line.removeprefix(marker))
            except json.JSONDecodeError as exc:
                raise RollbackError("invalid atomic writer-state evidence") from exc
            break
    if not isinstance(payload, Mapping):
        raise RollbackError("atomic writer-state query returned invalid evidence")
    raw_pause_states = payload.get("pause_states")
    raw_active_runs = payload.get("active_runs")
    raw_active_tasks = payload.get("active_task_instances")
    if (
        not isinstance(raw_pause_states, Mapping)
        or set(raw_pause_states) != set(DAGS)
        or any(type(value) is not bool for value in raw_pause_states.values())
        or not isinstance(raw_active_runs, list)
        or not isinstance(raw_active_tasks, list)
        or payload.get("atomic_metadata_snapshot") is not True
    ):
        raise RollbackError("atomic writer-state query returned invalid evidence")
    active: dict[str, dict[str, list[str]]] = {}
    for row in raw_active_runs:
        if not isinstance(row, Mapping):
            raise RollbackError("atomic writer-state query returned invalid evidence")
        dag_id = str(row.get("dag_id", ""))
        if dag_id not in DAGS:
            raise RollbackError("atomic writer-state query returned an unknown DAG")
        run_id = row.get("run_id")
        state = str(row.get("state") or "").lower()
        if not run_id or state not in {"running", "queued"}:
            raise RollbackError("atomic writer-state active run evidence is malformed")
        active.setdefault(dag_id, {}).setdefault(state, []).append(str(run_id))
    active_tasks: list[dict[str, str]] = []
    for row in raw_active_tasks:
        if not isinstance(row, Mapping):
            raise RollbackError("atomic writer-state query returned invalid evidence")
        normalized = {
            "dag_id": str(row.get("dag_id") or ""),
            "run_id": str(row.get("run_id") or ""),
            "task_id": str(row.get("task_id") or ""),
            "state": str(row.get("state") or "").lower(),
        }
        if (
            normalized["dag_id"] not in DAGS
            or not normalized["run_id"]
            or not normalized["task_id"]
            or normalized["state"] not in ACTIVE_TASK_INSTANCE_STATES
        ):
            raise RollbackError("atomic writer-state active task evidence is malformed")
        active_tasks.append(normalized)
    return {
        "pause_states": dict(raw_pause_states),
        "active_runs": active,
        "active_task_instances": active_tasks,
        "atomic_metadata_snapshot": True,
    }


def pause_shared_consumer(
    args: argparse.Namespace,
    *,
    run: Callable[..., subprocess.CompletedProcess[str]] = subprocess.run,
) -> dict[str, Any]:
    """Atomically pause the admitted shared Sofa consumer while it is idle."""

    context = _deployment_context(args)
    handoff = context.get("shared_handoff_final")
    container_id = (
        str(handoff.get("shared_scheduler_container") or "")
        if isinstance(handoff, Mapping)
        else ""
    )
    if re.fullmatch(r"[0-9a-f]{64}", container_id) is None:
        raise RollbackError("rollback has no exact shared scheduler identity")
    pause_ids = tuple(sorted(runtime_binding.DESTRUCTIVE_SHARED_PAUSE_STATES))
    active_ids = tuple(sorted(runtime_binding.DESTRUCTIVE_SHARED_STATE_DAGS))
    marker = "FOTMOB_ROLLBACK_SHARED_PAUSE_JSON="
    code = f"""
import json
from airflow.models import DagModel, DagRun, TaskInstance, Variable
from airflow.settings import Session
from sqlalchemy import text

consumer = {SHARED_CONSUMER_DAG_ID!r}
pause_ids = {pause_ids!r}
active_ids = {active_ids!r}
task_states = {ACTIVE_TASK_INSTANCE_STATES!r}
s = Session()
try:
    s.execute(text('SET TRANSACTION ISOLATION LEVEL SERIALIZABLE'))
    models = s.query(DagModel).filter(
        DagModel.dag_id.in_(pause_ids)
    ).with_for_update().all()
    by_id = {{model.dag_id: model for model in models}}
    owner = s.query(Variable).filter(
        Variable.key == 'fotmob_schedule_owner'
    ).with_for_update().one_or_none()
    runs = s.query(DagRun.dag_id, DagRun.run_id, DagRun.state).filter(
        DagRun.dag_id.in_(active_ids), DagRun.state.in_(('running', 'queued'))
    ).with_for_update().all()
    tasks = s.query(
        TaskInstance.dag_id, TaskInstance.run_id,
        TaskInstance.task_id, TaskInstance.state
    ).filter(
        TaskInstance.dag_id.in_(active_ids),
        TaskInstance.state.in_(task_states)
    ).with_for_update().all()
    active = [
        {{'dag_id': dag_id, 'run_id': str(run_id),
          'state': str(getattr(state, 'value', state)).lower()}}
        for dag_id, run_id, state in runs
    ]
    active_tasks = [
        {{'dag_id': str(dag_id), 'run_id': str(run_id),
          'task_id': str(task_id),
          'state': str(getattr(state, 'value', state)).lower().split('.')[-1]}}
        for dag_id, run_id, task_id, state in tasks
    ]
    if set(by_id) != set(pause_ids):
        raise RuntimeError('exact shared DagModel rows are required')
    if owner is None or str(owner.val).strip().lower() != 'isolated':
        raise RuntimeError('shared schedule owner is not isolated')
    if active or active_tasks:
        raise RuntimeError('shared FotMob consumer/downstream work is active')
    before = {{dag_id: bool(by_id[dag_id].is_paused) for dag_id in pause_ids}}
    for model in by_id.values():
        model.is_paused = True
    s.flush()
    after = {{dag_id: bool(by_id[dag_id].is_paused) for dag_id in pause_ids}}
    s.commit()
except Exception:
    s.rollback()
    raise
finally:
    s.close()
payload = {{
    'shared_scheduler_container_id': {container_id!r},
    'schedule_owner': str(owner.val).strip().lower(),
    'pause_states_before': before,
    'pause_states_after': after,
    'active_runs': active,
    'active_task_instances': active_tasks,
    'atomic_metadata_transaction': True,
}}
print({marker!r} + json.dumps(payload, sort_keys=True))
"""
    output = run(
        ("docker", "exec", container_id, "python", "-c", code),
        check=True,
        capture_output=True,
        text=True,
    ).stdout
    payload = None
    for line in reversed(output.splitlines()):
        if line.startswith(marker):
            try:
                payload = json.loads(line.removeprefix(marker))
            except json.JSONDecodeError as exc:
                raise RollbackError("invalid shared pause evidence") from exc
            break
    expected_after = dict(runtime_binding.DESTRUCTIVE_SHARED_PAUSE_STATES)
    if (
        not isinstance(payload, Mapping)
        or payload.get("shared_scheduler_container_id") != container_id
        or payload.get("schedule_owner") != "isolated"
        or payload.get("pause_states_after") != expected_after
        or payload.get("active_runs") != []
        or payload.get("active_task_instances") != []
        or payload.get("atomic_metadata_transaction") is not True
    ):
        raise RollbackError("shared consumer pause/readback is incomplete")
    return dict(payload)


def inspect_shared_consumer_pause(
    args: argparse.Namespace,
    *,
    expected_container_id: str,
    run: Callable[..., subprocess.CompletedProcess[str]] = subprocess.run,
) -> dict[str, Any]:
    """Read the shared pause/idle fence without changing scheduler state."""

    context = _deployment_context(args)
    handoff = context.get("shared_handoff_final")
    container_id = (
        str(handoff.get("shared_scheduler_container") or "")
        if isinstance(handoff, Mapping)
        else ""
    )
    if (
        re.fullmatch(r"[0-9a-f]{64}", container_id) is None
        or container_id != expected_container_id
    ):
        raise RollbackError("shared scheduler identity changed after pause evidence")
    pause_ids = tuple(sorted(runtime_binding.DESTRUCTIVE_SHARED_PAUSE_STATES))
    active_ids = tuple(sorted(runtime_binding.DESTRUCTIVE_SHARED_STATE_DAGS))
    marker = "FOTMOB_ROLLBACK_SHARED_READBACK_JSON="
    code = f"""
import json
from airflow.models import DagModel, DagRun, TaskInstance, Variable
from airflow.settings import Session
from sqlalchemy import text
consumer = {SHARED_CONSUMER_DAG_ID!r}
pause_ids = {pause_ids!r}
active_ids = {active_ids!r}
task_states = {ACTIVE_TASK_INSTANCE_STATES!r}
s = Session()
try:
    s.execute(text('SET TRANSACTION ISOLATION LEVEL REPEATABLE READ, READ ONLY'))
    models = s.query(DagModel).filter(DagModel.dag_id.in_(pause_ids)).all()
    by_id = {{model.dag_id: bool(model.is_paused) for model in models}}
    owner = s.query(Variable).filter(Variable.key == 'fotmob_schedule_owner').one_or_none()
    runs = s.query(DagRun.dag_id, DagRun.run_id, DagRun.state).filter(
        DagRun.dag_id.in_(active_ids), DagRun.state.in_(('running', 'queued'))
    ).all()
    tasks = s.query(
        TaskInstance.dag_id, TaskInstance.run_id,
        TaskInstance.task_id, TaskInstance.state
    ).filter(
        TaskInstance.dag_id.in_(active_ids),
        TaskInstance.state.in_(task_states)
    ).all()
    active = [{{'dag_id': dag_id, 'run_id': str(run_id),
               'state': str(getattr(state, 'value', state)).lower()}}
              for dag_id, run_id, state in runs]
    active_tasks = [
        {{'dag_id': str(dag_id), 'run_id': str(run_id),
          'task_id': str(task_id),
          'state': str(getattr(state, 'value', state)).lower().split('.')[-1]}}
        for dag_id, run_id, task_id, state in tasks
    ]
    s.commit()
except Exception:
    s.rollback()
    raise
finally:
    s.close()
payload = {{
    'shared_scheduler_container_id': {container_id!r},
    'schedule_owner': str(owner.val).strip().lower() if owner is not None else None,
    'pause_states': by_id,
    'active_runs': active,
    'active_task_instances': active_tasks,
    'atomic_metadata_snapshot': True,
}}
print({marker!r} + json.dumps(payload, sort_keys=True))
"""
    output = run(
        ("docker", "exec", container_id, "python", "-c", code),
        check=True,
        capture_output=True,
        text=True,
    ).stdout
    payload = None
    for line in reversed(output.splitlines()):
        if line.startswith(marker):
            try:
                payload = json.loads(line.removeprefix(marker))
            except json.JSONDecodeError as exc:
                raise RollbackError("invalid shared pause readback") from exc
            break
    expected_pause = dict(runtime_binding.DESTRUCTIVE_SHARED_PAUSE_STATES)
    if (
        not isinstance(payload, Mapping)
        or payload.get("shared_scheduler_container_id") != container_id
        or payload.get("schedule_owner") != "isolated"
        or payload.get("pause_states") != expected_pause
        or payload.get("active_runs") != []
        or payload.get("active_task_instances") != []
        or payload.get("atomic_metadata_snapshot") is not True
    ):
        raise RollbackError("shared consumer is not paused and idle")
    return dict(payload)


def _read_protected_json(path: Path, *, label: str) -> tuple[dict[str, Any], bytes]:
    def reject_duplicate_keys(pairs: Sequence[tuple[str, Any]]) -> dict[str, Any]:
        payload: dict[str, Any] = {}
        for key, value in pairs:
            if key in payload:
                raise RollbackError(f"{label} contains duplicate JSON key: {key}")
            payload[key] = value
        return payload

    try:
        raw = path.read_bytes()
        payload = json.loads(
            raw.decode("utf-8"), object_pairs_hook=reject_duplicate_keys
        )
    except RollbackError:
        raise
    except (OSError, UnicodeDecodeError, json.JSONDecodeError) as exc:
        raise RollbackError(f"{label} is unreadable") from exc
    if not isinstance(payload, dict):
        raise RollbackError(f"{label} must contain a JSON object")
    return payload, raw


def _validate_maintenance_pause_evidence(
    args: argparse.Namespace,
) -> tuple[dict[str, Any], dict[str, bool], str]:
    path = getattr(args, "pause_evidence", None)
    if path is None:
        raise RollbackError("restore-maintenance requires --pause-evidence")
    payload, raw = _read_protected_json(Path(path), label="pause evidence")
    supplied_sha = str(getattr(args, "pause_evidence_sha256", "")).lower()
    actual_sha = hashlib.sha256(raw).hexdigest()
    if not re.fullmatch(r"[0-9a-f]{64}", supplied_sha) or supplied_sha != actual_sha:
        raise RollbackError("pause evidence SHA-256 differs from reviewed bytes")
    shared = payload.get("shared_consumer_pause")
    expected_paused = dict(runtime_binding.DESTRUCTIVE_SHARED_PAUSE_STATES)
    before = shared.get("pause_states_before") if isinstance(shared, Mapping) else None
    publication = payload.get("publication_quiescence_after")
    identity = payload.get("deployment_identity")
    old_shared_container = (
        str(shared.get("shared_scheduler_container_id") or "")
        if isinstance(shared, Mapping)
        else ""
    )
    if (
        payload.get("schema_version") != "fotmob-rollback-v1"
        or payload.get("passed") is not True
        or payload.get("mode") != "pause"
        or payload.get("project") != args.project
        or not re.fullmatch(r"[0-9a-f]{40}", str(payload.get("git_sha") or ""))
        or payload.get("paused") != list(DAGS)
        or payload.get("pause_states") != {dag_id: True for dag_id in DAGS}
        or payload.get("running_runs") != {}
        or payload.get("queued_runs") != {}
        or not isinstance(shared, Mapping)
        or re.fullmatch(r"[0-9a-f]{64}", old_shared_container) is None
        or shared.get("schedule_owner") != "isolated"
        or shared.get("pause_states_after") != expected_paused
        or shared.get("active_runs") != []
        or shared.get("active_task_instances") != []
        or shared.get("atomic_metadata_transaction") is not True
        or not isinstance(before, Mapping)
        or set(before) != set(expected_paused)
        or any(type(value) is not bool for value in before.values())
        or not isinstance(publication, Mapping)
        or publication.get("safe") is not True
        or publication.get("active") is not False
        or publication.get("shared_scheduler_container_id") != old_shared_container
        or publication.get("control_database_bound") is not True
        or not isinstance(identity, Mapping)
        or identity.get("git_sha") != payload.get("git_sha")
        or identity.get("shared_scheduler_container_id") != old_shared_container
        or re.fullmatch(
            r"[0-9a-f]{32}", str(identity.get("deployment_id") or "")
        )
        is None
        or re.fullmatch(
            r"[0-9a-f]{64}", str(identity.get("scheduler_container_id") or "")
        )
        is None
        or identity.get("control_database_bound") is not True
        or re.fullmatch(
            r"[0-9a-f]{64}",
            str(identity.get("control_database_fingerprint") or ""),
        )
        is None
        or identity.get("control_database_fingerprint")
        != publication.get("control_database_fingerprint")
        or re.fullmatch(
            r"[0-9a-f]{64}", str(identity.get("deployment_report_sha256") or "")
        )
        is None
    ):
        raise RollbackError("pause evidence is not an exact destructive pause proof")
    _timestamp(payload.get("generated_at"))
    original = {
        dag_id: bool(before[dag_id])
        for dag_id in sorted(runtime_binding.SHARED_MAINTENANCE_DAGS)
    }
    return payload, original, actual_sha


def _validate_terminal_maintenance_operation(
    args: argparse.Namespace,
    *,
    paused_at: datetime,
) -> dict[str, Any]:
    path = getattr(args, "operation_report", None)
    if path is None:
        raise RollbackError("restore-maintenance requires --operation-report")
    payload, raw = _read_protected_json(Path(path), label="operation report")
    supplied_sha = str(getattr(args, "operation_report_sha256", "")).lower()
    actual_sha = hashlib.sha256(raw).hexdigest()
    if not re.fullmatch(r"[0-9a-f]{64}", supplied_sha) or supplied_sha != actual_sha:
        raise RollbackError("operation report SHA-256 differs from reviewed bytes")

    schema = payload.get("schema_version")
    aborted = bool(getattr(args, "aborted_before_mutation", False))
    timestamp_value: Any = None
    kind: str | None = None
    terminal_identity: dict[str, Any] | None = None
    if (
        schema == "fotmob-cleanup-execution-v1"
        and payload.get("passed") is True
        and payload.get("phase") == "complete"
        and not aborted
    ):
        kind = "cleanup_complete"
        timestamp_value = payload.get("generated_at")
        stopped = payload.get("writer_quiescence_after")
        if (
            not isinstance(stopped, Mapping)
            or stopped.get("scheduler_stopped") is not True
            or stopped.get("pause_states") != {dag_id: True for dag_id in DAGS}
            or stopped.get("active_runs") != {}
            or stopped.get("active_task_instances") != []
            or stopped.get("atomic_metadata_snapshot") is not True
        ):
            raise RollbackError("cleanup report lacks exact stopped-writer proof")
    elif (
        schema == "fotmob-competition-purge-journal-v1"
        and payload.get("status") == "complete"
        and isinstance(payload.get("completed_at"), str)
        and all(
            payload.get(field) is None
            for field in (
                "phase_a_intent",
                "raw_delete_intent",
                "snapshot_expiration_intent",
            )
        )
        and not aborted
    ):
        kind = "purge_complete"
        timestamp_value = payload.get("completed_at")
    elif (
        schema == "fotmob-rollback-v1"
        and payload.get("passed") is True
        and payload.get("mode") == "validate"
        and payload.get("writers_paused") == list(DAGS)
        and not aborted
    ):
        kind = "rollback_validated"
        timestamp_value = payload.get("generated_at")
        context = _deployment_context(args)
        handoff = context.get("shared_handoff_final")
        current_shared = (
            str(handoff.get("shared_scheduler_container") or "")
            if isinstance(handoff, Mapping)
            else ""
        )
        identity = payload.get("deployment_identity")
        if (
            not isinstance(identity, Mapping)
            or set(identity)
            != {
                "deployment_id",
                "git_sha",
                "scheduler_container_id",
                "shared_scheduler_container_id",
                "control_database_bound",
                "control_database_fingerprint",
            }
            or identity.get("deployment_id") != context.get("deployment_id")
            or identity.get("git_sha") != context.get("git_sha")
            or identity.get("scheduler_container_id")
            != context.get("scheduler_container_id")
            or identity.get("shared_scheduler_container_id") != current_shared
            or identity.get("control_database_bound") is not True
            or re.fullmatch(
                r"[0-9a-f]{64}",
                str(identity.get("control_database_fingerprint") or ""),
            )
            is None
        ):
            raise RollbackError("rollback terminal report identity differs")
        terminal_identity = dict(identity)
    elif schema in {
        "fotmob-cleanup-plan-v1",
        "fotmob-competition-purge-plan-v2",
    }:
        if not aborted:
            raise RollbackError(
                "reviewed plan requires --aborted-before-mutation before restoration"
            )
        kind = "reviewed_plan_aborted"
        timestamp_value = datetime.now(timezone.utc).isoformat()
    else:
        raise RollbackError("operation report is not terminal")

    completed_at = _timestamp(timestamp_value)
    if kind != "reviewed_plan_aborted" and completed_at < paused_at:
        raise RollbackError("operation report predates the destructive pause")
    return {
        "kind": kind,
        "completed_at": completed_at.isoformat(),
        "artifact_created_at": payload.get("generated_at")
        or payload.get("created_at"),
        "deployment_identity": terminal_identity,
        "sha256": actual_sha,
    }


def _restore_shared_maintenance_transaction(
    args: argparse.Namespace,
    *,
    desired_maintenance_states: Mapping[str, bool],
    run: Callable[..., subprocess.CompletedProcess[str]],
) -> dict[str, Any]:
    context = _deployment_context(args)
    handoff = context.get("shared_handoff_final")
    container_id = (
        str(handoff.get("shared_scheduler_container") or "")
        if isinstance(handoff, Mapping)
        else ""
    )
    if re.fullmatch(r"[0-9a-f]{64}", container_id) is None:
        raise RollbackError("restore has no exact shared scheduler identity")
    maintenance_ids = tuple(sorted(runtime_binding.SHARED_MAINTENANCE_DAGS))
    if (
        set(desired_maintenance_states) != set(maintenance_ids)
        or any(type(value) is not bool for value in desired_maintenance_states.values())
    ):
        raise RollbackError("recorded maintenance pause states are incomplete")
    pause_ids = tuple(sorted(runtime_binding.DESTRUCTIVE_SHARED_PAUSE_STATES))
    active_ids = tuple(sorted(runtime_binding.DESTRUCTIVE_SHARED_STATE_DAGS))
    all_paused = dict(runtime_binding.DESTRUCTIVE_SHARED_PAUSE_STATES)
    desired_after = {**all_paused, **dict(desired_maintenance_states)}
    marker = "FOTMOB_MAINTENANCE_RESTORE_JSON="
    code = f"""
import json
from airflow.models import DagModel, DagRun, TaskInstance, Variable
from airflow.settings import Session
from sqlalchemy import text

pause_ids = {pause_ids!r}
active_ids = {active_ids!r}
maintenance_ids = {maintenance_ids!r}
task_states = {ACTIVE_TASK_INSTANCE_STATES!r}
all_paused = {all_paused!r}
desired = {desired_after!r}
s = Session()
try:
    s.execute(text('SET TRANSACTION ISOLATION LEVEL SERIALIZABLE'))
    models = s.query(DagModel).filter(
        DagModel.dag_id.in_(pause_ids)
    ).with_for_update().all()
    by_id = {{model.dag_id: model for model in models}}
    owner = s.query(Variable).filter(
        Variable.key == 'fotmob_schedule_owner'
    ).with_for_update().one_or_none()
    runs = s.query(DagRun.dag_id, DagRun.run_id, DagRun.state).filter(
        DagRun.dag_id.in_(active_ids), DagRun.state.in_(('running', 'queued'))
    ).with_for_update().all()
    tasks = s.query(
        TaskInstance.dag_id, TaskInstance.run_id,
        TaskInstance.task_id, TaskInstance.state
    ).filter(
        TaskInstance.dag_id.in_(active_ids),
        TaskInstance.state.in_(task_states)
    ).with_for_update().all()
    active = [
        {{'dag_id': dag_id, 'run_id': str(run_id),
          'state': str(getattr(state, 'value', state)).lower()}}
        for dag_id, run_id, state in runs
    ]
    active_tasks = [
        {{'dag_id': str(dag_id), 'run_id': str(run_id),
          'task_id': str(task_id),
          'state': str(getattr(state, 'value', state)).lower().split('.')[-1]}}
        for dag_id, run_id, task_id, state in tasks
    ]
    if set(by_id) != set(pause_ids):
        raise RuntimeError('exact shared DagModel rows are required')
    if owner is None or str(owner.val).strip().lower() != 'isolated':
        raise RuntimeError('shared schedule owner is not isolated')
    before = {{dag_id: bool(by_id[dag_id].is_paused) for dag_id in pause_ids}}
    if before not in (all_paused, desired):
        raise RuntimeError('shared pause state changed after destructive operation')
    allowed_active = [
        row for row in active
        if before == desired
        and row['dag_id'] in maintenance_ids
        and desired[row['dag_id']] is False
    ]
    if len(allowed_active) != len(active):
        raise RuntimeError('FotMob or paused maintenance DAG run is active')
    allowed_run_keys = {{(row['dag_id'], row['run_id']) for row in allowed_active}}
    allowed_active_tasks = [
        row for row in active_tasks
        if before == desired
        and row['dag_id'] in maintenance_ids
        and desired[row['dag_id']] is False
        and (row['dag_id'], row['run_id']) in allowed_run_keys
    ]
    if len(allowed_active_tasks) != len(active_tasks):
        raise RuntimeError('FotMob or orphaned maintenance task is active')
    for dag_id in maintenance_ids:
        model = by_id[dag_id]
        model.is_paused = desired[dag_id]
    s.flush()
    after = {{dag_id: bool(by_id[dag_id].is_paused) for dag_id in pause_ids}}
    if after != desired:
        raise RuntimeError('maintenance state readback differs')
    already_restored = before == desired
    schedule_owner = str(owner.val).strip().lower()
    s.commit()
except Exception:
    s.rollback()
    raise
finally:
    s.close()
payload = {{
    'shared_scheduler_container_id': {container_id!r},
    'schedule_owner': schedule_owner,
    'pause_states_before': before,
    'pause_states_after': after,
    'active_runs': active,
    'active_task_instances': active_tasks,
    'allowed_maintenance_runs': allowed_active,
    'allowed_maintenance_task_instances': allowed_active_tasks,
    'restored_dag_ids': list(maintenance_ids),
    'atomic_metadata_transaction': True,
    'already_restored': already_restored,
}}
print({marker!r} + json.dumps(payload, sort_keys=True))
"""
    output = run(
        ("docker", "exec", container_id, "python", "-c", code),
        check=True,
        capture_output=True,
        text=True,
    ).stdout
    payload = None
    for line in reversed(output.splitlines()):
        if line.startswith(marker):
            try:
                payload = json.loads(line.removeprefix(marker))
            except json.JSONDecodeError as exc:
                raise RollbackError("invalid maintenance restore evidence") from exc
            break
    before = payload.get("pause_states_before") if isinstance(payload, Mapping) else None
    if (
        not isinstance(payload, Mapping)
        or payload.get("shared_scheduler_container_id") != container_id
        or payload.get("schedule_owner") != "isolated"
        or before not in (all_paused, desired_after)
        or payload.get("pause_states_after") != desired_after
        or not isinstance(payload.get("active_runs"), list)
        or payload.get("allowed_maintenance_runs") != payload.get("active_runs")
        or any(
            not isinstance(row, Mapping)
            or row.get("dag_id") not in maintenance_ids
            or desired_after.get(str(row.get("dag_id"))) is not False
            or str(row.get("state") or "").lower() not in {"running", "queued"}
            or not str(row.get("run_id") or "")
            for row in payload.get("active_runs", [])
        )
        or not isinstance(payload.get("active_task_instances"), list)
        or payload.get("allowed_maintenance_task_instances")
        != payload.get("active_task_instances")
        or any(
            not isinstance(row, Mapping)
            or row.get("dag_id") not in maintenance_ids
            or desired_after.get(str(row.get("dag_id"))) is not False
            or str(row.get("state") or "").lower()
            not in ACTIVE_TASK_INSTANCE_STATES
            or not str(row.get("run_id") or "")
            or not str(row.get("task_id") or "")
            or not any(
                run_row.get("dag_id") == row.get("dag_id")
                and run_row.get("run_id") == row.get("run_id")
                for run_row in payload.get("active_runs", [])
                if isinstance(run_row, Mapping)
            )
            for row in payload.get("active_task_instances", [])
        )
        or payload.get("restored_dag_ids") != list(maintenance_ids)
        or payload.get("atomic_metadata_transaction") is not True
        or type(payload.get("already_restored")) is not bool
    ):
        raise RollbackError("maintenance restore transaction evidence is incomplete")
    return dict(payload)


def restore_maintenance(
    args: argparse.Namespace,
    *,
    run: Callable[..., subprocess.CompletedProcess[str]] = subprocess.run,
) -> dict[str, Any]:
    if not args.execute or args.confirm != CONFIRM_RESTORE_MAINTENANCE:
        raise RollbackError(
            "restore-maintenance requires --execute --confirm "
            f"{CONFIRM_RESTORE_MAINTENANCE}; nothing changed"
        )
    pause, original, pause_sha = _validate_maintenance_pause_evidence(args)
    terminal = _validate_terminal_maintenance_operation(
        args, paused_at=_timestamp(pause["generated_at"])
    )
    context = _deployment_context(args)
    handoff = context.get("shared_handoff_final")
    current_shared_container = (
        str(handoff.get("shared_scheduler_container") or "")
        if isinstance(handoff, Mapping)
        else ""
    )
    paused_shared_container = pause["shared_consumer_pause"][
        "shared_scheduler_container_id"
    ]
    if (
        paused_shared_container != current_shared_container
        and terminal["kind"] != "rollback_validated"
    ):
        raise RollbackError(
            "shared scheduler changed without an exact validated rollback rotation"
        )
    isolated_stopped = terminal["kind"] == "cleanup_complete"
    live_before = validate_live_deployment(
        args, require_running=not isolated_stopped, run=run
    )
    if isolated_stopped and live_before.get("scheduler_running") is not False:
        raise RollbackError("cleanup restore requires the admitted scheduler stopped")
    shared_runtime_before = validate_live_shared_runtime(args, run=run)
    paused_control_fingerprint = pause["deployment_identity"][
        "control_database_fingerprint"
    ]
    if (
        shared_runtime_before.get("control_database_fingerprint")
        != paused_control_fingerprint
    ):
        raise RollbackError("shared control database changed after destructive pause")
    terminal_identity = terminal.get("deployment_identity")
    if (
        terminal["kind"] == "rollback_validated"
        and (
            not isinstance(terminal_identity, Mapping)
            or terminal_identity.get("control_database_fingerprint")
            != paused_control_fingerprint
        )
    ):
        raise RollbackError("rollback rotation changed the shared control database")
    if isolated_stopped:
        writer_state = dict(
            _read_protected_json(
                Path(args.operation_report), label="operation report"
            )[0]["writer_quiescence_after"]
        )
    else:
        writer_state = inspect_writer_state(args, run=run)
        require_writers_stopped(writer_state)
    publication_before = require_no_active_fotmob_publication(args, run=run)
    transition = _restore_shared_maintenance_transaction(
        args, desired_maintenance_states=original, run=run
    )
    live_after = validate_live_deployment(
        args, require_running=not isolated_stopped, run=run
    )
    if isolated_stopped and live_after.get("scheduler_running") is not False:
        raise RollbackError("cleanup restore restarted the isolated scheduler")
    shared_runtime_after = validate_live_shared_runtime(args, run=run)
    if (
        shared_runtime_after.get("control_database_fingerprint")
        != paused_control_fingerprint
    ):
        raise RollbackError("shared control database changed during restoration")
    if isolated_stopped:
        final_writer_state = writer_state
    else:
        final_writer_state = inspect_writer_state(args, run=run)
        require_writers_stopped(final_writer_state)
    publication_after = require_no_active_fotmob_publication(args, run=run)
    return {
        "schema_version": "fotmob-maintenance-restore-v1",
        "generated_at": _now(),
        "passed": True,
        "mode": "restore-maintenance",
        "project": args.project,
        "pause_evidence_sha256": pause_sha,
        "operation_report_sha256": terminal["sha256"],
        "terminal_operation": terminal,
        "maintenance_pause_states_restored": original,
        "live_deployment_before": live_before,
        "shared_runtime_before": shared_runtime_before,
        "shared_state_before": transition["pause_states_before"],
        "atomic_restore": transition,
        "live_deployment_after": live_after,
        "shared_runtime_after": shared_runtime_after,
        "writer_state_after": final_writer_state,
        "publication_quiescence_before": publication_before,
        "publication_quiescence_after": publication_after,
    }


def require_writers_stopped(state: Mapping[str, Any]) -> None:
    pause_states = state.get("pause_states")
    if not isinstance(pause_states, Mapping):
        raise RollbackError("writer state has no pause-state evidence")
    not_paused = [dag_id for dag_id in DAGS if pause_states.get(dag_id) is not True]
    if not_paused:
        raise RollbackError(f"writers are not paused: {not_paused!r}")
    if state.get("active_runs"):
        raise RollbackError(f"writers still have active runs: {state['active_runs']!r}")
    if state.get("active_task_instances"):
        raise RollbackError(
            "writers still have active task instances: "
            f"{state['active_task_instances']!r}"
        )
    if (
        state.get("active_task_instances") != []
        or state.get("atomic_metadata_snapshot") is not True
    ):
        raise RollbackError("writer state lacks atomic active-task evidence")


def _sanitized_pause_failure(exc: Exception) -> str:
    """Describe a pause failure without copying commands or process output."""

    error_type = type(exc).__name__
    if isinstance(exc, subprocess.CalledProcessError):
        return f"{error_type}(returncode={exc.returncode})"
    return error_type


def _exact_silver_run(
    args: argparse.Namespace,
    run_id: str,
    *,
    run: Callable[..., subprocess.CompletedProcess[str]],
) -> dict[str, Any] | None:
    rows = _parse_json_array(
        _airflow(
            args,
            "dags",
            "list-runs",
            "-d",
            SILVER_DAG_ID,
            "--output",
            "json",
            run=run,
        )
    )
    matches = [row for row in rows if str(row.get("run_id")) == run_id]
    if len(matches) > 1:
        raise RollbackError("Airflow returned duplicate exact Silver run identity")
    return matches[0] if matches else None


def _pause_all_writers(
    args: argparse.Namespace,
    *,
    run: Callable[..., subprocess.CompletedProcess[str]],
) -> dict[str, Any]:
    failures: list[str] = []
    for dag_id in DAGS:
        try:
            _airflow(args, "dags", "pause", dag_id, run=run)
        except Exception as exc:
            failures.append(f"pause[{dag_id}]={_sanitized_pause_failure(exc)}")

    state: dict[str, Any] = {}
    try:
        state = inspect_writer_state(args, run=run)
    except Exception as exc:
        failures.append("inspect_writer_state=" + _sanitized_pause_failure(exc))
    try:
        require_writers_stopped(state)
    except Exception as exc:
        failures.append(
            "require_writers_stopped=" + _sanitized_pause_failure(exc)
        )

    if failures:
        raise RollbackError(
            "writer pause/quiescence failed: " + "; ".join(failures)
        )
    return state


def _candidate_from_state(
    state: Mapping[str, Any], publication: Mapping[str, Any]
) -> Mapping[str, Any]:
    candidate = state.get("candidate")
    if (
        state.get("generation_id") != publication["generation_id"]
        or state.get("binding") != publication["binding"]
        or not isinstance(candidate, Mapping)
        or candidate.get("generation_id") != publication["generation_id"]
        or not re.fullmatch(r"[0-9a-f]{64}", str(candidate.get("digest", "")))
    ):
        raise RollbackError("Silver run did not record the exact publication candidate")
    return candidate


def _rollback_publication_report_base(
    args: argparse.Namespace,
    publication: Mapping[str, Any],
    *,
    silver_run_id: str,
) -> dict[str, Any]:
    context = _deployment_context(args)
    return {
        "schema_version": ROLLBACK_PUBLICATION_SCHEMA,
        "generated_at": _now(),
        "passed": False,
        "mode": "run-silver",
        "phase": "prepared_pending_acquire",
        "project": args.project,
        "deployment_report": str(args.deployment_report.resolve()),
        "consumer_git_sha": context["git_sha"],
        "publication_attempt": int(args.publication_attempt),
        "publication": dict(publication),
        "silver_dag_id": SILVER_DAG_ID,
        "silver_run_id": silver_run_id,
        "native_objects_action": "retain",
    }


def run_rollback_validation_silver(
    args: argparse.Namespace,
    *,
    run: Callable[..., subprocess.CompletedProcess[str]] = subprocess.run,
    sleeper: Callable[[float], None] = time.sleep,
    monotonic: Callable[[], float] = time.monotonic,
) -> dict[str, Any]:
    """Run one fenced Silver validation and abandon its synthetic generation."""

    if not args.execute or args.confirm != CONFIRM_RUN_SILVER:
        raise RollbackError(
            "run-silver requires --execute --confirm " + CONFIRM_RUN_SILVER
        )
    expected_sha = _validate_sha(args.expected_consumer_sha)
    context = _deployment_context(args)
    if context.get("kept_paused") is not True or context["git_sha"] != expected_sha:
        raise RollbackError(
            "run-silver requires the exact --keep-paused rollback deployment"
        )
    validate_live_deployment(args, require_running=True, run=run)
    require_writers_stopped(inspect_writer_state(args, run=run))
    require_no_active_fotmob_publication(args, run=run)
    publication = _rollback_publication_envelope(args, run=run)
    silver_run_id = "rollback_silver__" + str(publication["generation_id"]).replace(
        "-", ""
    )
    if args.silver_run_id and args.silver_run_id != silver_run_id:
        raise RollbackError(
            f"run-silver uses deterministic --silver-run-id {silver_run_id!r}"
        )
    report = _rollback_publication_report_base(
        args, publication, silver_run_id=silver_run_id
    )
    # This is a write-ahead recovery identity. A process kill during DB acquire
    # still leaves the deterministic generation/run IDs needed for inspection.
    _atomic_json(args.output, report)
    try:
        acquired = _initialize_rollback_publication(args, publication, run=run)
    except Exception as exc:
        report.update(
            {
                "generated_at": _now(),
                "passed": False,
                "phase": "acquire_ambiguous",
                "error": f"{type(exc).__name__}: {exc}",
                "recovery_required": True,
            }
        )
        _atomic_json(args.output, report)
        return report
    report.update(
        {
            "generated_at": _now(),
            "phase": "acquired_pending_trigger",
            "publication_state": _publication_state_summary(acquired),
        }
    )
    _atomic_json(args.output, report)

    terminal: dict[str, Any] | None = None
    ambiguous_error: str | None = None
    writer_state: Mapping[str, Any] | None = None
    conf = json.dumps(
        {"fotmob_publication": publication}, sort_keys=True, separators=(",", ":")
    )
    try:
        _airflow(args, "dags", "unpause", SILVER_DAG_ID, run=run)
        _airflow(
            args,
            "dags",
            "trigger",
            SILVER_DAG_ID,
            "--run-id",
            silver_run_id,
            "--conf",
            conf,
            run=run,
        )
        report.update({"generated_at": _now(), "phase": "silver_running"})
        _atomic_json(args.output, report)
        deadline = monotonic() + max(1, int(args.timeout_seconds))
        while monotonic() < deadline:
            observed = _exact_silver_run(args, silver_run_id, run=run)
            state = str((observed or {}).get("state", "")).casefold()
            if state in {"success", "failed"}:
                terminal = observed
                break
            sleeper(2)
        if terminal is None:
            ambiguous_error = "exact Silver run did not reach a terminal state before timeout"
    except Exception as exc:
        ambiguous_error = f"{type(exc).__name__}: {exc}"
    finally:
        try:
            writer_state = _pause_all_writers(args, run=run)
        except Exception as exc:
            ambiguous_error = ambiguous_error or (
                f"writer quiescence could not be proven: {type(exc).__name__}: {exc}"
            )

    if ambiguous_error is not None or terminal is None or writer_state is None:
        try:
            retained = _transition_rollback_publication(
                args,
                str(publication["generation_id"]),
                action="fail_retain",
                run=run,
            )
            retained_summary: Mapping[str, Any] = _publication_state_summary(retained)
        except Exception as exc:
            retained_summary = {
                "generation_id": publication["generation_id"],
                "active": True,
                "retention_error": f"{type(exc).__name__}: {exc}",
            }
        report.update(
            {
                "generated_at": _now(),
                "passed": False,
                "phase": "lock_retained_pending_terminal_proof",
                "error": ambiguous_error or "Silver terminal state is ambiguous",
                "publication_state": retained_summary,
                "recovery_required": True,
            }
        )
        _atomic_json(args.output, report)
        return report

    run_state = str(terminal.get("state", "")).casefold()
    report["silver_terminal"] = dict(terminal)
    report["writer_state_after"] = dict(writer_state)
    if run_state == "failed":
        released = _transition_rollback_publication(
            args,
            str(publication["generation_id"]),
            action="fail_release",
            run=run,
        )
        report.update(
            {
                "generated_at": _now(),
                "passed": False,
                "phase": "failed_generation_released",
                "publication_state": _publication_state_summary(released),
                "error": "rollback Silver validation run failed",
            }
        )
        _atomic_json(args.output, report)
        return report

    generation_id = str(publication["generation_id"])
    try:
        state = _get_rollback_publication(args, generation_id, run=run)
        if state is None:
            raise RollbackError("successful Silver generation is absent")
        candidate = _candidate_from_state(state, publication)
        if (
            state.get("status") != "running"
            or state.get("phase") != "writing"
            or state.get("active") is not True
        ):
            raise RollbackError("successful Silver candidate is not in writing phase")
        sealed = _transition_rollback_publication(
            args, generation_id, action="seal", run=run
        )
        if (
            sealed.get("status") != "succeeded"
            or sealed.get("phase") != "ready"
            or sealed.get("active") is not True
        ):
            raise RollbackError("rollback publication generation was not sealed ready")
        abandoned = _transition_rollback_publication(
            args, generation_id, action="abandon", run=run
        )
        if (
            abandoned.get("status") != "succeeded"
            or abandoned.get("phase") != "abandoned"
            or abandoned.get("active") is not False
            or abandoned.get("released") is not True
            or abandoned.get("published") is not False
        ):
            raise RollbackError(
                "rollback publication generation was not abandoned safely"
            )
    except Exception as exc:
        retained_summary: Mapping[str, Any] = {
            "generation_id": generation_id,
            "active": True,
        }
        try:
            current = _get_rollback_publication(args, generation_id, run=run)
            if current is None:
                raise RollbackError("rollback publication generation is absent")
            if (
                current.get("status") == "running"
                and current.get("phase") == "writing"
            ):
                current = _transition_rollback_publication(
                    args, generation_id, action="fail_retain", run=run
                )
            retained_summary = _publication_state_summary(current)
        except Exception as state_exc:
            retained_summary = {
                **retained_summary,
                "retention_error": f"{type(state_exc).__name__}: {state_exc}",
            }
        report.update(
            {
                "generated_at": _now(),
                "passed": False,
                "phase": "lock_retained_pending_terminal_proof",
                "error": f"{type(exc).__name__}: {exc}",
                "publication_state": retained_summary,
                "recovery_required": True,
            }
        )
        _atomic_json(args.output, report)
        return report
    report.update(
        {
            "generated_at": _now(),
            "passed": True,
            "phase": "abandoned",
            "candidate": {
                "generation_id": candidate["generation_id"],
                "digest": candidate["digest"],
                "transform_task_ids": candidate.get("transform_task_ids"),
            },
            "publication_state": _publication_state_summary(abandoned),
            "recovery_required": False,
        }
    )
    _atomic_json(args.output, report)
    return report


def _load_rollback_publication_report(
    args: argparse.Namespace,
    *,
    run: Callable[..., subprocess.CompletedProcess[str]],
) -> tuple[dict[str, Any], dict[str, Any]]:
    path = getattr(args, "publication_report", None)
    if path is None:
        raise RollbackError("operation requires --publication-report")
    try:
        payload = json.loads(path.read_text(encoding="utf-8"))
    except (OSError, json.JSONDecodeError) as exc:
        raise RollbackError(f"invalid rollback publication report: {exc}") from exc
    if not isinstance(payload, dict) or payload.get("schema_version") != (
        ROLLBACK_PUBLICATION_SCHEMA
    ):
        raise RollbackError("unsupported rollback publication report")
    context = _deployment_context(args)
    if (
        payload.get("project") != args.project
        or payload.get("deployment_report")
        != str(args.deployment_report.resolve())
        or payload.get("consumer_git_sha") != context["git_sha"]
        or payload.get("publication_attempt") != int(args.publication_attempt)
        or payload.get("silver_dag_id") != SILVER_DAG_ID
    ):
        raise RollbackError("rollback publication report stack identity differs")
    publication = payload.get("publication")
    expected = _rollback_publication_envelope(args, run=run)
    if not isinstance(publication, Mapping) or dict(publication) != expected:
        raise RollbackError("rollback publication report binding is not exact")
    expected_run_id = "rollback_silver__" + expected["generation_id"].replace(
        "-", ""
    )
    if payload.get("silver_run_id") != expected_run_id:
        raise RollbackError("rollback publication report has a different Silver run")
    return payload, expected


def recover_rollback_publication(
    args: argparse.Namespace,
    *,
    run: Callable[..., subprocess.CompletedProcess[str]] = subprocess.run,
) -> dict[str, Any]:
    """Release a retained synthetic generation only after exact terminal proof."""

    if not args.execute or args.confirm != CONFIRM_RECOVER_PUBLICATION:
        raise RollbackError(
            "recover-publication requires --execute --confirm "
            + CONFIRM_RECOVER_PUBLICATION
        )
    report, publication = _load_rollback_publication_report(args, run=run)
    validate_live_deployment(args, require_running=True, run=run)
    writer_state = _pause_all_writers(args, run=run)
    silver_run_id = str(report["silver_run_id"])
    terminal = _exact_silver_run(args, silver_run_id, run=run)
    run_state = str((terminal or {}).get("state", "")).casefold()
    generation_id = str(publication["generation_id"])
    if terminal is None and report.get("phase") in {
        "prepared_pending_acquire",
        "acquire_ambiguous",
        "acquired_pending_trigger",
    }:
        state = _get_rollback_publication(args, generation_id, run=run)
        if state is None and report.get("phase") in {
            "prepared_pending_acquire",
            "acquire_ambiguous",
        }:
            recovered = {
                **report,
                "generated_at": _now(),
                "passed": False,
                "mode": "recover-publication",
                "phase": "no_generation_acquired",
                "writer_state_after": writer_state,
                "publication_state": None,
                "error": (
                    "exact generation and Silver run are both absent after "
                    "pre-acquire recovery; retry the same publication attempt"
                ),
                "recovery_required": False,
            }
            _atomic_json(args.output, recovered)
            return recovered
        if state is None:
            raise RollbackError(
                "acquired-pending-trigger report has no exact control DB generation"
            )
        if (
            state.get("generation_id") != generation_id
            or state.get("binding") != publication["binding"]
            or state.get("owner_dag_id") != "fotmob_rollback_validation"
            or state.get("status") != "running"
            or state.get("phase") != "writing"
            or state.get("active") is not True
        ):
            raise RollbackError(
                "pre-trigger recovery did not find the exact active writing generation"
            )
        released = _transition_rollback_publication(
            args, generation_id, action="fail_release", run=run
        )
        if (
            released.get("status") != "failed"
            or released.get("phase") != "failed"
            or released.get("active") is not False
            or released.get("released") is not True
        ):
            raise RollbackError(
                "pre-trigger publication generation was not released safely"
            )
        recovered = {
            **report,
            "generated_at": _now(),
            "passed": False,
            "mode": "recover-publication",
            "phase": "pretrigger_generation_released",
            "writer_state_after": writer_state,
            "publication_state": _publication_state_summary(released),
            "error": (
                "acquire committed but the exact Silver run was never created; "
                "generation released after pre-trigger quiescence proof. Retry with "
                f"--publication-attempt {int(args.publication_attempt) + 1}"
            ),
            "recovery_required": False,
        }
        _atomic_json(args.output, recovered)
        return recovered
    if run_state not in {"success", "failed"}:
        retained = {
            **report,
            "generated_at": _now(),
            "passed": False,
            "mode": "recover-publication",
            "phase": "lock_retained_pending_terminal_proof",
            "error": "exact Silver run is absent or non-terminal; lock retained",
            "recovery_required": True,
        }
        _atomic_json(args.output, retained)
        return retained

    state = _get_rollback_publication(args, generation_id, run=run)
    if state is None:
        raise RollbackError("exact rollback publication generation is absent")
    if state.get("binding") != publication["binding"]:
        raise RollbackError("retained publication binding differs from recovery report")
    phase = str(state.get("phase", "")).casefold()
    recovered = {
        **report,
        "generated_at": _now(),
        "mode": "recover-publication",
        "silver_terminal": dict(terminal),
        "writer_state_after": writer_state,
    }
    if run_state == "failed" or phase == "failed":
        if phase == "writing" or phase == "failed":
            released = _transition_rollback_publication(
                args, generation_id, action="fail_release", run=run
            )
        else:
            raise RollbackError(
                f"failed Silver run has unsafe publication phase {phase!r}"
            )
        recovered.update(
            {
                "passed": False,
                "phase": "failed_generation_released",
                "publication_state": _publication_state_summary(released),
                "error": (
                    "retained generation was released after terminal proof; "
                    "start a new rollback Silver validation with "
                    f"--publication-attempt {int(args.publication_attempt) + 1}"
                ),
                "recovery_required": False,
            }
        )
        _atomic_json(args.output, recovered)
        return recovered

    candidate = _candidate_from_state(state, publication)
    if phase == "writing":
        sealed = _transition_rollback_publication(
            args, generation_id, action="seal", run=run
        )
        if sealed.get("phase") != "ready" or sealed.get("active") is not True:
            raise RollbackError("recovered generation was not sealed ready")
        state = sealed
        phase = "ready"
    if phase == "ready":
        state = _transition_rollback_publication(
            args, generation_id, action="abandon", run=run
        )
        phase = str(state.get("phase", "")).casefold()
    if phase != "abandoned" or state.get("active") is not False:
        raise RollbackError("recovered generation was not abandoned safely")
    recovered.update(
        {
            "passed": True,
            "phase": "abandoned",
            "candidate": {
                "generation_id": candidate["generation_id"],
                "digest": candidate["digest"],
                "transform_task_ids": candidate.get("transform_task_ids"),
            },
            "publication_state": _publication_state_summary(state),
            "recovery_required": False,
        }
    )
    _atomic_json(args.output, recovered)
    return recovered


def validate_rollback_publication_evidence(
    args: argparse.Namespace,
    *,
    expected_sha: str,
    run: Callable[..., subprocess.CompletedProcess[str]],
) -> dict[str, Any]:
    report, publication = _load_rollback_publication_report(args, run=run)
    candidate = report.get("candidate")
    if (
        report.get("passed") is not True
        or report.get("phase") != "abandoned"
        or report.get("recovery_required") is not False
        or report.get("consumer_git_sha") != expected_sha
        or report.get("silver_run_id") != args.silver_run_id
        or not isinstance(candidate, Mapping)
        or candidate.get("generation_id") != publication["generation_id"]
        or not re.fullmatch(r"[0-9a-f]{64}", str(candidate.get("digest", "")))
    ):
        raise RollbackError("rollback publication report is not safely completed")
    state = _get_rollback_publication(
        args, str(publication["generation_id"]), run=run
    )
    if state is None:
        raise RollbackError("exact rollback publication generation is absent")
    observed_candidate = _candidate_from_state(state, publication)
    if (
        state.get("status") != "succeeded"
        or state.get("phase") != "abandoned"
        or state.get("active") is not False
        or observed_candidate.get("digest") != candidate.get("digest")
    ):
        raise RollbackError("control DB does not contain the abandoned exact candidate")
    return {
        "generation_id": publication["generation_id"],
        "binding": publication["binding"],
        "candidate_digest": candidate["digest"],
        "phase": "abandoned",
        "active": False,
        "released": True,
    }


def rollback_plan(args: argparse.Namespace) -> dict[str, Any]:
    context = _deployment_context(args)
    return {
        "schema_version": "fotmob-rollback-v1",
        "generated_at": _now(),
        "passed": True,
        "mode": "plan",
        "rollback_mode": "coordinator_only",
        "mutated": False,
        "project": args.project,
        "deployed_git_sha": context["git_sha"],
        "deployment_report": str(args.deployment_report.resolve()),
        "native_objects_action": "retain",
        "dynamic_catalog_evidence_action": "retain",
        "dynamic_catalog_evidence_objects": sorted(PRESERVED_DYNAMIC_CATALOG_EVIDENCE),
        "steps": [
            {
                "order": 1,
                "action": "pause_writers",
                "command": (
                    "python scripts/fotmob_rollback.py pause --execute "
                    f"--confirm {CONFIRM_PAUSE} <common options>"
                ),
            },
            {
                "order": 2,
                "action": "deploy_consumer_revert",
                "requirement": (
                    "Deploy a reviewed immutable commit whose FotMob Silver/xref/DQ "
                    "consumers read the frozen legacy tables. Do not alter native data."
                ),
            },
            {
                "order": 3,
                "action": "run_fenced_legacy_silver_and_dq",
                "command": (
                    "python scripts/fotmob_rollback.py run-silver --execute "
                    f"--confirm {CONFIRM_RUN_SILVER} "
                    "--expected-consumer-sha <40-hex> "
                    "--publication-attempt <positive-int> <common options>"
                ),
            },
            {
                "order": 4,
                "action": "validate",
                "command": (
                    "python scripts/fotmob_rollback.py validate "
                    "--expected-consumer-sha <40-hex> --silver-run-id <run-id> "
                    "--publication-report <run-silver-report> "
                    "--publication-attempt <same-int> <common options>"
                ),
            },
            {
                "order": 5,
                "action": "restore_maintenance",
                "command": (
                    "python scripts/fotmob_rollback.py restore-maintenance --execute "
                    f"--confirm {CONFIRM_RESTORE_MAINTENANCE} "
                    "--pause-evidence <pause-report> "
                    "--pause-evidence-sha256 <sha256> "
                    "--operation-report <validate-report> "
                    "--operation-report-sha256 <sha256> <common options>"
                ),
            },
        ],
    }


def pause_writers(
    args: argparse.Namespace,
    *,
    run: Callable[..., subprocess.CompletedProcess[str]] = subprocess.run,
) -> dict[str, Any]:
    if not args.execute or args.confirm != CONFIRM_PAUSE:
        raise RollbackError(
            f"pause requires --execute --confirm {CONFIRM_PAUSE}; nothing changed"
    )
    validate_live_deployment(args, require_running=True, run=run)
    publication_quiescence_before = require_no_active_fotmob_publication(
        args, run=run
    )
    shared_pause = pause_shared_consumer(args, run=run)
    _pause_all_writers(args, run=run)
    context = _deployment_context(args)
    deployed_sha = _container_deploy_sha(args, run=run)
    if deployed_sha != context["git_sha"]:
        raise RollbackError(
            "paused scheduler Git SHA differs from the admitted deployment report"
        )
    # Bracket the atomic metadata snapshot with live runtime attestation: a
    # container replacement on either side must fail rollback validation.
    validate_live_deployment(args, require_running=True, run=run)
    final_writer_state = inspect_writer_state(args, run=run)
    require_writers_stopped(final_writer_state)
    publication_quiescence_after = require_no_active_fotmob_publication(
        args, run=run
    )
    live_identity = validate_live_deployment(args, require_running=True, run=run)
    deployment_report_sha256 = hashlib.sha256(
        args.deployment_report.read_bytes()
    ).hexdigest()
    return {
        "schema_version": "fotmob-rollback-v1",
        "generated_at": _now(),
        "passed": True,
        "mode": "pause",
        "project": args.project,
        "catalog": args.catalog,
        "schema": args.bronze_schema,
        "git_sha": context["git_sha"],
        "paused": list(DAGS),
        "pause_states": final_writer_state["pause_states"],
        "running_runs": {},
        "queued_runs": {},
        "shared_consumer_pause": shared_pause,
        "deployment_identity": {
            "deployment_id": context["deployment_id"],
            "git_sha": context["git_sha"],
            "scheduler_container_id": context["scheduler_container_id"],
            "shared_scheduler_container_id": shared_pause[
                "shared_scheduler_container_id"
            ],
            "control_database_bound": publication_quiescence_after.get(
                "control_database_bound"
            )
            is True,
            "control_database_fingerprint": publication_quiescence_after.get(
                "control_database_fingerprint"
            ),
            "deployment_report_sha256": deployment_report_sha256,
        },
        "publication_quiescence_before": publication_quiescence_before,
        "publication_quiescence_after": publication_quiescence_after,
        "live_deployment": live_identity,
        "native_objects_action": "retain",
        "dynamic_catalog_evidence_action": "retain",
        "dynamic_catalog_evidence_objects": sorted(PRESERVED_DYNAMIC_CATALOG_EVIDENCE),
    }


def _validate_sha(value: str) -> str:
    sha = value.strip().lower()
    if len(sha) != 40 or any(character not in "0123456789abcdef" for character in sha):
        raise RollbackError("--expected-consumer-sha must be a full 40-hex Git SHA")
    return sha


def _legacy_counts(
    client: QueryClient, *, catalog: str, schema: str
) -> dict[str, int]:
    for value in (catalog, schema):
        if not re.fullmatch(r"[A-Za-z0-9_]+", value):
            raise RollbackError(f"unsafe SQL identifier: {value!r}")
    counts: dict[str, int] = {}
    for table in LEGACY_TABLES:
        rows = client.query(
            f'-- rollback:legacy:{table}\nSELECT COUNT(*) FROM "{catalog}"."{schema}"."{table}"'
        )
        if len(rows) != 1 or len(rows[0]) != 1:
            raise RollbackError(f"{table}: count query did not return one scalar")
        count = int(rows[0][0])
        if count <= 0:
            raise RollbackError(f"{table}: frozen legacy table is empty")
        counts[table] = count
    return counts


def validate_rollback(
    args: argparse.Namespace,
    *,
    run: Callable[..., subprocess.CompletedProcess[str]] = subprocess.run,
    client_factory: Callable[..., QueryClient] = connect_from_env,
) -> dict[str, Any]:
    expected_sha = _validate_sha(args.expected_consumer_sha)
    context = _deployment_context(args)
    if context.get("kept_paused") is not True:
        raise RollbackError(
            "rollback validation requires a deploy report created with --keep-paused"
        )
    if str(context.get("git_sha")) != expected_sha:
        raise RollbackError("deployment report Git SHA differs from consumer revert SHA")
    deployed_at = _timestamp(context["generated_at"])
    validate_live_deployment(args, require_running=True, run=run)
    env_result = _container_deploy_sha(args, run=run)
    if env_result != expected_sha:
        raise RollbackError(
            f"consumer revert is not deployed: expected={expected_sha}, observed={env_result!r}"
        )

    writer_state = inspect_writer_state(args, run=run)
    require_writers_stopped(writer_state)

    silver_runs = _parse_json_array(
        _airflow(
            args,
            "dags",
            "list-runs",
            "-d",
            "dag_transform_fotmob_silver",
            "--output",
            "json",
            run=run,
        )
    )
    matching = [row for row in silver_runs if str(row.get("run_id")) == args.silver_run_id]
    if len(matching) != 1 or str(matching[0].get("state")).lower() != "success":
        raise RollbackError(
            f"Silver/DQ run {args.silver_run_id!r} is absent or not successful"
        )
    run_started_at = _timestamp(matching[0].get("start_date"))
    if run_started_at < deployed_at:
        raise RollbackError("Silver/DQ evidence predates the rollback deployment")
    handoff = context.get("shared_handoff_final")
    shared_container_id = (
        str(handoff.get("shared_scheduler_container") or "")
        if isinstance(handoff, Mapping)
        else ""
    )
    shared_before = inspect_shared_consumer_pause(
        args, expected_container_id=shared_container_id, run=run
    )
    publication_evidence = validate_rollback_publication_evidence(
        args, expected_sha=expected_sha, run=run
    )
    publication_before = require_no_active_fotmob_publication(args, run=run)

    # Local/ambient Trino variables are not evidence. Bind the query client to
    # the exact non-secret endpoint and credential configured in the admitted
    # scheduler container before reading rollback data.
    trino_binding = bind_admitted_trino(args, require_running=True, run=run)
    if not args.trino_env_file:
        raise RollbackError("validate requires --trino-env-file")
    try:
        runtime_binding.load_host_trino_environment(args.trino_env_file)
    except runtime_binding.RuntimeBindingError as exc:
        raise RollbackError(str(exc)) from exc
    client = client_factory(catalog=args.catalog, schema=args.bronze_schema)
    try:
        marker_before = runtime_binding.validate_data_plane_marker(client, context)
        counts = _legacy_counts(
            client, catalog=args.catalog, schema=args.bronze_schema
        )
        marker_after = runtime_binding.validate_data_plane_marker(client, context)
    finally:
        client.close()
    live_identity = validate_live_deployment(args, require_running=True, run=run)
    final_writer_state = inspect_writer_state(args, run=run)
    require_writers_stopped(final_writer_state)
    shared_after = inspect_shared_consumer_pause(
        args, expected_container_id=shared_container_id, run=run
    )
    live_identity = validate_live_deployment(args, require_running=True, run=run)
    publication_after = require_no_active_fotmob_publication(args, run=run)
    return {
        "schema_version": "fotmob-rollback-v1",
        "generated_at": _now(),
        "passed": True,
        "mode": "validate",
        "consumer_git_sha": expected_sha,
        "silver_dq_run_id": args.silver_run_id,
        "silver_dq_started_at": run_started_at.isoformat(),
        "deployment_generated_at": deployed_at.isoformat(),
        "legacy_table_counts": counts,
        "writers_paused": list(DAGS),
        "pause_states": final_writer_state["pause_states"],
        "shared_consumer_readback_before": shared_before,
        "shared_consumer_readback_after": shared_after,
        "deployment_identity": {
            "deployment_id": context["deployment_id"],
            "git_sha": context["git_sha"],
            "scheduler_container_id": context["scheduler_container_id"],
            "shared_scheduler_container_id": shared_container_id,
            "control_database_bound": publication_after.get(
                "control_database_bound"
            )
            is True,
            "control_database_fingerprint": publication_after.get(
                "control_database_fingerprint"
            ),
        },
        "trino_binding": {
            "runtime": trino_binding["trino"],
            "data_plane_before": marker_before,
            "data_plane_after": marker_after,
        },
        "live_deployment": live_identity,
        "publication_quiescence_before": publication_before,
        "publication_quiescence_after": publication_after,
        "rollback_publication": publication_evidence,
        "native_objects_action": "retain",
        "dynamic_catalog_evidence_action": "retain",
        "dynamic_catalog_evidence_objects": sorted(PRESERVED_DYNAMIC_CATALOG_EVIDENCE),
    }


def build_parser() -> argparse.ArgumentParser:
    default_compose = Path(__file__).resolve().parents[1] / "deploy/fotmob/airflow.compose.yaml"
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument(
        "command",
        choices=(
            "plan",
            "pause",
            "run-silver",
            "recover-publication",
            "validate",
            "restore-maintenance",
        ),
    )
    parser.add_argument("--compose-file", type=Path, default=default_compose)
    parser.add_argument("--env-file", type=Path, required=True)
    parser.add_argument(
        "--trino-env-file",
        type=Path,
        help="Host-reachable Trino endpoint used only by validate",
    )
    parser.add_argument("--deployment-report", type=Path, required=True)
    parser.add_argument(
        "--publication-report",
        type=Path,
        help="Durable report from run-silver/recover-publication",
    )
    parser.add_argument(
        "--pause-evidence",
        type=Path,
        help="Durable report from pause used to recover maintenance states",
    )
    parser.add_argument("--pause-evidence-sha256", default="")
    parser.add_argument(
        "--operation-report",
        type=Path,
        help="Reviewed terminal cleanup, purge, rollback, or aborted plan report",
    )
    parser.add_argument("--operation-report-sha256", default="")
    parser.add_argument(
        "--aborted-before-mutation",
        action="store_true",
        help="Explicitly certify that a reviewed plan was abandoned before mutation",
    )
    parser.add_argument("--project", default="fotmob-airflow")
    parser.add_argument("--output", type=Path, required=True)
    parser.add_argument("--execute", action="store_true")
    parser.add_argument("--confirm")
    parser.add_argument("--expected-consumer-sha", default="")
    parser.add_argument("--silver-run-id", default="")
    parser.add_argument("--timeout-seconds", type=int, default=12 * 60 * 60)
    parser.add_argument(
        "--publication-attempt",
        type=int,
        default=1,
        help=(
            "Positive deterministic synthetic-generation attempt; increment only "
            "after an earlier attempt is proven terminal and released"
        ),
    )
    parser.add_argument("--catalog", default="iceberg")
    parser.add_argument("--bronze-schema", default="bronze")
    return parser


def main(argv: Sequence[str] | None = None) -> int:
    args = build_parser().parse_args(argv)
    try:
        if args.command == "plan":
            report = rollback_plan(args)
        elif args.command == "pause":
            report = pause_writers(args)
        elif args.command == "run-silver":
            report = run_rollback_validation_silver(args)
        elif args.command == "recover-publication":
            report = recover_rollback_publication(args)
        elif args.command == "validate":
            if not args.silver_run_id:
                raise RollbackError("validate requires --silver-run-id")
            if not args.publication_report:
                raise RollbackError("validate requires --publication-report")
            report = validate_rollback(args)
        else:
            report = restore_maintenance(args)
    except Exception as exc:
        existing: dict[str, Any] | None = None
        if args.command in {"run-silver", "recover-publication"}:
            try:
                candidate = json.loads(args.output.read_text(encoding="utf-8"))
                if (
                    isinstance(candidate, dict)
                    and candidate.get("schema_version")
                    == ROLLBACK_PUBLICATION_SCHEMA
                ):
                    existing = candidate
            except (OSError, json.JSONDecodeError):
                pass
        report = {
            **(existing or {}),
            "schema_version": (
                ROLLBACK_PUBLICATION_SCHEMA
                if existing is not None
                else "fotmob-rollback-v1"
            ),
            "generated_at": _now(),
            "passed": False,
            "mode": args.command,
            "error": f"{type(exc).__name__}: {exc}",
            "native_objects_action": "retain",
        }
        if existing is not None:
            report["recovery_required"] = True
    _atomic_json(args.output, report)
    print(json.dumps(report, ensure_ascii=False, sort_keys=True))
    return 0 if report.get("passed") is True else 1


if __name__ == "__main__":
    raise SystemExit(main())
