#!/usr/bin/env python3
"""Collect the first successful automatic FotMob daily publication.

The command is deliberately read-only until its final atomic report write.  It
validates the protected active deployment, re-attests both admitted scheduler
containers and their bind-mounted runtime manifests, and reads the isolated
and shared Airflow metadata databases independently.  The observation is
accepted only when the same exact lineage is returned twice without drift.
"""

from __future__ import annotations

import argparse
import hashlib
import json
import os
import re
import stat
import subprocess
import sys
import tempfile
import uuid
from datetime import date, datetime, timedelta, timezone
from pathlib import Path
from typing import Any, Callable, Mapping, Sequence

sys.dont_write_bytecode = True

try:  # package import in tests / ``python -m``
    from scripts import fotmob_runtime as runtime_binding
except ModuleNotFoundError:  # direct ``python scripts/fotmob_observe.py``
    import fotmob_runtime as runtime_binding


SCHEMA_VERSION = "fotmob-scheduled-observation-v1"
ISOLATED_SNAPSHOT_SCHEMA = "fotmob-scheduled-observation-isolated-v1"
SHARED_SNAPSHOT_SCHEMA = "fotmob-scheduled-observation-shared-v1"
OWNER_DAG_ID = "dag_orchestrate_fotmob"
INGEST_DAG_ID = "dag_ingest_fotmob"
SILVER_DAG_ID = "dag_transform_fotmob_silver"
SOFA_DAG_ID = "dag_sofascore_pipeline"
SOFA_FINALIZER_TASK_ID = "finalize_fotmob_publication"
PUBLICATION_CONF_KEY = "fotmob_publication"
LEGACY_OWNER_DAGS = frozenset(runtime_binding.LEGACY_OWNER_DAGS)
ACTIVE_AUTOMATIC_DAGS = frozenset(runtime_binding.AUTOMATIC_ACTIVE_DAGS)
ACTIVE_TASK_STATES = (
    "queued",
    "running",
    "scheduled",
    "deferred",
    "up_for_retry",
    "up_for_reschedule",
    "restarting",
)
MAX_PROTECTED_REPORT_BYTES = 2 * 1024 * 1024
_SHA256_RE = re.compile(r"[0-9a-f]{64}")
_FULL_GIT_SHA_RE = re.compile(r"[0-9a-f]{40}")
_DEPLOYMENT_ID_RE = re.compile(r"[0-9a-f]{32}")
_CONTAINER_ID_RE = re.compile(r"[0-9a-f]{64}")


class ObservationError(RuntimeError):
    """A protected or live observation failed closed."""


def _unique_json_object(pairs: Sequence[tuple[str, Any]]) -> dict[str, Any]:
    value: dict[str, Any] = {}
    for key, item in pairs:
        if key in value:
            raise ValueError(f"duplicate JSON key: {key}")
        value[key] = item
    return value


def _read_protected_json(path: Path, *, label: str) -> tuple[bytes, dict[str, Any]]:
    """Read one stable regular report without following a final symlink."""

    if not path.is_absolute():
        raise ObservationError(f"{label} must be an absolute protected path")
    try:
        direct = path.lstat()
        if stat.S_ISLNK(direct.st_mode):
            raise ObservationError(f"protected {label} cannot be a symlink")
        resolved = path.resolve(strict=True)
        before = resolved.lstat()
        raw = resolved.read_bytes()
        after = resolved.lstat()
    except ObservationError:
        raise
    except OSError as exc:
        raise ObservationError(f"cannot read protected {label}: {exc}") from exc
    if (
        not stat.S_ISREG(before.st_mode)
        or before.st_nlink != 1
        or before.st_dev != after.st_dev
        or before.st_ino != after.st_ino
        or before.st_size != after.st_size
        or before.st_mtime_ns != after.st_mtime_ns
        or not raw
        or len(raw) != before.st_size
        or len(raw) > MAX_PROTECTED_REPORT_BYTES
        or stat.S_IMODE(before.st_mode) & 0o022
    ):
        raise ObservationError(f"protected {label} is not a stable regular file")
    try:
        payload = json.loads(raw.decode("utf-8"), object_pairs_hook=_unique_json_object)
    except (UnicodeDecodeError, ValueError, json.JSONDecodeError) as exc:
        raise ObservationError(f"protected {label} is invalid JSON") from exc
    if not isinstance(payload, dict):
        raise ObservationError(f"protected {label} must contain a JSON object")
    return raw, payload


def _instant(value: Any, *, label: str) -> datetime:
    try:
        parsed = datetime.fromisoformat(str(value).replace("Z", "+00:00"))
    except (TypeError, ValueError) as exc:
        raise ObservationError(f"{label} is not an ISO-8601 instant") from exc
    if parsed.tzinfo is None or parsed.utcoffset() is None:
        raise ObservationError(f"{label} has no timezone")
    return parsed.astimezone(timezone.utc)


def _canonical_instant(value: Any, *, label: str) -> str:
    return _instant(value, label=label).isoformat(timespec="microseconds")


def _state(value: Any) -> str:
    return str(getattr(value, "value", value) or "").casefold().split(".")[-1]


def _scheduled_run_id(value: Any) -> str:
    return f"scheduled__{_instant(value, label='logical date').isoformat()}"


def _canonical_binding(value: Any, *, git_sha: str) -> dict[str, str]:
    fields = {
        "schema",
        "source",
        "owner",
        "data_interval_start",
        "data_interval_end",
        "runtime_fingerprint",
    }
    if not isinstance(value, Mapping) or set(value) != fields:
        raise ObservationError("publication binding is incomplete")
    binding = {
        "schema": str(value.get("schema") or ""),
        "source": str(value.get("source") or ""),
        "owner": str(value.get("owner") or ""),
        "data_interval_start": _canonical_instant(
            value.get("data_interval_start"), label="publication interval start"
        ),
        "data_interval_end": _canonical_instant(
            value.get("data_interval_end"), label="publication interval end"
        ),
        "runtime_fingerprint": str(value.get("runtime_fingerprint") or ""),
    }
    start = _instant(binding["data_interval_start"], label="publication interval start")
    end = _instant(binding["data_interval_end"], label="publication interval end")
    if (
        dict(value) != binding
        or binding["schema"] != "fotmob-publication-v1"
        or binding["source"] != "fotmob"
        or binding["owner"] != "isolated"
        or binding["runtime_fingerprint"] != git_sha
        or end - start != timedelta(hours=24)
        or any(
            instant.hour != 14
            or instant.minute != 0
            or instant.second != 0
            or instant.microsecond != 0
            for instant in (start, end)
        )
    ):
        raise ObservationError(
            "publication binding is not the exact 14:00 UTC daily interval"
        )
    return binding


def make_generation_id(binding: Mapping[str, Any]) -> str:
    material = json.dumps(dict(binding), sort_keys=True, separators=(",", ":"))
    return str(uuid.uuid5(uuid.NAMESPACE_URL, f"fotmob-publication:{material}"))


def _one(values: Any, *, label: str) -> Any:
    if not isinstance(values, list) or len(values) != 1:
        raise ObservationError(f"{label} is missing or ambiguous")
    return values[0]


def _require_no_active_tasks(values: Any, *, label: str) -> None:
    if not isinstance(values, list):
        raise ObservationError(f"{label} active task proof is malformed")
    if values:
        raise ObservationError(f"{label} has an active task instance")


def _deployment_identity(context: Mapping[str, Any]) -> dict[str, str]:
    identity = {
        "deployment_id": str(context.get("deployment_id") or ""),
        "git_sha": str(context.get("git_sha") or ""),
        "scheduler_container_id": str(context.get("scheduler_container_id") or ""),
    }
    if (
        _DEPLOYMENT_ID_RE.fullmatch(identity["deployment_id"]) is None
        or _FULL_GIT_SHA_RE.fullmatch(identity["git_sha"]) is None
        or _CONTAINER_ID_RE.fullmatch(identity["scheduler_container_id"]) is None
    ):
        raise ObservationError("active deployment identity is invalid")
    return identity


def load_active_deployment(args: argparse.Namespace) -> tuple[dict[str, Any], str]:
    """Load the exact protected active automatic deployment twice."""

    before, protected_payload = _read_protected_json(
        args.deployment_report, label="deployment report"
    )
    try:
        context = runtime_binding.load_deployment_context(
            args.deployment_report,
            project=args.project,
            compose_file=args.compose_file,
        )
    except runtime_binding.RuntimeBindingError as exc:
        raise ObservationError(str(exc)) from exc
    after, repeated_payload = _read_protected_json(
        args.deployment_report, label="deployment report"
    )
    if before != after or protected_payload != repeated_payload:
        raise ObservationError("protected deployment report drifted while loading")
    rollout = context.get("automatic_rollout_summary")
    if (
        context.get("activation_state") != "active"
        or context.get("kept_paused") is not False
        or set(context.get("paused") or ()) != LEGACY_OWNER_DAGS
        or set(context.get("unpaused") or ()) != ACTIVE_AUTOMATIC_DAGS
        or not isinstance(rollout, Mapping)
        or rollout.get("phase") != "active"
        or rollout.get("passed") is not True
        or not rollout.get("owner_at")
    ):
        raise ObservationError(
            "scheduled observation requires the exact active automatic deployment"
        )
    _instant(rollout["owner_at"], label="automatic owner activation")
    evidence = Path(str(context.get("evidence_dir") or ""))
    try:
        evidence = evidence.resolve(strict=True)
        deployment = args.deployment_report.resolve(strict=True)
        deployment.relative_to(evidence)
    except (OSError, ValueError) as exc:
        raise ObservationError(
            "deployment report is outside the protected evidence directory"
        ) from exc
    context["deployment_report"] = str(deployment)
    _deployment_identity(context)
    return context, hashlib.sha256(before).hexdigest()


def validate_output_path(context: Mapping[str, Any], output: Path) -> Path:
    """Resolve a canonical non-link target strictly below ``evidence_dir``."""

    if not output.is_absolute():
        raise ObservationError("observation output must be an absolute protected path")
    try:
        direct = output.lstat()
    except FileNotFoundError:
        direct = None
    except OSError as exc:
        raise ObservationError(f"cannot inspect observation output: {exc}") from exc
    if direct is not None and stat.S_ISLNK(direct.st_mode):
        raise ObservationError("observation output cannot be a symlink")
    if direct is not None and (
        not stat.S_ISREG(direct.st_mode) or direct.st_nlink != 1
    ):
        raise ObservationError("observation output must be one regular file")
    try:
        evidence = Path(str(context.get("evidence_dir") or "")).resolve(strict=True)
        parent = output.parent.resolve(strict=True)
        resolved = output.resolve(strict=False)
        parent.relative_to(evidence)
    except (OSError, ValueError) as exc:
        raise ObservationError(
            "observation output must stay inside the deployment evidence directory"
        ) from exc
    if not parent.is_dir() or output != resolved:
        raise ObservationError("observation output path is not canonical")
    deployment = Path(str(context.get("deployment_report") or "")).resolve()
    if resolved == deployment:
        raise ObservationError("observation output cannot replace deployment report")
    return resolved


def _atomic_json(path: Path, payload: Mapping[str, Any]) -> None:
    expected_raw = (json.dumps(payload, ensure_ascii=False, indent=2) + "\n").encode(
        "utf-8"
    )
    temporary: Path | None = None
    installed_here = False
    try:
        with tempfile.NamedTemporaryFile(
            mode="w",
            encoding="utf-8",
            dir=path.parent,
            prefix=".fotmob-observation-",
            delete=False,
        ) as stream:
            os.chmod(stream.name, 0o600)
            stream.write(expected_raw.decode("utf-8"))
            stream.flush()
            os.fsync(stream.fileno())
            temporary = Path(stream.name)
        try:
            # This same-filesystem link is atomic and cannot replace a report
            # created after the initial output-path check.
            os.link(temporary, path)
            installed_here = True
        except FileExistsError:
            existing_raw, existing = _read_protected_json(
                path, label="scheduled observation output"
            )
            if (
                existing != dict(payload)
                or existing_raw != expected_raw
                or stat.S_IMODE(path.lstat().st_mode) != 0o600
            ):
                raise ObservationError(
                    "observation output concurrently acquired unrelated evidence"
                ) from None
        temporary.unlink()
        temporary = None
        directory_fd = os.open(path.parent, os.O_RDONLY | getattr(os, "O_DIRECTORY", 0))
        try:
            os.fsync(directory_fd)
        finally:
            os.close(directory_fd)
    finally:
        if temporary is not None and temporary.exists():
            temporary.unlink()
    if not installed_here:
        return
    installed = path.lstat()
    if (
        not stat.S_ISREG(installed.st_mode)
        or stat.S_ISLNK(installed.st_mode)
        or installed.st_nlink != 1
        or stat.S_IMODE(installed.st_mode) != 0o600
    ):
        raise ObservationError("protected observation output installation failed")


def write_protected_observation(
    context: Mapping[str, Any], output: Path, report: Mapping[str, Any]
) -> None:
    target = validate_output_path(context, output)
    if (
        report.get("schema_version") != SCHEMA_VERSION
        or report.get("passed") is not True
    ):
        raise ObservationError("only a successful scheduled observation may be written")
    if target.exists():
        existing_raw, existing = _read_protected_json(
            target, label="scheduled observation output"
        )
        expected_raw = (json.dumps(report, ensure_ascii=False, indent=2) + "\n").encode(
            "utf-8"
        )
        if existing != dict(report) or existing_raw != expected_raw:
            raise ObservationError(
                "observation output already contains unrelated protected evidence"
            )
        if stat.S_IMODE(target.lstat().st_mode) != 0o600:
            raise ObservationError("existing observation output is not mode 0600")
        return
    _atomic_json(target, report)


def validate_live_runtimes(
    args: argparse.Namespace,
    context: Mapping[str, Any],
    *,
    run: Callable[..., subprocess.CompletedProcess[str]] = subprocess.run,
) -> dict[str, Any]:
    """Re-attest exact isolated/shared containers, manifests and control DB."""

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
        raise ObservationError(str(exc)) from exc
    if (
        isolated.get("scheduler_container_id") != context.get("scheduler_container_id")
        or shared.get("shared_scheduler_container_id")
        != (context.get("shared_handoff_final") or {}).get("shared_scheduler_container")
        or shared.get("control_database_bound") is not True
        or not shared.get("control_database_fingerprint")
    ):
        raise ObservationError(
            "live scheduler/control database attestation is incomplete"
        )
    return {"isolated": isolated, "shared": shared}


def _parse_marker(output: str, *, marker: str, label: str) -> dict[str, Any]:
    rows = [
        line.removeprefix(marker)
        for line in output.splitlines()
        if line.startswith(marker)
    ]
    if len(rows) != 1:
        raise ObservationError(f"{label} returned missing or ambiguous evidence")
    try:
        value = json.loads(rows[0], object_pairs_hook=_unique_json_object)
    except (ValueError, json.JSONDecodeError) as exc:
        raise ObservationError(f"{label} returned invalid JSON") from exc
    if not isinstance(value, dict):
        raise ObservationError(f"{label} did not return one JSON object")
    return value


def _container_json(
    container_id: str,
    *,
    code: str,
    marker: str,
    label: str,
    run: Callable[..., subprocess.CompletedProcess[str]],
) -> dict[str, Any]:
    if _CONTAINER_ID_RE.fullmatch(container_id) is None:
        raise ObservationError(f"{label} has no exact container identity")
    try:
        result = run(
            ("docker", "exec", container_id, "python", "-c", code),
            check=True,
            capture_output=True,
            text=True,
        )
    except subprocess.CalledProcessError as exc:
        raise ObservationError(f"cannot collect {label} evidence") from exc
    return _parse_marker(result.stdout, marker=marker, label=label)


def isolated_snapshot_code(context: Mapping[str, Any]) -> str:
    """Return the read-only isolated Airflow/ControlStore collector program."""

    rollout = context.get("automatic_rollout_summary")
    activation_at = rollout.get("owner_at") if isinstance(rollout, Mapping) else None
    expected = {
        "deployment_id": context.get("deployment_id"),
        "git_sha": context.get("git_sha"),
        "scheduler_container_id": context.get("scheduler_container_id"),
        "activation_at": activation_at,
    }
    marker = "FOTMOB_SCHEDULED_OBSERVATION_ISOLATED_JSON="
    return f"""
import json
import os
from collections.abc import Mapping
from datetime import datetime, timedelta, timezone
from airflow.models import DagRun, TaskInstance, XCom
from airflow.settings import Session
from scrapers.fbref.control import ControlStore

EXPECTED=json.loads({json.dumps(expected, sort_keys=True)!r})
ACTIVE_STATES={ACTIVE_TASK_STATES!r}
OWNER={OWNER_DAG_ID!r}
INGEST={INGEST_DAG_ID!r}
SILVER={SILVER_DAG_ID!r}

def state(value):
    return str(getattr(value, 'value', value) or '').casefold().split('.')[-1]

def instant(value):
    if value is None:
        return None
    return value.astimezone(timezone.utc).isoformat(timespec='microseconds')

def run_value(row):
    return {{
        'dag_id': str(row.dag_id),
        'run_id': str(row.run_id),
        'run_type': state(row.run_type),
        'state': state(row.state),
        'logical_date': instant(row.logical_date),
        'data_interval_start': instant(row.data_interval_start),
        'data_interval_end': instant(row.data_interval_end),
        'start_date': instant(row.start_date),
        'conf': dict(row.conf) if isinstance(row.conf, Mapping) else {{}},
    }}

def xcom_values(session, dag_id, run_id, task_id):
    rows=(session.query(XCom).filter(
        XCom.dag_id == dag_id,
        XCom.run_id == run_id,
        XCom.task_id == task_id,
        XCom.key == 'return_value',
    ).order_by(XCom.timestamp.desc()).all())
    return [XCom.deserialize_value(row) for row in rows]

def task_rows(session, dag_id, run_id, task_id):
    return session.query(TaskInstance).filter(
        TaskInstance.dag_id == dag_id,
        TaskInstance.run_id == run_id,
        TaskInstance.task_id == task_id,
    ).all()

def task_states(session, dag_id, run_id, task_id):
    return [state(row.state) for row in task_rows(session, dag_id, run_id, task_id)]

def task_starts(session, dag_id, run_id, task_id):
    return [instant(row.start_date) for row in task_rows(session, dag_id, run_id, task_id)]

def active_tasks(session, dag_id, run_id):
    rows=session.query(TaskInstance).filter(
        TaskInstance.dag_id == dag_id,
        TaskInstance.run_id == run_id,
        TaskInstance.state.in_(ACTIVE_STATES),
    ).all()
    return [{{'task_id': str(row.task_id), 'state': state(row.state)}} for row in rows]

activation=datetime.fromisoformat(EXPECTED['activation_at'].replace('Z', '+00:00'))
query_boundary=activation - timedelta(days=1)
session=Session()
try:
    owners=(session.query(DagRun).filter(
        DagRun.dag_id == OWNER,
        DagRun.state == 'success',
        DagRun.execution_date >= query_boundary,
    ).order_by(DagRun.start_date.asc(), DagRun.execution_date.asc(), DagRun.run_id.asc()).all())
    observed=[]
    store=ControlStore.from_env()
    for owner in owners:
        decisions=xcom_values(session, OWNER, owner.run_id, 'choose_fotmob_lane')
        daily=any(isinstance(item, Mapping) and item.get('lane') == 'daily' for item in decisions)
        initializers=(xcom_values(session, OWNER, owner.run_id, 'initialize_fotmob_publication') if daily else [])
        generation=(initializers[0].get('generation_id') if len(initializers) == 1 and isinstance(initializers[0], Mapping) else None)
        ingest_id=('fotmob_orchestrated__' + str(generation)) if generation else ''
        silver_id=('fotmob_silver__' + str(generation)) if generation else ''
        ingest_rows=(session.query(DagRun).filter(DagRun.dag_id == INGEST, DagRun.run_id == ingest_id).all() if ingest_id else [])
        silver_rows=(session.query(DagRun).filter(DagRun.dag_id == SILVER, DagRun.run_id == silver_id).all() if silver_id else [])
        observed.append({{
            'owner': run_value(owner),
            'attest_task_states': task_states(session, OWNER, owner.run_id, 'attest_isolated_runtime'),
            'attest_task_start_dates': task_starts(session, OWNER, owner.run_id, 'attest_isolated_runtime'),
            'attestations': xcom_values(session, OWNER, owner.run_id, 'attest_isolated_runtime'),
            'decisions': decisions,
            'initializers': initializers,
            'trigger_states': task_states(session, OWNER, owner.run_id, 'trigger_fotmob_ingest'),
            'active_tasks': active_tasks(session, OWNER, owner.run_id),
            'ingest_runs': [run_value(row) for row in ingest_rows],
            'ingest_trigger_states': (task_states(session, INGEST, ingest_id, 'trigger_silver_transform') if ingest_id else []),
            'ingest_active_tasks': (active_tasks(session, INGEST, ingest_id) if ingest_id else []),
            'silver_runs': [run_value(row) for row in silver_rows],
            'silver_active_tasks': (active_tasks(session, SILVER, silver_id) if silver_id else []),
            'publication': (store.get_publication_generation(generation, source='fotmob') if generation else None),
        }})
finally:
    session.close()

payload={{
    'schema_version': {ISOLATED_SNAPSHOT_SCHEMA!r},
    'deployment': {{
        'deployment_id': os.environ.get('FOTMOB_DEPLOYMENT_ID'),
        'git_sha': os.environ.get('FOTMOB_DEPLOY_GIT_SHA'),
        'scheduler_container_id': EXPECTED['scheduler_container_id'],
    }},
    'activation_at': EXPECTED['activation_at'],
    'daily_runs': observed,
}}
print({marker!r} + json.dumps(payload, default=str, sort_keys=True))
""".strip()


def shared_snapshot_code(generation_id: str, binding: Mapping[str, str]) -> str:
    """Return the read-only shared Airflow collector program."""

    expected = {"generation_id": generation_id, "binding": dict(binding)}
    expected_run_id = _scheduled_run_id(binding["data_interval_start"])
    marker = "FOTMOB_SCHEDULED_OBSERVATION_SHARED_JSON="
    return f"""
import json
from collections.abc import Mapping
from datetime import datetime, timezone
from airflow.models import DagRun, TaskInstance, XCom
from airflow.settings import Session
from sqlalchemy import or_

EXPECTED=json.loads({json.dumps(expected, sort_keys=True)!r})
EXPECTED_RUN_ID={expected_run_id!r}
ACTIVE_STATES={ACTIVE_TASK_STATES!r}
SOFA={SOFA_DAG_ID!r}
FINALIZER={SOFA_FINALIZER_TASK_ID!r}

def state(value):
    return str(getattr(value, 'value', value) or '').casefold().split('.')[-1]

def instant(value):
    if value is None:
        return None
    return value.astimezone(timezone.utc).isoformat(timespec='microseconds')

def xcom_values(session, run_id):
    rows=(session.query(XCom).filter(
        XCom.dag_id == SOFA,
        XCom.run_id == run_id,
        XCom.task_id == FINALIZER,
        XCom.key == 'return_value',
    ).order_by(XCom.timestamp.desc()).all())
    return [XCom.deserialize_value(row) for row in rows]

def task_rows(session, run_id):
    return session.query(TaskInstance).filter(
        TaskInstance.dag_id == SOFA,
        TaskInstance.run_id == run_id,
        TaskInstance.task_id == FINALIZER,
    ).all()

def active_tasks(session, run_id):
    rows=session.query(TaskInstance).filter(
        TaskInstance.dag_id == SOFA,
        TaskInstance.run_id == run_id,
        TaskInstance.state.in_(ACTIVE_STATES),
    ).all()
    return [{{'task_id': str(row.task_id), 'state': state(row.state)}} for row in rows]

start=datetime.fromisoformat(EXPECTED['binding']['data_interval_start'])
session=Session()
try:
    rows=(session.query(DagRun).filter(
        DagRun.dag_id == SOFA,
        or_(DagRun.run_id == EXPECTED_RUN_ID, DagRun.execution_date == start),
    ).order_by(DagRun.start_date.asc(), DagRun.run_id.asc()).all())
    runs=[]
    for row in rows:
        finalizers=task_rows(session, row.run_id)
        runs.append({{
            'dag_id': str(row.dag_id),
            'run_id': str(row.run_id),
            'run_type': state(row.run_type),
            'state': state(row.state),
            'logical_date': instant(row.logical_date),
            'data_interval_start': instant(row.data_interval_start),
            'data_interval_end': instant(row.data_interval_end),
            'finalizer_states': [state(item.state) for item in finalizers],
            'finalizer_xcoms': xcom_values(session, row.run_id),
            'active_tasks': active_tasks(session, row.run_id),
        }})
finally:
    session.close()

payload={{
    'schema_version': {SHARED_SNAPSHOT_SCHEMA!r},
    'expected': EXPECTED,
    'runs': runs,
}}
print({marker!r} + json.dumps(payload, default=str, sort_keys=True))
""".strip()


def collect_isolated_snapshot(
    args: argparse.Namespace,
    context: Mapping[str, Any],
    *,
    run: Callable[..., subprocess.CompletedProcess[str]] = subprocess.run,
) -> dict[str, Any]:
    del args
    return _container_json(
        str(context.get("scheduler_container_id") or ""),
        code=isolated_snapshot_code(context),
        marker="FOTMOB_SCHEDULED_OBSERVATION_ISOLATED_JSON=",
        label="isolated scheduled observation",
        run=run,
    )


def collect_shared_snapshot(
    context: Mapping[str, Any],
    generation_id: str,
    binding: Mapping[str, str],
    *,
    run: Callable[..., subprocess.CompletedProcess[str]] = subprocess.run,
) -> dict[str, Any]:
    handoff = context.get("shared_handoff_final")
    container_id = (
        str(handoff.get("shared_scheduler_container") or "")
        if isinstance(handoff, Mapping)
        else ""
    )
    return _container_json(
        container_id,
        code=shared_snapshot_code(generation_id, binding),
        marker="FOTMOB_SCHEDULED_OBSERVATION_SHARED_JSON=",
        label="shared scheduled observation",
        run=run,
    )


def _ignored_owner_decision(values: Any) -> bool:
    if not isinstance(values, list) or len(values) != 1:
        return False
    value = values[0]
    if value is False:
        return True
    return isinstance(value, Mapping) and value.get("lane") in {
        "refresh",
        "backfill",
    }


def _validate_isolated_snapshot(
    context: Mapping[str, Any], snapshot: Mapping[str, Any]
) -> dict[str, Any]:
    identity = _deployment_identity(context)
    rollout = context.get("automatic_rollout_summary")
    activation_at = rollout.get("owner_at") if isinstance(rollout, Mapping) else None
    activation = _instant(activation_at, label="automatic owner activation")
    if (
        not isinstance(snapshot, Mapping)
        or snapshot.get("schema_version") != ISOLATED_SNAPSHOT_SCHEMA
        or snapshot.get("deployment") != identity
        or _instant(snapshot.get("activation_at"), label="isolated activation boundary")
        != activation
    ):
        raise ObservationError("isolated observation deployment identity drifted")
    observed = snapshot.get("daily_runs")
    if not isinstance(observed, list) or any(
        not isinstance(item, Mapping) for item in observed
    ):
        raise ObservationError("isolated owner observation is malformed")

    candidates: list[Mapping[str, Any]] = []
    for item in observed:
        decisions = item.get("decisions")
        if _ignored_owner_decision(decisions):
            continue
        if not isinstance(decisions, list) or not any(
            isinstance(value, Mapping) and value.get("lane") == "daily"
            for value in decisions
        ):
            raise ObservationError("automatic owner lane evidence is invalid")
        owner = item.get("owner")
        if not isinstance(owner, Mapping):
            raise ObservationError("daily owner evidence is malformed")
        raw_attest_starts = item.get("attest_task_start_dates")
        parsed_attest_starts = (
            [
                _instant(value, label="owner attestation start")
                for value in raw_attest_starts
            ]
            if isinstance(raw_attest_starts, list)
            else []
        )
        # Historical rows in the deliberately broad query window are not
        # candidates.  Ignore them before requiring task/XCom completeness;
        # otherwise one stale failed or pre-task DagRun would poison every
        # later observation.  A post-activation malformed row still fails.
        if parsed_attest_starts and max(parsed_attest_starts) < activation:
            continue
        if not parsed_attest_starts:
            owner_started = _instant(owner.get("start_date"), label="owner start date")
            if owner_started < activation:
                continue
        if _state(owner.get("run_type")) != "scheduled":
            raise ObservationError("manual daily owner run is forbidden")
        if (
            _one(item.get("attest_task_states"), label="owner attestation task")
            != "success"
        ):
            raise ObservationError("owner attestation task did not succeed")
        attest_started = _instant(
            _one(raw_attest_starts, label="owner attestation start"),
            label="owner attestation start",
        )
        # DagRun creation/start can race the transaction helper's owner
        # readback.  The successful attestation task is the admission edge:
        # it can read the active certificate only after that report exists.
        if attest_started < activation:
            continue
        attestation = _one(item.get("attestations"), label="owner attestation XCom")
        if not isinstance(attestation, Mapping) or any(
            attestation.get(key) != value for key, value in identity.items()
        ):
            raise ObservationError("owner attestation XCom differs from deployment")
        candidates.append(item)
    if not candidates:
        raise ObservationError(
            "no successful scheduled DAILY owner exists after activation"
        )

    def owner_order(item: Mapping[str, Any]) -> tuple[datetime, datetime, str]:
        owner = item.get("owner") or {}
        return (
            _instant(
                _one(
                    item.get("attest_task_start_dates"),
                    label="owner attestation start",
                ),
                label="owner attestation start",
            ),
            _instant(owner.get("logical_date"), label="owner logical date"),
            str(owner.get("run_id") or ""),
        )

    candidates.sort(key=owner_order)
    earliest_key = owner_order(candidates[0])[:2]
    if sum(owner_order(item)[:2] == earliest_key for item in candidates) != 1:
        raise ObservationError("earliest scheduled DAILY owner is ambiguous")
    item = candidates[0]
    owner = item["owner"]
    owner_logical = _instant(owner.get("logical_date"), label="owner logical date")
    if (
        owner.get("dag_id") != OWNER_DAG_ID
        or owner.get("state") != "success"
        or owner.get("run_id") != _scheduled_run_id(owner_logical)
    ):
        raise ObservationError("earliest scheduled DAILY owner identity is invalid")

    if (
        _one(item.get("attest_task_states"), label="owner attestation task")
        != "success"
    ):
        raise ObservationError("owner attestation task did not succeed")
    attest_started = _instant(
        _one(item.get("attest_task_start_dates"), label="owner attestation start"),
        label="owner attestation start",
    )
    if attest_started < activation:
        raise ObservationError("owner attestation predates automatic activation")
    attestation = _one(item.get("attestations"), label="owner attestation XCom")
    if not isinstance(attestation, Mapping) or any(
        attestation.get(key) != value for key, value in identity.items()
    ):
        raise ObservationError("owner attestation XCom differs from deployment")

    decision = _one(item.get("decisions"), label="owner decision XCom")
    if not isinstance(decision, Mapping) or decision.get("lane") != "daily":
        raise ObservationError("owner decision is not exact DAILY evidence")
    decision_conf = decision.get("conf")
    if not isinstance(decision_conf, Mapping) or decision_conf.get("mode") != "daily":
        raise ObservationError("daily owner child conf is incomplete")
    try:
        selected_date = date.fromisoformat(str(decision.get("selected_date") or ""))
    except ValueError as exc:
        raise ObservationError("daily owner selected date is invalid") from exc
    if selected_date.isoformat() != decision.get("selected_date"):
        raise ObservationError("daily owner selected date is not canonical")

    initializer = _one(item.get("initializers"), label="owner initializer XCom")
    if not isinstance(initializer, Mapping):
        raise ObservationError("owner initializer XCom is malformed")
    binding = _canonical_binding(
        initializer.get("binding"), git_sha=identity["git_sha"]
    )
    generation_id = str(initializer.get("generation_id") or "")
    try:
        if str(uuid.UUID(generation_id)) != generation_id:
            raise ValueError("non-canonical UUID")
    except (AttributeError, TypeError, ValueError) as exc:
        raise ObservationError("initializer generation ID is invalid") from exc
    if generation_id != make_generation_id(binding):
        raise ObservationError("initializer generation differs from exact binding")
    if (
        _instant(binding["data_interval_end"], label="publication interval end").date()
        != selected_date
    ):
        raise ObservationError("daily binding differs from owner selected date")
    publication = {"generation_id": generation_id, "binding": binding}

    _require_no_active_tasks(item.get("active_tasks"), label="owner run")
    if _one(item.get("trigger_states"), label="owner ingest trigger") != "success":
        raise ObservationError("owner ingest trigger did not succeed")

    ingest = _one(item.get("ingest_runs"), label="ingest child run")
    ingest_run_id = f"fotmob_orchestrated__{generation_id}"
    expected_ingest_conf = {**dict(decision_conf), PUBLICATION_CONF_KEY: publication}
    if (
        not isinstance(ingest, Mapping)
        or ingest.get("dag_id") != INGEST_DAG_ID
        or ingest.get("run_id") != ingest_run_id
        or _state(ingest.get("run_type")) != "manual"
        or ingest.get("state") != "success"
        or ingest.get("conf") != expected_ingest_conf
    ):
        raise ObservationError("ingest child success/conf lineage differs")
    _require_no_active_tasks(item.get("ingest_active_tasks"), label="ingest run")
    if (
        _one(item.get("ingest_trigger_states"), label="ingest Silver trigger")
        != "success"
    ):
        raise ObservationError("ingest Silver trigger did not succeed")

    silver = _one(item.get("silver_runs"), label="Silver child run")
    silver_run_id = f"fotmob_silver__{generation_id}"
    if (
        not isinstance(silver, Mapping)
        or silver.get("dag_id") != SILVER_DAG_ID
        or silver.get("run_id") != silver_run_id
        or _state(silver.get("run_type")) != "manual"
        or silver.get("state") != "success"
        or silver.get("conf") != {PUBLICATION_CONF_KEY: publication}
    ):
        raise ObservationError("Silver child success/conf lineage differs")
    _require_no_active_tasks(item.get("silver_active_tasks"), label="Silver run")

    control = item.get("publication")
    sofa_run_id = _scheduled_run_id(binding["data_interval_start"])
    if (
        not isinstance(control, Mapping)
        or control.get("generation_id") != generation_id
        or control.get("source") != "fotmob"
        or control.get("status") != "succeeded"
        or control.get("phase") != "published"
        or control.get("binding") != binding
        or control.get("active") is not False
        or control.get("lock_active") is not False
        or control.get("published", True) is not True
        or control.get("released", True) is not True
        or control.get("consumer") != {"dag_id": SOFA_DAG_ID, "run_id": sofa_run_id}
    ):
        raise ObservationError(
            "ControlStore publication is not published, released and inactive"
        )
    return {
        "generation_id": generation_id,
        "binding": binding,
        "owner_run_id": str(owner["run_id"]),
        "ingest_run_id": ingest_run_id,
        "silver_run_id": silver_run_id,
        "sofa_run_id": sofa_run_id,
    }


def _validate_shared_snapshot(
    snapshot: Mapping[str, Any], selected: Mapping[str, Any]
) -> dict[str, Any]:
    publication = {
        "generation_id": selected["generation_id"],
        "binding": dict(selected["binding"]),
    }
    if (
        not isinstance(snapshot, Mapping)
        or snapshot.get("schema_version") != SHARED_SNAPSHOT_SCHEMA
        or snapshot.get("expected") != publication
    ):
        raise ObservationError("shared observation binding drifted")
    runs = snapshot.get("runs")
    if not isinstance(runs, list) or len(runs) != 1:
        raise ObservationError("scheduled Sofa interval is missing or ambiguous")
    sofa = runs[0]
    binding = publication["binding"]
    if not isinstance(sofa, Mapping):
        raise ObservationError("scheduled Sofa run evidence is malformed")
    if (
        sofa.get("dag_id") != SOFA_DAG_ID
        or sofa.get("run_id") != selected["sofa_run_id"]
        or _state(sofa.get("run_type")) != "scheduled"
        or sofa.get("state") != "success"
        or _canonical_instant(sofa.get("logical_date"), label="Sofa logical date")
        != binding["data_interval_start"]
        or _canonical_instant(
            sofa.get("data_interval_start"), label="Sofa interval start"
        )
        != binding["data_interval_start"]
        or _canonical_instant(sofa.get("data_interval_end"), label="Sofa interval end")
        != binding["data_interval_end"]
    ):
        raise ObservationError("Sofa run is not the exact scheduled 14:00 interval")
    _require_no_active_tasks(sofa.get("active_tasks"), label="Sofa run")
    if _one(sofa.get("finalizer_states"), label="Sofa finalizer task") != "success":
        raise ObservationError("Sofa finalizer task did not succeed")
    final = _one(sofa.get("finalizer_xcoms"), label="Sofa finalizer XCom")
    if (
        not isinstance(final, Mapping)
        or final.get("generation_id") != selected["generation_id"]
        or final.get("binding") != binding
        or final.get("status") != "succeeded"
        or final.get("phase") != "published"
        or final.get("active") is not False
        or final.get("published") is not True
        or final.get("released") is not True
    ):
        raise ObservationError("Sofa finalizer XCom is not exact published evidence")
    return dict(sofa)


def validate_observation(
    context: Mapping[str, Any],
    isolated: Mapping[str, Any],
    shared: Mapping[str, Any],
) -> dict[str, Any]:
    """Validate two metadata-plane snapshots and build the purge schema."""

    selected = _validate_isolated_snapshot(context, isolated)
    sofa = _validate_shared_snapshot(shared, selected)
    generation_id = selected["generation_id"]
    binding = dict(selected["binding"])
    identity = _deployment_identity(context)
    return {
        "schema_version": SCHEMA_VERSION,
        "passed": True,
        "deployment": identity,
        "runs": {
            "owner": {
                "dag_id": OWNER_DAG_ID,
                "run_id": selected["owner_run_id"],
                "run_type": "scheduled",
                "generation_id": generation_id,
                "state": "success",
            },
            "ingest": {
                "dag_id": INGEST_DAG_ID,
                "run_id": selected["ingest_run_id"],
                "owner_run_id": selected["owner_run_id"],
                "generation_id": generation_id,
                "state": "success",
            },
            "silver": {
                "dag_id": SILVER_DAG_ID,
                "run_id": selected["silver_run_id"],
                "ingest_run_id": selected["ingest_run_id"],
                "generation_id": generation_id,
                "state": "success",
            },
            "sofascore": {
                "dag_id": SOFA_DAG_ID,
                "run_id": str(sofa["run_id"]),
                "generation_id": generation_id,
                "state": "success",
            },
            "finalizer": {
                "dag_id": SOFA_DAG_ID,
                "run_id": str(sofa["run_id"]),
                "task_id": SOFA_FINALIZER_TASK_ID,
                "generation_id": generation_id,
                "state": "success",
            },
        },
        "publication": {
            "generation_id": generation_id,
            "binding": binding,
            "status": "succeeded",
            "phase": "published",
            "active": False,
            "published": True,
            "released": True,
        },
    }


def _same_deployment(before: Mapping[str, Any], after: Mapping[str, Any]) -> bool:
    keys = (
        "deployment_id",
        "git_sha",
        "scheduler_container_id",
        "metadb_container_id",
        "resolved_image_id",
        "resolved_postgres_image_id",
        "evidence_dir",
        "automatic_rollout_summary",
        "paused",
        "unpaused",
    )
    before_handoff = before.get("shared_handoff_final")
    after_handoff = after.get("shared_handoff_final")
    return all(before.get(key) == after.get(key) for key in keys) and (
        isinstance(before_handoff, Mapping)
        and isinstance(after_handoff, Mapping)
        and before_handoff.get("shared_scheduler_container")
        == after_handoff.get("shared_scheduler_container")
        and before_handoff.get("runtime_code_sha256")
        == after_handoff.get("runtime_code_sha256")
        and before_handoff.get("control_database")
        == after_handoff.get("control_database")
    )


def collect_scheduled_observation(
    args: argparse.Namespace,
    *,
    run: Callable[..., subprocess.CompletedProcess[str]] = subprocess.run,
) -> dict[str, Any]:
    """Collect the same first successful scheduled lineage twice."""

    context_before, deployment_digest_before = load_active_deployment(args)
    if hasattr(args, "output"):
        validate_output_path(context_before, args.output)
    live_before = validate_live_runtimes(args, context_before, run=run)

    isolated_before = collect_isolated_snapshot(args, context_before, run=run)
    selected_before = _validate_isolated_snapshot(context_before, isolated_before)
    shared_before = collect_shared_snapshot(
        context_before,
        selected_before["generation_id"],
        selected_before["binding"],
        run=run,
    )
    report_before = validate_observation(context_before, isolated_before, shared_before)

    second_error: ObservationError | None = None
    report_after: dict[str, Any] | None = None
    try:
        isolated_after = collect_isolated_snapshot(args, context_before, run=run)
        selected_after = _validate_isolated_snapshot(context_before, isolated_after)
        shared_after = collect_shared_snapshot(
            context_before,
            selected_after["generation_id"],
            selected_after["binding"],
            run=run,
        )
        report_after = validate_observation(
            context_before, isolated_after, shared_after
        )
    except ObservationError as exc:
        second_error = exc

    context_after, deployment_digest_after = load_active_deployment(args)
    live_after = validate_live_runtimes(args, context_after, run=run)
    if second_error is not None:
        raise ObservationError(
            "scheduled observation drifted during repeated read-only collection"
        ) from second_error
    if (
        deployment_digest_before != deployment_digest_after
        or not _same_deployment(context_before, context_after)
        or live_before != live_after
        or report_after != report_before
    ):
        raise ObservationError(
            "deployment, runtime, control DB, or scheduled lineage drifted"
        )
    return report_before


def _parser(argv: Sequence[str] | None = None) -> argparse.Namespace:
    repository = Path(__file__).resolve().parents[1]
    parser = argparse.ArgumentParser(
        description="Collect the first protected automatic FotMob scheduled run"
    )
    parser.add_argument("--project", default="fotmob-airflow")
    parser.add_argument(
        "--compose-file",
        type=Path,
        default=repository / "deploy/fotmob/airflow.compose.yaml",
    )
    parser.add_argument("--env-file", type=Path, required=True)
    parser.add_argument("--deployment-report", type=Path, required=True)
    parser.add_argument("--output", type=Path, required=True)
    return parser.parse_args(argv)


def main(argv: Sequence[str] | None = None) -> int:
    args = _parser(argv)
    try:
        report = collect_scheduled_observation(args)
        final_context, _digest = load_active_deployment(args)
        if _deployment_identity(final_context) != report.get("deployment"):
            raise ObservationError("deployment changed before protected output write")
        write_protected_observation(final_context, args.output, report)
    except (ObservationError, OSError, subprocess.CalledProcessError) as exc:
        print(f"fotmob scheduled observation refused: {exc}", file=sys.stderr)
        return 2
    print(json.dumps(report, ensure_ascii=False, indent=2))
    return 0


if __name__ == "__main__":  # pragma: no cover
    raise SystemExit(main())
