#!/usr/bin/env python3
"""Versioned, fail-closed ESPN release deployment operator.

``plan`` is deliberately pure: it reads the reviewed release inputs and emits
one canonical JSON document to stdout.  It does not create a directory, take a
lock, run a guard, make a backup, or invoke Compose.  ``apply`` and ``resume``
are the only mutating modes and accept only that exact canonical document plus
its SHA-256.

The operator intentionally executes commands without a shell.  Every child is
placed in a new process group, its output is sent directly to an operator-owned
regular file, and its lifetime is bounded by both an operation deadline and the
three-hour transition deadline.  Console output is advisory and EPIPE-safe;
durable files are the correctness boundary.
"""

from __future__ import annotations

import argparse
from contextlib import contextmanager
from dataclasses import dataclass
from datetime import datetime, timezone
import fcntl
import hashlib
import json
import os
from pathlib import Path
import re
import signal
import stat
import subprocess
import sys
import tempfile
import time
from typing import Callable, Iterator, Mapping, Sequence, TextIO


if sys.flags.optimize != 0:
    raise RuntimeError("optimized Python is forbidden for the ESPN deploy operator")


PLAN_VERSION = "espn-release-deploy-plan-v1"
TRANSITION_JOURNAL_VERSION = "espn-release-deploy-transition-v1"
GUARD_JOURNAL_VERSION = "espn-release-deploy-guard-attempts-v1"
CHECKPOINT_VERSION = "espn-release-deploy-checkpoint-v1"
HEARTBEAT_VERSION = "espn-release-deploy-heartbeat-v1"
RESULT_VERSION = "espn-release-deploy-result-v1"
FINGERPRINT_VERSION = "espn-release-deploy-physical-fingerprint-v1"

GUARD_TIMEOUT_SECONDS = 1_800
GUARD_PROCESS_TIMEOUT_SECONDS = 1_740
TOTAL_TIMEOUT_SECONDS = 10_800
HEARTBEAT_INTERVAL_SECONDS = 60
TERMINATION_GRACE_SECONDS = 10

GUARD_PHASES = (
    "initial_state",
    "pre_backup",
    "pre_checkpoint_mutation",
    "pre_airflow_init",
    "pre_recreate",
    "post_deploy",
)
GUARD_REPORT_KIND = "espn-release-guard-v1"
GUARD_REPORT_SCHEMA_VERSION = 1
GUARD_PYTHON_PATH = "/root/.venvs/dpf-test/bin/python"
GUARD_SCRIPT_RELATIVE_PATH = Path("scripts/espn_release_guard_v1.py")
GUARD_DOCKER_PATH = "/usr/bin/docker"
GUARD_POLL_SECONDS = "15"
GUARD_MAX_WAIT_SECONDS = "1740"
GUARD_SUCCESS_CHECKS = (
    "exact_dag_inventory",
    "all_dags_paused",
    "zero_active_dagruns",
    "transaction_read_only",
)
FINGERPRINT_CONTAINERS = (
    "espn-airflow-airflow-metadb-1",
    "espn-airflow-airflow-scheduler-1",
    "espn-airflow-airflow-webserver-1",
)
RELEASE_TREES = (
    ("dags", Path("dags")),
    ("scrapers", Path("scrapers")),
    ("scripts", Path("scripts")),
    ("config-espn", Path("configs/espn")),
    ("config-medallion", Path("configs/medallion")),
)

_DIGEST = re.compile(r"[0-9a-f]{64}")
_COMMIT = re.compile(r"[0-9a-f]{40}")
_TRANSITION_ID = re.compile(r"[a-z0-9][a-z0-9._-]{2,95}")
_DIGEST_IMAGE = re.compile(r"[^\s@]+@sha256:[0-9a-f]{64}")
_EVENT_STATUSES = {"started", "succeeded", "failed"}


class DeployError(RuntimeError):
    """A fail-closed deploy contract violation."""


def canonical_bytes(value: object) -> bytes:
    """Return the one accepted canonical JSON representation."""

    try:
        text = json.dumps(
            value,
            ensure_ascii=False,
            sort_keys=True,
            separators=(",", ":"),
            allow_nan=False,
        )
    except (TypeError, ValueError) as exc:
        raise DeployError("value cannot be encoded as canonical JSON") from exc
    return (text + "\n").encode("utf-8")


def canonical_sha256(value: object) -> str:
    return hashlib.sha256(canonical_bytes(value)).hexdigest()


def _file_sha256(path: Path) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as handle:
        for block in iter(lambda: handle.read(1024 * 1024), b""):
            digest.update(block)
    return digest.hexdigest()


def _require_digest(value: object, label: str) -> str:
    if not isinstance(value, str) or _DIGEST.fullmatch(value) is None:
        raise DeployError(f"{label} must be a lowercase SHA-256")
    return value


def _require_absolute_path(value: object, label: str) -> Path:
    if not isinstance(value, str) or not value:
        raise DeployError(f"{label} must be a non-empty absolute path")
    path = Path(value)
    if not path.is_absolute() or ".." in path.parts:
        raise DeployError(f"{label} must be an absolute normalized path")
    if path == Path("/") or len(path.parts) < 3:
        raise DeployError(f"{label} is too broad for a deploy target")
    return path


def _require_readable_regular(path: Path, label: str) -> None:
    try:
        details = path.lstat()
    except OSError as exc:
        raise DeployError(f"{label} is missing: {path}") from exc
    if not stat.S_ISREG(details.st_mode) or path.is_symlink():
        raise DeployError(f"{label} must be a regular non-symlink file: {path}")


def release_tree_sha256(release_root: Path) -> str:
    """Hash the exact runtime projection used by ESPN release identity."""

    root = Path(release_root)
    if not root.is_dir() or root.is_symlink():
        raise DeployError(f"release root is missing or unsafe: {root}")
    digest = hashlib.sha256()
    for label, relative_root in RELEASE_TREES:
        directory = root / relative_root
        if not directory.is_dir() or directory.is_symlink():
            raise DeployError(f"release tree is missing or unsafe: {directory}")
        entries = sorted(directory.rglob("*"), key=lambda item: item.as_posix())
        if any(path.is_symlink() for path in entries):
            raise DeployError(f"release tree contains a symlink: {directory}")
        files = [
            path
            for path in entries
            if path.is_file()
            and "__pycache__" not in path.parts
            and path.suffix not in {".pyc", ".pyo"}
        ]
        if not files:
            raise DeployError(f"release tree is empty: {directory}")
        for path in files:
            relative = f"{label}/{path.relative_to(directory).as_posix()}".encode(
                "utf-8"
            )
            payload = path.read_bytes()
            digest.update(len(relative).to_bytes(8, "big"))
            digest.update(relative)
            digest.update(len(payload).to_bytes(8, "big"))
            digest.update(payload)
    return digest.hexdigest()


def dagbag_sha256(dagbag_root: Path) -> str:
    root = Path(dagbag_root)
    if not root.is_dir() or root.is_symlink():
        raise DeployError(f"DagBag root is missing or unsafe: {root}")
    digest = hashlib.sha256()
    entries = sorted(root.rglob("*"), key=lambda item: item.as_posix())
    if not entries:
        raise DeployError("DagBag projection is empty")
    for path in entries:
        relative = path.relative_to(root).as_posix().encode("utf-8")
        if path.is_symlink():
            kind = b"symlink"
            payload = os.readlink(path).encode("utf-8")
        elif path.is_file():
            kind = b"file"
            payload = path.read_bytes()
        elif path.is_dir():
            continue
        else:
            raise DeployError(f"DagBag contains an unsupported entry: {path}")
        for value in (kind, relative, payload):
            digest.update(len(value).to_bytes(8, "big"))
            digest.update(value)
    return digest.hexdigest()


def _guard_artifacts(values: object) -> list[dict[str, str]]:
    if not isinstance(values, list) or not values:
        raise DeployError("guard_artifacts must contain at least one reviewed file")
    result: list[dict[str, str]] = []
    seen: set[str] = set()
    for raw in values:
        path = _require_absolute_path(raw, "guard artifact")
        _require_readable_regular(path, "guard artifact")
        rendered = str(path)
        if rendered in seen:
            raise DeployError(f"duplicate guard artifact: {rendered}")
        seen.add(rendered)
        result.append({"path": rendered, "sha256": _file_sha256(path)})
    return result


def _executable_identity(value: object) -> dict[str, object]:
    path = _require_absolute_path(value, "guard executable")
    try:
        details = path.lstat()
        resolved = path.resolve(strict=True)
        resolved_details = resolved.stat()
    except OSError as exc:
        raise DeployError(f"guard executable is missing: {path}") from exc
    if not stat.S_ISREG(resolved_details.st_mode) or not os.access(resolved, os.X_OK):
        raise DeployError("guard executable must resolve to an executable regular file")
    if path.is_symlink():
        symlink_target: str | None = os.readlink(path)
    elif stat.S_ISREG(details.st_mode):
        symlink_target = None
    else:
        raise DeployError("guard executable must be a regular file or one symlink")
    return {
        "path": str(path),
        "resolved_path": str(resolved),
        "symlink_target": symlink_target,
        "sha256": _file_sha256(resolved),
    }


def build_plan(spec: Mapping[str, object]) -> dict[str, object]:
    """Build a deterministic plan without crossing any mutating boundary."""

    required = {
        "transition_id",
        "release_commit",
        "release_tree_sha256",
        "release_root",
        "dagbag_root",
        "compose_file",
        "env_file",
        "stack_lock_root",
        "state_root",
        "backup_root",
        "airflow_image",
        "postgres_image",
        "layout_mode",
        "guard_argv",
        "guard_artifacts",
    }
    if set(spec) != required:
        raise DeployError(
            "plan specification fields drifted: "
            f"missing={sorted(required - set(spec))}, extra={sorted(set(spec) - required)}"
        )
    transition_id = spec["transition_id"]
    if (
        not isinstance(transition_id, str)
        or _TRANSITION_ID.fullmatch(transition_id) is None
    ):
        raise DeployError("transition_id must be a safe 3-96 character identifier")
    release_commit = spec["release_commit"]
    if not isinstance(release_commit, str) or _COMMIT.fullmatch(release_commit) is None:
        raise DeployError("release_commit must be a lowercase full Git commit")
    expected_tree = _require_digest(spec["release_tree_sha256"], "release_tree_sha256")
    release_root = _require_absolute_path(spec["release_root"], "release_root")
    observed_tree = release_tree_sha256(release_root)
    if observed_tree != expected_tree:
        raise DeployError("reviewed release tree SHA-256 does not match release_root")
    dagbag_root = _require_absolute_path(spec["dagbag_root"], "dagbag_root")
    observed_dagbag = dagbag_sha256(dagbag_root)
    compose_file = _require_absolute_path(spec["compose_file"], "compose_file")
    _require_readable_regular(compose_file, "Compose file")
    env_file = _require_absolute_path(spec["env_file"], "env_file")
    # The plan records but never reads the protected environment file.  Its
    # type, owner, and mode are enforced by apply/resume.
    stack_lock_root = _require_absolute_path(spec["stack_lock_root"], "stack_lock_root")
    state_root = _require_absolute_path(spec["state_root"], "state_root")
    backup_root = _require_absolute_path(spec["backup_root"], "backup_root")
    if (
        stack_lock_root == state_root
        or stack_lock_root in state_root.parents
        or state_root in stack_lock_root.parents
    ):
        raise DeployError("stack_lock_root must be separate from transition state")
    if (
        state_root == backup_root
        or state_root in backup_root.parents
        or backup_root in state_root.parents
    ):
        raise DeployError("state_root and backup_root must be separate durable trees")
    airflow_image = spec["airflow_image"]
    postgres_image = spec["postgres_image"]
    if (
        not isinstance(airflow_image, str)
        or _DIGEST_IMAGE.fullmatch(airflow_image) is None
    ):
        raise DeployError("airflow_image must be pinned by sha256 digest")
    if (
        not isinstance(postgres_image, str)
        or _DIGEST_IMAGE.fullmatch(postgres_image) is None
    ):
        raise DeployError("postgres_image must be pinned by sha256 digest")
    layout_mode = spec["layout_mode"]
    if layout_mode not in {"legacy14", "compact6"}:
        raise DeployError("layout_mode must be legacy14 or compact6")
    guard_argv = spec["guard_argv"]
    if (
        not isinstance(guard_argv, list)
        or not guard_argv
        or any(not isinstance(value, str) or not value for value in guard_argv)
    ):
        raise DeployError("guard_argv must be a non-empty JSON argv array")
    reviewed_guards = _guard_artifacts(spec["guard_artifacts"])
    guard_script = release_root / GUARD_SCRIPT_RELATIVE_PATH
    expected_guard_argv = [
        GUARD_PYTHON_PATH,
        "-B",
        str(guard_script),
        "guard",
        "--docker-path",
        GUARD_DOCKER_PATH,
        "--poll-seconds",
        GUARD_POLL_SECONDS,
        "--max-wait-seconds",
        GUARD_MAX_WAIT_SECONDS,
    ]
    if guard_argv != expected_guard_argv:
        raise DeployError("guard_argv must be the exact versioned release guard argv")
    executable = _executable_identity(guard_argv[0])
    artifact_paths = {item["path"] for item in reviewed_guards}
    required_guard_artifacts = {str(guard_script), GUARD_DOCKER_PATH}
    if not required_guard_artifacts.issubset(artifact_paths):
        raise DeployError(
            "guard_artifacts must include the required artifact set: "
            "versioned guard script and Docker executable"
        )
    if not any(value in artifact_paths for value in guard_argv[1:]):
        raise DeployError("guard argv must directly name at least one hashed artifact")
    for value in guard_argv[1:]:
        candidate = Path(value)
        if candidate.is_absolute() and (candidate.exists() or candidate.is_symlink()):
            normalized = str(_require_absolute_path(value, "guard argv file"))
            if normalized not in artifact_paths:
                raise DeployError(
                    f"guard argv file is not bound to guard_artifacts: {normalized}"
                )

    operator_path = Path(__file__).resolve()
    _require_readable_regular(operator_path, "release deploy operator")

    compose_prefix = [
        "docker",
        "compose",
        "--env-file",
        str(env_file),
        "--project-name",
        "espn-airflow",
        "-f",
        str(compose_file),
        "--profile",
        "ui",
    ]
    restore_ownership = canonical_sha256(
        {
            "transition_id": transition_id,
            "release_commit": release_commit,
            "release_tree_sha256": expected_tree,
            "state_root": str(state_root),
            "backup_root": str(backup_root),
            "postgres_image": postgres_image,
        }
    )
    restore_suffix = restore_ownership[:20]
    backup_path = backup_root / f"airflow-before-{transition_id}.dump"
    toc_path = backup_root / f"airflow-before-{transition_id}.dump.toc"
    plan: dict[str, object] = {
        "kind": PLAN_VERSION,
        "mutates": False,
        "transition_id": transition_id,
        "release_commit": release_commit,
        "operator_path": str(operator_path),
        "operator_sha256": _file_sha256(operator_path),
        "release_tree_sha256": expected_tree,
        "release_root": str(release_root),
        "dagbag_root": str(dagbag_root),
        "dagbag_sha256": observed_dagbag,
        "compose_file": str(compose_file),
        "compose_sha256": _file_sha256(compose_file),
        "env_file": str(env_file),
        "stack_lock_root": str(stack_lock_root),
        "state_root": str(state_root),
        "backup_root": str(backup_root),
        "backup_path": str(backup_path),
        "backup_toc_path": str(toc_path),
        "layout_mode": layout_mode,
        "airflow_image": airflow_image,
        "postgres_image": postgres_image,
        "guard": {
            "argv": list(guard_argv),
            "artifacts": reviewed_guards,
            "executable": executable,
        },
        "guard_phases": list(GUARD_PHASES),
        "fingerprint_containers": list(FINGERPRINT_CONTAINERS),
        "commands": {
            "compose_config": [*compose_prefix, "config", "--quiet"],
            "backup": [
                "docker",
                "exec",
                FINGERPRINT_CONTAINERS[0],
                "pg_dump",
                "--username=airflow",
                "--dbname=airflow",
                "--format=custom",
                "--compress=9",
                "--serializable-deferrable",
                "--no-owner",
                "--no-privileges",
            ],
            "archive_list": [
                "docker",
                "exec",
                "-i",
                FINGERPRINT_CONTAINERS[0],
                "pg_restore",
                "--list",
            ],
            "airflow_init": [
                *compose_prefix,
                "up",
                "--no-deps",
                "--force-recreate",
                "--abort-on-container-exit",
                "--exit-code-from",
                "airflow-init",
                "airflow-init",
            ],
            "recreate": [
                *compose_prefix,
                "up",
                "-d",
                "--no-deps",
                "--wait",
                "--wait-timeout",
                "180",
                "--force-recreate",
                "airflow-scheduler",
                "airflow-webserver",
            ],
        },
        "restore_drill": {
            "container": f"espn-deploy-restore-{restore_suffix}",
            "image": postgres_image,
            "network": "none",
            "tmpfs": "rw,nosuid,nodev,size=536870912",
            "ownership_label": restore_ownership,
        },
        "limits": {
            "guard_timeout_seconds": GUARD_TIMEOUT_SECONDS,
            "heartbeat_interval_seconds": HEARTBEAT_INTERVAL_SECONDS,
            "total_timeout_seconds": TOTAL_TIMEOUT_SECONDS,
        },
    }
    plan["plan_sha256"] = canonical_sha256(plan)
    return plan


def _validate_plan_document(plan: object) -> dict[str, object]:
    if not isinstance(plan, dict):
        raise DeployError("reviewed deploy plan must be a JSON object")
    expected_top = {
        "kind",
        "mutates",
        "transition_id",
        "release_commit",
        "operator_path",
        "operator_sha256",
        "release_tree_sha256",
        "release_root",
        "dagbag_root",
        "dagbag_sha256",
        "compose_file",
        "compose_sha256",
        "env_file",
        "stack_lock_root",
        "state_root",
        "backup_root",
        "backup_path",
        "backup_toc_path",
        "layout_mode",
        "airflow_image",
        "postgres_image",
        "guard",
        "guard_phases",
        "fingerprint_containers",
        "commands",
        "restore_drill",
        "limits",
        "plan_sha256",
    }
    if set(plan) != expected_top:
        raise DeployError("reviewed deploy plan fields drifted")
    if plan.get("kind") != PLAN_VERSION or plan.get("mutates") is not False:
        raise DeployError("reviewed deploy plan version/mutation marker drifted")
    transition_id = plan.get("transition_id")
    if (
        not isinstance(transition_id, str)
        or _TRANSITION_ID.fullmatch(transition_id) is None
    ):
        raise DeployError("reviewed transition identity is malformed")
    release_commit = plan.get("release_commit")
    if not isinstance(release_commit, str) or _COMMIT.fullmatch(release_commit) is None:
        raise DeployError("reviewed release commit is malformed")
    for key in (
        "release_tree_sha256",
        "dagbag_sha256",
        "compose_sha256",
        "operator_sha256",
        "plan_sha256",
    ):
        _require_digest(plan.get(key), key)
    if plan.get("layout_mode") not in {"legacy14", "compact6"}:
        raise DeployError("reviewed layout mode is unknown")
    if plan.get("guard_phases") != list(GUARD_PHASES):
        raise DeployError("reviewed six-phase guard order drifted")
    if plan.get("fingerprint_containers") != list(FINGERPRINT_CONTAINERS):
        raise DeployError("reviewed physical fingerprint target set drifted")
    if plan.get("limits") != {
        "guard_timeout_seconds": GUARD_TIMEOUT_SECONDS,
        "heartbeat_interval_seconds": HEARTBEAT_INTERVAL_SECONDS,
        "total_timeout_seconds": TOTAL_TIMEOUT_SECONDS,
    }:
        raise DeployError("reviewed timeout/heartbeat limits drifted")
    unsigned = {key: value for key, value in plan.items() if key != "plan_sha256"}
    if canonical_sha256(unsigned) != plan["plan_sha256"]:
        raise DeployError("reviewed plan SHA-256 is invalid")
    return plan


def load_reviewed_plan(path: Path, expected_plan_sha256: str) -> dict[str, object]:
    expected = _require_digest(expected_plan_sha256, "requested plan SHA-256")
    candidate = Path(path)
    _require_readable_regular(candidate, "reviewed plan")
    payload = candidate.read_bytes()
    try:
        decoded = json.loads(payload)
    except (UnicodeDecodeError, json.JSONDecodeError) as exc:
        raise DeployError("reviewed plan JSON is invalid") from exc
    plan = _validate_plan_document(decoded)
    if payload != canonical_bytes(plan):
        raise DeployError("reviewed plan must use exact canonical JSON bytes")
    if plan["plan_sha256"] != expected:
        raise DeployError("requested plan SHA-256 does not match the reviewed plan")
    return plan


def _event_sha(body: Mapping[str, object]) -> str:
    return canonical_sha256(body)


def _seal_transition(value: dict[str, object]) -> dict[str, object]:
    result = json.loads(json.dumps(value))
    result.pop("journal_sha256", None)
    result["journal_sha256"] = canonical_sha256(result)
    return result


def _seal_guards(value: dict[str, object]) -> dict[str, object]:
    result = json.loads(json.dumps(value))
    result.pop("journal_sha256", None)
    result["journal_sha256"] = canonical_sha256(result)
    return result


def new_transition_journal(
    plan: Mapping[str, object], recorded_at: str
) -> dict[str, object]:
    body = {
        "sequence": 1,
        "event": "transition_started",
        "phase": None,
        "recorded_at": recorded_at,
        "previous_event_sha256": None,
        "guard_event_sha256": None,
        "detail": {},
    }
    body["event_sha256"] = _event_sha(body)
    return _seal_transition(
        {
            "kind": TRANSITION_JOURNAL_VERSION,
            "transition_id": plan["transition_id"],
            "plan_sha256": plan["plan_sha256"],
            "budget_started_at": recorded_at,
            "guard_attempt_journal_sha256": None,
            "events": [body],
        }
    )


def new_guard_journal(
    plan: Mapping[str, object], *, budget_started_at: str | None = None
) -> dict[str, object]:
    return _seal_guards(
        {
            "kind": GUARD_JOURNAL_VERSION,
            "transition_id": plan["transition_id"],
            "plan_sha256": plan["plan_sha256"],
            "budget_started_at": budget_started_at,
            "events": [],
        }
    )


def reseal_transition_journal(journal: Mapping[str, object]) -> dict[str, object]:
    return _seal_transition(dict(journal))


def transition_tail_sha256(journal: Mapping[str, object]) -> str:
    events = journal.get("events")
    if not isinstance(events, list) or not events or not isinstance(events[-1], dict):
        raise DeployError("transition journal has no valid tail")
    return _require_digest(events[-1].get("event_sha256"), "transition tail")


def append_transition_event(
    journal: Mapping[str, object],
    *,
    event: str,
    phase: str | None,
    recorded_at: str,
    guard_event_sha256: str | None = None,
    detail: Mapping[str, object] | None = None,
) -> dict[str, object]:
    result = _validated_transition_journal(journal)
    events = list(result["events"])
    if guard_event_sha256 is not None:
        _require_digest(guard_event_sha256, "guard event SHA-256")
    body: dict[str, object] = {
        "sequence": len(events) + 1,
        "event": event,
        "phase": phase,
        "recorded_at": recorded_at,
        "previous_event_sha256": transition_tail_sha256(result),
        "guard_event_sha256": guard_event_sha256,
        "detail": dict(detail or {}),
    }
    body["event_sha256"] = _event_sha(body)
    events.append(body)
    result["events"] = events
    return _seal_transition(result)


def _validated_transition_journal(
    journal: Mapping[str, object],
) -> dict[str, object]:
    value = json.loads(json.dumps(journal))
    checksum = value.pop("journal_sha256", None)
    if not isinstance(checksum, str) or checksum != canonical_sha256(value):
        raise DeployError("transition journal checksum drifted")
    if value.get("kind") != TRANSITION_JOURNAL_VERSION:
        raise DeployError("transition journal version drifted")
    expected_top = {
        "kind",
        "transition_id",
        "plan_sha256",
        "budget_started_at",
        "guard_attempt_journal_sha256",
        "events",
    }
    if set(value) != expected_top:
        raise DeployError("transition journal fields drifted")
    transition_id = value.get("transition_id")
    if (
        not isinstance(transition_id, str)
        or _TRANSITION_ID.fullmatch(transition_id) is None
    ):
        raise DeployError("transition journal identity is malformed")
    _require_digest(value.get("plan_sha256"), "transition journal plan SHA-256")
    _parse_utc(value.get("budget_started_at"))
    guard_pointer = value.get("guard_attempt_journal_sha256")
    if guard_pointer is not None:
        _require_digest(guard_pointer, "transition guard journal pointer")
    events = value.get("events")
    if not isinstance(events, list) or not events:
        raise DeployError("transition journal event list is malformed")
    previous = None
    allowed_events = {
        "transition_started",
        "guard_started",
        "guard_succeeded",
        "guard_failed",
        "action_started",
        "action_succeeded",
        "action_failed",
        "checkpoint_sealed",
        "deployed",
    }
    expected_event_fields = {
        "sequence",
        "event",
        "phase",
        "recorded_at",
        "previous_event_sha256",
        "guard_event_sha256",
        "detail",
        "event_sha256",
    }
    for index, raw in enumerate(events, start=1):
        if not isinstance(raw, dict) or set(raw) != expected_event_fields:
            raise DeployError("transition journal event is malformed")
        event = dict(raw)
        digest = event.pop("event_sha256", None)
        event_name = event.get("event")
        phase = event.get("phase")
        guard_sha = event.get("guard_event_sha256")
        if (
            event.get("sequence") != index
            or event.get("previous_event_sha256") != previous
            or not isinstance(digest, str)
            or digest != _event_sha(event)
            or event_name not in allowed_events
            or not isinstance(event.get("detail"), dict)
        ):
            raise DeployError("transition journal event chain drifted")
        _parse_utc(event.get("recorded_at"))
        if str(event_name).startswith("guard_"):
            if phase not in GUARD_PHASES:
                raise DeployError("transition guard event phase is malformed")
            _require_digest(guard_sha, "transition guard event SHA-256")
        elif guard_sha is not None:
            raise DeployError("non-guard transition event has a guard pointer")
        elif event_name == "checkpoint_sealed":
            if phase != "pre_checkpoint_mutation":
                raise DeployError("checkpoint transition phase is malformed")
        elif phase is not None:
            raise DeployError("non-guard transition event has an unexpected phase")
        if index == 1 and (
            event_name != "transition_started"
            or event.get("detail") != {}
            or event.get("previous_event_sha256") is not None
            or guard_sha is not None
        ):
            raise DeployError("transition journal does not start at its origin")
        previous = digest
    value["journal_sha256"] = checksum
    return value


def _validated_guard_journal(journal: Mapping[str, object]) -> dict[str, object]:
    value = json.loads(json.dumps(journal))
    checksum = value.pop("journal_sha256", None)
    if not isinstance(checksum, str) or checksum != canonical_sha256(value):
        raise DeployError("guard-attempt journal checksum drifted")
    if value.get("kind") != GUARD_JOURNAL_VERSION:
        raise DeployError("guard-attempt journal version drifted")
    if set(value) != {
        "kind",
        "transition_id",
        "plan_sha256",
        "budget_started_at",
        "events",
    }:
        raise DeployError("guard-attempt journal fields drifted")
    transition_id = value.get("transition_id")
    if (
        not isinstance(transition_id, str)
        or _TRANSITION_ID.fullmatch(transition_id) is None
    ):
        raise DeployError("guard-attempt journal identity is malformed")
    _require_digest(value.get("plan_sha256"), "guard journal plan SHA-256")
    if value.get("budget_started_at") is not None:
        _parse_utc(value["budget_started_at"])
    events = value.get("events")
    if not isinstance(events, list):
        raise DeployError("guard-attempt event list is malformed")
    previous = None
    attempts: dict[tuple[str, int], list[str]] = {}
    succeeded_phases: set[str] = set()
    introduced_phases: list[str] = []
    prefix_events: list[object] = []
    expected_event_fields = {
        "sequence",
        "phase",
        "status",
        "attempt",
        "duration_seconds",
        "physical_fingerprint",
        "physical_fingerprint_path",
        "recorded_at",
        "log_path",
        "log_sha256",
        "transition_event_sha256",
        "previous_event_sha256",
        "journal_before_sha256",
        "event_sha256",
    }
    for index, raw in enumerate(events, start=1):
        if not isinstance(raw, dict) or set(raw) != expected_event_fields:
            raise DeployError("guard-attempt event is malformed")
        event = dict(raw)
        digest = event.pop("event_sha256", None)
        phase = event.get("phase")
        status_name = event.get("status")
        attempt = event.get("attempt")
        if (
            event.get("sequence") != index
            or event.get("previous_event_sha256") != previous
            or not isinstance(digest, str)
            or digest != _event_sha(event)
            or phase not in GUARD_PHASES
            or status_name not in _EVENT_STATUSES
            or not isinstance(attempt, int)
            or isinstance(attempt, bool)
            or attempt <= 0
        ):
            raise DeployError("guard-attempt event chain drifted")
        expected_before = _seal_guards(
            {
                "kind": value.get("kind"),
                "transition_id": value.get("transition_id"),
                "plan_sha256": value.get("plan_sha256"),
                "budget_started_at": value.get("budget_started_at"),
                "events": prefix_events,
            }
        )["journal_sha256"]
        if event.get("journal_before_sha256") != expected_before:
            raise DeployError("guard-attempt journal prefix binding drifted")
        _require_digest(
            event.get("transition_event_sha256"), "guard transition binding"
        )
        _parse_utc(event.get("recorded_at"))
        _require_digest(event.get("physical_fingerprint"), "physical fingerprint")
        fingerprint_path = event.get("physical_fingerprint_path")
        if (
            not isinstance(fingerprint_path, str)
            or not Path(fingerprint_path).is_absolute()
        ):
            raise DeployError("guard physical fingerprint path is malformed")
        log_path = event.get("log_path")
        if not isinstance(log_path, str) or not Path(log_path).is_absolute():
            raise DeployError("guard process log path is malformed")
        if status_name == "started":
            if event.get("log_sha256") is not None:
                raise DeployError("started guard event has a terminal log SHA")
        else:
            _require_digest(event.get("log_sha256"), "guard process log SHA-256")
        duration = event.get("duration_seconds")
        if (
            not isinstance(duration, (int, float))
            or isinstance(duration, bool)
            or duration < 0
        ):
            raise DeployError("guard-attempt duration is invalid")
        key = (str(phase), attempt)
        statuses = attempts.setdefault(key, [])
        if status_name == "started" and str(phase) not in introduced_phases:
            expected_phase = GUARD_PHASES[len(introduced_phases)]
            if phase != expected_phase:
                raise DeployError(
                    f"guard phases are out of order: expected {expected_phase}, got {phase}"
                )
            if introduced_phases and introduced_phases[-1] not in succeeded_phases:
                raise DeployError("guard phase advanced before predecessor succeeded")
            introduced_phases.append(str(phase))
        statuses.append(str(status_name))
        if statuses[0] != "started" or len(statuses) > 2:
            raise DeployError(
                "guard attempt does not begin with exactly one started event"
            )
        if len(statuses) == 2 and statuses[1] not in {"succeeded", "failed"}:
            raise DeployError("guard attempt terminal state is malformed")
        if status_name == "succeeded":
            succeeded_phases.add(str(phase))
        previous = digest
        prefix_events.append(raw)
    for phase in succeeded_phases:
        success_index = next(
            index
            for index, item in enumerate(events)
            if item["phase"] == phase and item["status"] == "succeeded"
        )
        if any(item["phase"] == phase for item in events[success_index + 1 :]):
            raise DeployError("completed guard phase identity was extended")
    value["journal_sha256"] = checksum
    return value


def append_guard_event(
    journal: Mapping[str, object],
    *,
    transition_event_sha256: str,
    phase: str,
    status: str,
    attempt: int,
    duration_seconds: float,
    physical_fingerprint: str,
    physical_fingerprint_path: str,
    recorded_at: str,
    log_path: str,
    log_sha256: str | None,
) -> tuple[dict[str, object], dict[str, object]]:
    result = _validated_guard_journal(journal)
    if phase not in GUARD_PHASES or status not in _EVENT_STATUSES:
        raise DeployError("unsupported guard phase or status")
    _require_digest(transition_event_sha256, "guard transition binding")
    _require_digest(physical_fingerprint, "physical fingerprint")
    if (
        not isinstance(physical_fingerprint_path, str)
        or not Path(physical_fingerprint_path).is_absolute()
    ):
        raise DeployError("physical fingerprint evidence path must be absolute")
    if not isinstance(attempt, int) or isinstance(attempt, bool) or attempt <= 0:
        raise DeployError("guard attempt must be a positive integer")
    if (
        not isinstance(duration_seconds, (int, float))
        or isinstance(duration_seconds, bool)
        or duration_seconds < 0
    ):
        raise DeployError("guard duration must be non-negative")
    if not isinstance(log_path, str) or not Path(log_path).is_absolute():
        raise DeployError("guard log path must be absolute")
    if log_sha256 is not None:
        _require_digest(log_sha256, "guard log SHA-256")
    events = list(result["events"])
    phase_events = [item for item in events if item["phase"] == phase]
    if any(item["status"] == "succeeded" for item in phase_events):
        raise DeployError(f"guard phase {phase} already completed")
    same_attempt = [item for item in phase_events if item["attempt"] == attempt]
    if status == "started":
        expected_attempt = 1 + max(
            (int(item["attempt"]) for item in phase_events), default=0
        )
        if same_attempt or attempt != expected_attempt:
            raise DeployError(
                "guard started attempt is not the next immutable identity"
            )
        if duration_seconds != 0 or log_sha256 is not None:
            raise DeployError(
                "guard started event must have zero duration and no log SHA"
            )
    else:
        if len(same_attempt) >= 2:
            raise DeployError("guard attempt identity is already completed")
        if (
            len(same_attempt) != 1
            or same_attempt[0]["status"] != "started"
            or same_attempt[0]["log_path"] != log_path
        ):
            raise DeployError(
                "guard attempt already completed or has no matching start"
            )
        if log_sha256 is None:
            raise DeployError("completed guard event requires a sealed log SHA")
    body: dict[str, object] = {
        "sequence": len(events) + 1,
        "phase": phase,
        "status": status,
        "attempt": attempt,
        "duration_seconds": float(duration_seconds),
        "physical_fingerprint": physical_fingerprint,
        "physical_fingerprint_path": physical_fingerprint_path,
        "recorded_at": recorded_at,
        "log_path": log_path,
        "log_sha256": log_sha256,
        "transition_event_sha256": transition_event_sha256,
        "previous_event_sha256": (None if not events else events[-1]["event_sha256"]),
        "journal_before_sha256": result["journal_sha256"],
    }
    body["event_sha256"] = _event_sha(body)
    events.append(body)
    result["events"] = events
    sealed = _seal_guards(result)
    return sealed, body


def guard_phase_succeeded(journal: Mapping[str, object], phase: str) -> bool:
    value = _validated_guard_journal(journal)
    return any(
        event["phase"] == phase and event["status"] == "succeeded"
        for event in value["events"]
    )


def next_guard_attempt(journal: Mapping[str, object], phase: str) -> int | None:
    value = _validated_guard_journal(journal)
    if guard_phase_succeeded(value, phase):
        return None
    attempts = [
        int(event["attempt"]) for event in value["events"] if event["phase"] == phase
    ]
    return max(attempts, default=0) + 1


def validate_cross_bound_journals(
    transition: Mapping[str, object], guards: Mapping[str, object]
) -> None:
    transition_value = _validated_transition_journal(transition)
    guard_value = _validated_guard_journal(guards)
    identity = (
        transition_value.get("transition_id"),
        transition_value.get("plan_sha256"),
    )
    if identity != (
        guard_value.get("transition_id"),
        guard_value.get("plan_sha256"),
    ):
        raise DeployError("transition and guard journals have different identities")
    if transition_value.get("guard_attempt_journal_sha256") != guard_value.get(
        "journal_sha256"
    ):
        raise DeployError("transition journal does not bind the guard-attempt journal")
    transition_hashes = {event["event_sha256"] for event in transition_value["events"]}
    guard_hashes = {event["event_sha256"] for event in guard_value["events"]}
    if any(
        event["transition_event_sha256"] not in transition_hashes
        for event in guard_value["events"]
    ):
        raise DeployError("guard journal references an unknown transition checkpoint")
    if any(
        event["guard_event_sha256"] not in guard_hashes
        for event in transition_value["events"]
        if event.get("guard_event_sha256") is not None
    ):
        raise DeployError("transition journal references an unknown guard attempt")
    linked_guard_hashes = {
        event["guard_event_sha256"]
        for event in transition_value["events"]
        if event.get("guard_event_sha256") is not None
    }
    if linked_guard_hashes != guard_hashes:
        raise DeployError("guard journal has an uncheckpointed attempt event")
    guard_by_sha = {event["event_sha256"]: event for event in guard_value["events"]}
    for event in transition_value["events"]:
        guard_sha = event.get("guard_event_sha256")
        if guard_sha is None:
            continue
        guard_event = guard_by_sha[guard_sha]
        if (
            event.get("event")
            != _guard_transition_event_name(str(guard_event["status"]))
            or event.get("phase") != guard_event["phase"]
            or event.get("detail", {}).get("attempt") != guard_event["attempt"]
            or guard_event.get("transition_event_sha256")
            != event.get("previous_event_sha256")
        ):
            raise DeployError(
                "guard/transition event identities are not adjacent and cross-bound"
            )


def _reconcile_torn_cross_binding(
    transition: Mapping[str, object], guards: Mapping[str, object]
) -> tuple[dict[str, object], dict[str, object], bool]:
    """Recover the one legal two-file crash window.

    Guard evidence is persisted first.  If the process dies before the matching
    transition event is persisted, the newest guard event is an unmatched
    suffix whose transition binding is the current transition tail and whose
    ``journal_before_sha256`` is the transition journal's old guard pointer.
    No completed record is edited; recovery appends only the missing cross-link.
    """

    transition_value = _validated_transition_journal(transition)
    guard_value = _validated_guard_journal(guards)
    if (
        transition_value.get("transition_id"),
        transition_value.get("plan_sha256"),
    ) != (guard_value.get("transition_id"), guard_value.get("plan_sha256")):
        raise DeployError("transition and guard journals have different identities")
    linked = {
        event["guard_event_sha256"]
        for event in transition_value["events"]
        if event.get("guard_event_sha256") is not None
    }
    unmatched = [
        event for event in guard_value["events"] if event["event_sha256"] not in linked
    ]
    if not unmatched:
        validate_cross_bound_journals(transition_value, guard_value)
        return transition_value, guard_value, False
    if len(unmatched) != 1 or unmatched[0] is not guard_value["events"][-1]:
        raise DeployError(
            "guard/transition journals have an unrecoverable cross-binding fork"
        )
    pending = unmatched[0]
    if pending.get("transition_event_sha256") != transition_tail_sha256(
        transition_value
    ) or pending.get("journal_before_sha256") != transition_value.get(
        "guard_attempt_journal_sha256"
    ):
        raise DeployError(
            "torn guard event is not bound to the current transition tail"
        )
    detail: dict[str, object] = {"attempt": pending["attempt"], "recovered": True}
    if pending["status"] == "started":
        detail["physical_fingerprint"] = pending["physical_fingerprint"]
    transition_value = append_transition_event(
        transition_value,
        event=_guard_transition_event_name(str(pending["status"])),
        phase=str(pending["phase"]),
        recorded_at=str(pending["recorded_at"]),
        guard_event_sha256=str(pending["event_sha256"]),
        detail=detail,
    )
    transition_value["guard_attempt_journal_sha256"] = guard_value["journal_sha256"]
    transition_value = _seal_transition(transition_value)
    validate_cross_bound_journals(transition_value, guard_value)
    return transition_value, guard_value, True


def _safe_parent(path: Path) -> None:
    parent = path.parent
    details = parent.lstat()
    if (
        not stat.S_ISDIR(details.st_mode)
        or parent.is_symlink()
        or details.st_uid != os.geteuid()
        or stat.S_IMODE(details.st_mode) != 0o700
    ):
        raise DeployError(f"durable parent directory is unsafe: {parent}")


def _ensure_private_directory(path: Path) -> None:
    if not path.exists():
        path.mkdir(parents=True, mode=0o700)
    details = path.lstat()
    if (
        not stat.S_ISDIR(details.st_mode)
        or path.is_symlink()
        or details.st_uid != os.geteuid()
        or stat.S_IMODE(details.st_mode) != 0o700
    ):
        raise DeployError(f"private deploy directory is unsafe: {path}")


def _atomic_regular_write(path: Path, payload: bytes, mode: int) -> None:
    _safe_parent(path)
    if path.exists() and (path.is_symlink() or not path.is_file()):
        raise DeployError(f"durable path is not a regular file: {path}")
    descriptor, temporary_raw = tempfile.mkstemp(
        prefix=f".{path.name}.", suffix=".tmp", dir=path.parent
    )
    temporary = Path(temporary_raw)
    try:
        os.fchmod(descriptor, mode)
        with os.fdopen(descriptor, "wb") as handle:
            handle.write(payload)
            handle.flush()
            os.fsync(handle.fileno())
        os.replace(temporary, path)
        os.chmod(path, mode)
        directory_descriptor = os.open(path.parent, os.O_RDONLY | os.O_DIRECTORY)
        try:
            os.fsync(directory_descriptor)
        finally:
            os.close(directory_descriptor)
    finally:
        temporary.unlink(missing_ok=True)


def _write_checksummed_json(path: Path, payload: Mapping[str, object]) -> None:
    encoded = canonical_bytes(dict(payload))
    _atomic_regular_write(path, encoded, 0o600)
    checksum = hashlib.sha256(encoded).hexdigest()
    checksum_path = path.with_name(path.name + ".sha256")
    _atomic_regular_write(
        checksum_path, f"{checksum}  {path.name}\n".encode("ascii"), 0o600
    )


def _read_checksummed_json(
    path: Path, *, repair_valid_main_first_crash: bool = False
) -> dict[str, object]:
    checksum_path = path.with_name(path.name + ".sha256")
    _require_readable_regular(path, "durable journal")
    details = path.stat()
    if details.st_uid != os.geteuid() or stat.S_IMODE(details.st_mode) != 0o600:
        raise DeployError(f"durable journal is not owner-only: {path}")
    payload = path.read_bytes()
    digest = hashlib.sha256(payload).hexdigest()
    try:
        value = json.loads(payload)
    except (UnicodeDecodeError, json.JSONDecodeError) as exc:
        raise DeployError(f"durable journal JSON is invalid: {path}") from exc
    if not isinstance(value, dict) or payload != canonical_bytes(value):
        raise DeployError(f"durable journal is not canonical: {path}")
    expected_sidecar = f"{digest}  {path.name}\n".encode("ascii")
    sidecar_matches = False
    if checksum_path.exists():
        _require_readable_regular(checksum_path, "durable journal checksum")
        checksum_details = checksum_path.stat()
        if (
            checksum_details.st_uid != os.geteuid()
            or stat.S_IMODE(checksum_details.st_mode) != 0o600
        ):
            raise DeployError(f"durable journal is not owner-only: {checksum_path}")
        sidecar_matches = checksum_path.read_bytes() == expected_sidecar
    if not sidecar_matches:
        if not repair_valid_main_first_crash:
            raise DeployError(f"durable journal checksum sidecar drifted: {path}")
        kind = value.get("kind")
        if kind == TRANSITION_JOURNAL_VERSION:
            _validated_transition_journal(value)
        elif kind == GUARD_JOURNAL_VERSION:
            _validated_guard_journal(value)
        elif kind == HEARTBEAT_VERSION:
            _validate_heartbeat_document(value)
        else:
            raise DeployError(
                "only an internally valid journal/heartbeat may repair a sidecar"
            )
        _atomic_regular_write(checksum_path, expected_sidecar, 0o600)
    return value


def _exclusive_regular_write(path: Path, payload: bytes, mode: int = 0o400) -> None:
    _safe_parent(path)
    try:
        descriptor = os.open(
            path,
            os.O_WRONLY | os.O_CREAT | os.O_EXCL | os.O_NOFOLLOW,
            mode,
        )
    except OSError as exc:
        raise DeployError(
            f"immutable evidence already exists or is unsafe: {path}"
        ) from exc
    with os.fdopen(descriptor, "wb") as handle:
        handle.write(payload)
        handle.flush()
        os.fsync(handle.fileno())
    directory_descriptor = os.open(path.parent, os.O_RDONLY | os.O_DIRECTORY)
    try:
        os.fsync(directory_descriptor)
    finally:
        os.close(directory_descriptor)


def _write_immutable_checksummed_json(path: Path, payload: Mapping[str, object]) -> str:
    encoded = canonical_bytes(dict(payload))
    checksum = hashlib.sha256(encoded).hexdigest()
    _exclusive_regular_write(path, encoded)
    try:
        _exclusive_regular_write(
            path.with_name(path.name + ".sha256"),
            f"{checksum}  {path.name}\n".encode("ascii"),
        )
    except BaseException as exc:
        raise DeployError(
            f"immutable evidence was sealed without its checksum: {path}"
        ) from exc
    return checksum


def emit_console(value: object, *, stream: TextIO | None = None) -> None:
    """Best-effort console output; BrokenPipe never changes durable state."""

    target = sys.stdout if stream is None else stream
    try:
        if isinstance(value, str):
            payload = value if value.endswith("\n") else value + "\n"
        else:
            payload = canonical_bytes(value).decode("utf-8")
        target.write(payload)
        target.flush()
    except (BrokenPipeError, OSError):
        return


def _utc_now(clock: Callable[[], float] = time.time) -> str:
    return (
        datetime.fromtimestamp(clock(), timezone.utc).isoformat().replace("+00:00", "Z")
    )


def _parse_utc(value: object) -> float:
    if not isinstance(value, str):
        raise DeployError("persisted budget start is malformed")
    try:
        parsed = datetime.fromisoformat(value.replace("Z", "+00:00"))
    except ValueError as exc:
        raise DeployError("persisted budget start is malformed") from exc
    if parsed.tzinfo is None:
        raise DeployError("persisted budget start is not timezone-aware")
    return parsed.timestamp()


def _validate_heartbeat_document(
    value: object, plan: Mapping[str, object] | None = None
) -> dict[str, object]:
    expected_fields = {
        "kind",
        "transition_id",
        "plan_sha256",
        "sequence",
        "operation",
        "observed_at",
        "operation_elapsed_seconds",
        "total_elapsed_seconds",
        "remaining_seconds",
        "eta_deadline",
    }
    if not isinstance(value, dict) or set(value) != expected_fields:
        raise DeployError("durable heartbeat schema drifted")
    transition_id = value.get("transition_id")
    if (
        value.get("kind") != HEARTBEAT_VERSION
        or not isinstance(transition_id, str)
        or _TRANSITION_ID.fullmatch(transition_id) is None
    ):
        raise DeployError("durable heartbeat identity is malformed")
    _require_digest(value.get("plan_sha256"), "heartbeat plan SHA-256")
    if plan is not None and (
        transition_id != plan["transition_id"]
        or value.get("plan_sha256") != plan["plan_sha256"]
    ):
        raise DeployError("durable heartbeat plan identity drifted")
    sequence = value.get("sequence")
    operation = value.get("operation")
    if (
        not isinstance(sequence, int)
        or isinstance(sequence, bool)
        or sequence <= 0
        or not isinstance(operation, str)
        or not operation
    ):
        raise DeployError("durable heartbeat sequence/operation is malformed")
    _parse_utc(value.get("observed_at"))
    _parse_utc(value.get("eta_deadline"))
    for key in (
        "operation_elapsed_seconds",
        "total_elapsed_seconds",
        "remaining_seconds",
    ):
        number = value.get(key)
        if (
            not isinstance(number, (int, float))
            or isinstance(number, bool)
            or number < 0
        ):
            raise DeployError(f"durable heartbeat {key} is malformed")
    operation_elapsed = float(value["operation_elapsed_seconds"])
    total_elapsed = float(value["total_elapsed_seconds"])
    remaining = float(value["remaining_seconds"])
    if (
        total_elapsed > TOTAL_TIMEOUT_SECONDS
        or remaining > TOTAL_TIMEOUT_SECONDS
        or abs(total_elapsed + remaining - TOTAL_TIMEOUT_SECONDS) > 0.002
        or operation_elapsed > total_elapsed + 0.002
    ):
        raise DeployError("durable heartbeat budget accounting drifted")
    return json.loads(json.dumps(value))


def _heartbeat_evidence_reference(
    plan: Mapping[str, object], path: Path, *, required: bool
) -> dict[str, str] | None:
    checksum_path = path.with_name(path.name + ".sha256")
    pair = (path.exists(), checksum_path.exists())
    if pair == (False, False):
        if required:
            raise DeployError("immutable result requires durable heartbeat evidence")
        return None
    if pair not in {(True, False), (True, True)}:
        raise DeployError("durable heartbeat evidence is only partially present")
    value = _read_checksummed_json(path, repair_valid_main_first_crash=True)
    _validate_heartbeat_document(value, plan)
    return {"path": str(path), "sha256": _file_sha256(path)}


@dataclass
class Budget:
    started_epoch: float
    clock: Callable[[], float] = time.time
    monotonic_clock: Callable[[], float] = time.monotonic
    invocation_started_epoch: float | None = None
    invocation_started_monotonic: float | None = None

    def __post_init__(self) -> None:
        wall_anchor = (
            self.clock()
            if self.invocation_started_epoch is None
            else self.invocation_started_epoch
        )
        monotonic_anchor = (
            self.monotonic_clock()
            if self.invocation_started_monotonic is None
            else self.invocation_started_monotonic
        )
        invocation_allowance = min(
            float(TOTAL_TIMEOUT_SECONDS),
            max(0.0, self.deadline_epoch - wall_anchor),
        )
        self.invocation_deadline_monotonic = monotonic_anchor + invocation_allowance

    @property
    def deadline_epoch(self) -> float:
        return self.started_epoch + TOTAL_TIMEOUT_SECONDS

    def remaining(self) -> float:
        return min(
            self.deadline_epoch - self.clock(),
            self.invocation_deadline_monotonic - self.monotonic_clock(),
        )

    def require(self, label: str) -> float:
        remaining = self.remaining()
        if remaining <= 0:
            raise DeployError(
                f"three-hour total deployment deadline expired before {label}"
            )
        return remaining


@dataclass
class ProcessResult:
    returncode: int
    duration_seconds: float
    log_sha256: str
    output_sha256: str | None = None


@dataclass
class ExecutionContext:
    plan: dict[str, object]
    state_root: Path
    log_root: Path
    transition_path: Path
    guard_path: Path
    heartbeat_path: Path
    transition: dict[str, object]
    guards: dict[str, object]
    budget: Budget
    clock: Callable[[], float] = time.time
    sleeper: Callable[[float], None] = time.sleep
    heartbeat_sequence: int = 0

    def persist_transition(self) -> None:
        self.transition["guard_attempt_journal_sha256"] = self.guards["journal_sha256"]
        self.transition = _seal_transition(self.transition)
        _write_checksummed_json(self.transition_path, self.transition)

    def persist_guards(self) -> None:
        _write_checksummed_json(self.guard_path, self.guards)

    def transition_event(
        self,
        event: str,
        *,
        phase: str | None = None,
        guard_event_sha256: str | None = None,
        detail: Mapping[str, object] | None = None,
    ) -> None:
        self.transition = append_transition_event(
            self.transition,
            event=event,
            phase=phase,
            recorded_at=_utc_now(self.clock),
            guard_event_sha256=guard_event_sha256,
            detail=detail,
        )
        self.persist_transition()

    def heartbeat(self, operation: str, operation_started_monotonic: float) -> None:
        self.heartbeat_sequence += 1
        monotonic_now = self.budget.monotonic_clock()
        wall_now = self.clock()
        remaining = max(0.0, self.budget.remaining())
        effective_eta_epoch = min(self.budget.deadline_epoch, wall_now + remaining)
        total_elapsed = max(0.0, TOTAL_TIMEOUT_SECONDS - remaining)
        operation_elapsed = min(
            max(0.0, monotonic_now - operation_started_monotonic), total_elapsed
        )
        payload = {
            "kind": HEARTBEAT_VERSION,
            "transition_id": self.plan["transition_id"],
            "plan_sha256": self.plan["plan_sha256"],
            "sequence": self.heartbeat_sequence,
            "operation": operation,
            "observed_at": _utc_now(lambda: wall_now),
            "operation_elapsed_seconds": round(operation_elapsed, 3),
            "total_elapsed_seconds": round(total_elapsed, 3),
            "remaining_seconds": round(remaining, 3),
            "eta_deadline": datetime.fromtimestamp(effective_eta_epoch, timezone.utc)
            .isoformat()
            .replace("+00:00", "Z"),
        }
        _write_checksummed_json(self.heartbeat_path, payload)
        emit_console(
            {
                "operation": operation,
                "elapsed_seconds": payload["total_elapsed_seconds"],
                "remaining_seconds": payload["remaining_seconds"],
            }
        )


def _open_owned_output(path: Path) -> tuple[int, TextIO]:
    _safe_parent(path)
    try:
        descriptor = os.open(
            path,
            os.O_WRONLY | os.O_CREAT | os.O_EXCL | os.O_NOFOLLOW,
            0o600,
        )
    except OSError as exc:
        raise DeployError(
            f"child output path already exists or is unsafe: {path}"
        ) from exc
    details = os.fstat(descriptor)
    if (
        not stat.S_ISREG(details.st_mode)
        or details.st_uid != os.geteuid()
        or details.st_nlink != 1
    ):
        os.close(descriptor)
        raise DeployError(f"child output is not an owned regular file: {path}")
    return descriptor, os.fdopen(descriptor, "wb")


def _terminate_process_group(process: subprocess.Popen[bytes]) -> None:
    try:
        os.killpg(process.pid, signal.SIGTERM)
    except ProcessLookupError:
        return
    deadline = time.monotonic() + TERMINATION_GRACE_SECONDS
    while process.poll() is None and time.monotonic() < deadline:
        time.sleep(0.1)
    if process.poll() is None:
        try:
            os.killpg(process.pid, signal.SIGKILL)
        except ProcessLookupError:
            pass
    try:
        process.wait(timeout=TERMINATION_GRACE_SECONDS)
    except subprocess.TimeoutExpired as exc:
        raise DeployError("timed-out child process group survived SIGKILL") from exc


def _run_process(
    context: ExecutionContext,
    argv: Sequence[str],
    *,
    operation: str,
    log_path: Path,
    timeout_seconds: float,
    env: Mapping[str, str] | None = None,
    stdin_path: Path | None = None,
    stdout_path: Path | None = None,
    check: bool = True,
) -> ProcessResult:
    if not argv or any(not isinstance(value, str) or not value for value in argv):
        raise DeployError(f"{operation} argv is malformed")
    remaining = context.budget.require(operation)
    allowed = min(float(timeout_seconds), remaining)
    if allowed <= 0:
        raise DeployError(f"no deployment budget remains for {operation}")
    monotonic_started = time.monotonic()
    operation_deadline = monotonic_started + allowed
    _log_descriptor, log_handle = _open_owned_output(log_path)
    output_handle: TextIO | None = None
    input_handle: TextIO | None = None
    if stdout_path is not None:
        _output_descriptor, output_handle = _open_owned_output(stdout_path)
    if stdin_path is not None:
        _require_readable_regular(stdin_path, f"{operation} stdin")
        input_handle = stdin_path.open("rb")
    process: subprocess.Popen[bytes] | None = None
    timed_out = False
    next_heartbeat = monotonic_started
    try:
        process = subprocess.Popen(
            list(argv),
            stdin=subprocess.DEVNULL if input_handle is None else input_handle,
            stdout=log_handle if output_handle is None else output_handle,
            stderr=subprocess.STDOUT if output_handle is None else log_handle,
            env=None if env is None else dict(env),
            start_new_session=True,
            shell=False,
        )
        while process.poll() is None:
            monotonic_now = time.monotonic()
            wall_now = context.clock()
            if (
                monotonic_now >= operation_deadline
                or wall_now >= context.budget.deadline_epoch
            ):
                timed_out = True
                _terminate_process_group(process)
                break
            if monotonic_now >= next_heartbeat:
                context.heartbeat(operation, monotonic_started)
                next_heartbeat = monotonic_now + HEARTBEAT_INTERVAL_SECONDS
            context.sleeper(min(0.1, operation_deadline - monotonic_now))
        returncode = process.wait()
        context.heartbeat(operation, monotonic_started)
    finally:
        if process is not None and process.poll() is None:
            _terminate_process_group(process)
        if input_handle is not None:
            input_handle.close()
        for handle in (output_handle, log_handle):
            if handle is not None:
                handle.flush()
                os.fsync(handle.fileno())
                handle.close()
        for path in (log_path, stdout_path):
            if path is not None and path.exists():
                os.chmod(path, 0o400)
    duration = max(0.0, time.monotonic() - monotonic_started)
    log_sha = _file_sha256(log_path)
    output_sha = None if stdout_path is None else _file_sha256(stdout_path)
    if timed_out:
        raise DeployError(f"{operation} exceeded its real process-group timeout")
    if check and returncode != 0:
        raise DeployError(
            f"{operation} exited with status {returncode}; see {log_path}"
        )
    context.budget.require(f"completion of {operation}")
    return ProcessResult(returncode, duration, log_sha, output_sha)


def _compose_environment(plan: Mapping[str, object]) -> dict[str, str]:
    environment = os.environ.copy()
    environment.update(
        {
            "ESPN_AIRFLOW_IMAGE": str(plan["airflow_image"]),
            "ESPN_POSTGRES_IMAGE": str(plan["postgres_image"]),
            "ESPN_RELEASE_ROOT": str(plan["release_root"]),
            "ESPN_DAGBAG_ROOT": str(plan["dagbag_root"]),
            "ESPN_RELEASE_COMMIT": str(plan["release_commit"]),
            "ESPN_RELEASE_TREE_SHA256": str(plan["release_tree_sha256"]),
            "ESPN_BRONZE_LAYOUT_MODE": str(plan["layout_mode"]),
            "ESPN_AIRFLOW_UI_BIND": "127.0.0.1:8086",
        }
    )
    return environment


def _fingerprint_command(container: str) -> list[str]:
    template = (
        "{{json .Id}}\n{{json .Image}}\n{{json .Config.Image}}\n"
        "{{json .State.Status}}\n"
        "{{json .State.StartedAt}}\n"
        '{{if .State.Health}}{{json .State.Health.Status}}{{else}}"none"{{end}}\n'
        "{{json .HostConfig.PortBindings}}\n{{json .Mounts}}"
    )
    return ["docker", "inspect", "--type", "container", "--format", template, container]


def _capture_physical_fingerprint(
    context: ExecutionContext,
    *,
    phase: str,
    attempt: int,
    stage: str,
    phase_deadline_monotonic: float,
) -> dict[str, object]:
    if stage not in {"before", "after"}:
        raise DeployError("physical fingerprint stage is invalid")
    containers: dict[str, object] = {}
    capture_nonce = f"{os.getpid():x}-{time.monotonic_ns():x}"
    for index, container in enumerate(FINGERPRINT_CONTAINERS, start=1):
        remaining = min(
            context.budget.require(f"{phase} physical fingerprint"),
            phase_deadline_monotonic - time.monotonic(),
        )
        if remaining <= 0:
            raise DeployError(f"{phase} guard deadline expired during fingerprint")
        log_path = context.log_root / (
            f"guard-{phase}-attempt-{attempt:03d}-fingerprint-"
            f"{stage}-{capture_nonce}-{index}.log"
        )
        observed = _run_process(
            context,
            _fingerprint_command(container),
            operation=f"{phase} physical fingerprint {container}",
            log_path=log_path,
            timeout_seconds=remaining,
            check=False,
        )
        if observed.returncode != 0:
            containers[container] = {
                "presence": "unavailable",
                "inspect_returncode": observed.returncode,
                "inspect_log_path": str(log_path),
                "inspect_log_sha256": observed.log_sha256,
            }
            continue
        try:
            lines = log_path.read_text(encoding="utf-8").splitlines()
            fields = [json.loads(line) for line in lines]
            if len(fields) != 8:
                raise ValueError("wrong field count")
            (
                container_id,
                image_id,
                image_reference,
                status_name,
                started_at,
                health,
                bindings,
                mounts,
            ) = fields
        except (OSError, UnicodeDecodeError, ValueError, json.JSONDecodeError) as exc:
            raise DeployError(
                f"physical fingerprint is malformed for {container}"
            ) from exc
        if not container_id or not image_id or not status_name or not started_at:
            raise DeployError(f"physical fingerprint is incomplete for {container}")
        containers[container] = {
            "container_id": container_id,
            "image_id": image_id,
            "image_reference": image_reference,
            "status": status_name,
            "started_at": started_at,
            "health": health,
            "port_bindings": bindings,
            "mounts_sha256": canonical_sha256(mounts),
            "mounts": mounts,
            "inspect_log_path": str(log_path),
            "inspect_log_sha256": observed.log_sha256,
        }
    evidence = {
        "kind": FINGERPRINT_VERSION,
        "transition_id": context.plan["transition_id"],
        "plan_sha256": context.plan["plan_sha256"],
        "phase": phase,
        "attempt": attempt,
        "stage": stage,
        "containers": containers,
    }
    evidence_path = context.log_root / (
        f"guard-{phase}-attempt-{attempt:03d}-fingerprint-{stage}-{capture_nonce}.json"
    )
    _exclusive_regular_write(evidence_path, canonical_bytes(evidence))
    evidence_sha = _file_sha256(evidence_path)
    return {
        "containers": containers,
        "fingerprint_sha256": evidence_sha,
        "evidence_path": str(evidence_path),
    }


def _assert_postdeploy_fingerprint(
    plan: Mapping[str, object], fingerprint: Mapping[str, object]
) -> None:
    containers = fingerprint.get("containers")
    if not isinstance(containers, dict) or set(containers) != set(
        FINGERPRINT_CONTAINERS
    ):
        raise DeployError("post-deploy physical container inventory drifted")
    metadb = containers[FINGERPRINT_CONTAINERS[0]]
    scheduler = containers[FINGERPRINT_CONTAINERS[1]]
    webserver = containers[FINGERPRINT_CONTAINERS[2]]
    if not all(isinstance(value, dict) for value in (metadb, scheduler, webserver)):
        raise DeployError("post-deploy physical fingerprint is malformed")
    if metadb.get("status") != "running" or metadb.get("health") != "healthy":
        raise DeployError("post-deploy metadb is not healthy")
    if scheduler.get("status") != "running" or scheduler.get("health") != "healthy":
        raise DeployError("post-deploy scheduler is not healthy")
    if webserver.get("status") != "running" or webserver.get("health") not in {
        "healthy",
        "none",
    }:
        raise DeployError("post-deploy webserver is not runnable")
    expected_binding = {"8080/tcp": [{"HostIp": "127.0.0.1", "HostPort": "8086"}]}
    if webserver.get("port_bindings") != expected_binding:
        raise DeployError(
            "post-deploy webserver is not bound exactly to 127.0.0.1:8086"
        )
    expected_images = {
        FINGERPRINT_CONTAINERS[0]: plan["postgres_image"],
        FINGERPRINT_CONTAINERS[1]: plan["airflow_image"],
        FINGERPRINT_CONTAINERS[2]: plan["airflow_image"],
    }
    for name, payload in containers.items():
        if payload.get("image_reference") != expected_images[name]:
            raise DeployError(f"post-deploy {name} image reference drifted")
    expected_mounts = {
        "/opt/airflow/dags": Path(str(plan["dagbag_root"])).resolve(),
        "/opt/espn-source/dags": Path(str(plan["release_root"])).resolve() / "dags",
        "/opt/airflow/scrapers": Path(str(plan["release_root"])).resolve() / "scrapers",
        "/opt/airflow/scripts": Path(str(plan["release_root"])).resolve() / "scripts",
        "/opt/airflow/configs/espn": Path(str(plan["release_root"])).resolve()
        / "configs/espn",
        "/opt/airflow/configs/medallion": Path(str(plan["release_root"])).resolve()
        / "configs/medallion",
    }
    for payload, label in ((scheduler, "scheduler"), (webserver, "webserver")):
        mounts = payload.get("mounts")
        if not isinstance(mounts, list):
            raise DeployError(f"post-deploy {label} mount inventory is malformed")
        required = [
            item
            for item in mounts
            if isinstance(item, dict) and item.get("Destination") in expected_mounts
        ]
        if len(required) != len(expected_mounts) or any(
            item.get("Type") != "bind" or item.get("RW") is not False
            for item in required
        ):
            raise DeployError(
                f"post-deploy {label} release mounts are not exact read-only binds"
            )
        observed = {
            str(item["Destination"]): Path(str(item.get("Source"))).resolve()
            for item in required
        }
        if observed != expected_mounts:
            raise DeployError(f"post-deploy {label} release mount identity drifted")


def _validate_owned_regular_hash(path: Path, digest: object, *, label: str) -> None:
    expected = _require_digest(digest, f"{label} SHA-256")
    _require_readable_regular(path, label)
    details = path.stat()
    if (
        details.st_uid != os.geteuid()
        or stat.S_IMODE(details.st_mode) != 0o400
        or _file_sha256(path) != expected
    ):
        raise DeployError(f"{label} ownership/mode/content drifted")


def _validate_guard_success_report(
    plan: Mapping[str, object],
    *,
    phase: str,
    attempt: int,
    log_path: Path,
    log_sha256: object,
) -> None:
    """Accept only the exact canonical, plan-bound v1 success envelope."""

    _validate_owned_regular_hash(log_path, log_sha256, label="guard success report")
    payload = log_path.read_bytes()
    try:
        report = json.loads(payload)
        canonical = canonical_bytes(report)
    except (UnicodeDecodeError, json.JSONDecodeError, DeployError) as exc:
        raise DeployError("guard success report is not canonical JSON") from exc
    expected_fields = {
        "kind",
        "schema_version",
        "status",
        "phase",
        "attempt",
        "transition_id",
        "plan_sha256",
        "checks",
    }
    if (
        not isinstance(report, dict)
        or set(report) != expected_fields
        or payload != canonical
        or report.get("kind") != GUARD_REPORT_KIND
        or type(report.get("schema_version")) is not int
        or report.get("schema_version") != GUARD_REPORT_SCHEMA_VERSION
        or report.get("status") != "ok"
        or report.get("phase") != phase
        or type(report.get("attempt")) is not int
        or report.get("attempt") != attempt
        or report.get("transition_id") != plan.get("transition_id")
        or report.get("plan_sha256") != plan.get("plan_sha256")
    ):
        raise DeployError("guard success report identity or status drifted")
    checks = report.get("checks")
    if (
        not isinstance(checks, dict)
        or set(checks) != set(GUARD_SUCCESS_CHECKS)
        or any(checks[name] is not True for name in GUARD_SUCCESS_CHECKS)
    ):
        raise DeployError("guard success report checks did not all pass")


def _validate_physical_fingerprint_evidence(
    plan: Mapping[str, object], guards: Mapping[str, object]
) -> None:
    journal = _validated_guard_journal(guards)
    for event in journal["events"]:
        evidence_path = Path(str(event["physical_fingerprint_path"]))
        _validate_owned_regular_hash(
            evidence_path,
            event["physical_fingerprint"],
            label="guard physical fingerprint evidence",
        )
        payload = evidence_path.read_bytes()
        try:
            evidence = json.loads(payload)
        except (UnicodeDecodeError, json.JSONDecodeError) as exc:
            raise DeployError(
                "guard physical fingerprint evidence is invalid JSON"
            ) from exc
        expected_fields = {
            "kind",
            "transition_id",
            "plan_sha256",
            "phase",
            "attempt",
            "stage",
            "containers",
        }
        if (
            not isinstance(evidence, dict)
            or set(evidence) != expected_fields
            or payload != canonical_bytes(evidence)
            or evidence.get("kind") != FINGERPRINT_VERSION
            or evidence.get("transition_id") != plan["transition_id"]
            or evidence.get("plan_sha256") != plan["plan_sha256"]
            or evidence.get("phase") != event["phase"]
            or evidence.get("attempt") != event["attempt"]
            or evidence.get("stage") not in {"before", "after"}
        ):
            raise DeployError("guard physical fingerprint evidence identity drifted")
        if (
            event["status"] == "started"
            and evidence["stage"] != "before"
            or event["status"] == "succeeded"
            and evidence["stage"] != "after"
        ):
            raise DeployError("guard physical fingerprint evidence stage drifted")
        containers = evidence.get("containers")
        if not isinstance(containers, dict) or set(containers) != set(
            FINGERPRINT_CONTAINERS
        ):
            raise DeployError("guard physical fingerprint container set drifted")
        for container in containers.values():
            if not isinstance(container, dict):
                raise DeployError("guard physical fingerprint container is malformed")
            inspect_path = Path(str(container.get("inspect_log_path")))
            _validate_owned_regular_hash(
                inspect_path,
                container.get("inspect_log_sha256"),
                label="guard container inspect log",
            )
        if event["status"] != "started":
            _validate_owned_regular_hash(
                Path(str(event["log_path"])),
                event["log_sha256"],
                label="guard process log",
            )


def _guard_transition_event_name(status_name: str) -> str:
    return f"guard_{status_name}"


def _run_guard_phase(context: ExecutionContext, phase: str) -> None:
    if guard_phase_succeeded(context.guards, phase):
        return
    attempt = next_guard_attempt(context.guards, phase)
    if attempt is None:
        return
    phase_started_monotonic = time.monotonic()
    phase_deadline_monotonic = phase_started_monotonic + GUARD_TIMEOUT_SECONDS
    fingerprint = _capture_physical_fingerprint(
        context,
        phase=phase,
        attempt=attempt,
        stage="before",
        phase_deadline_monotonic=phase_deadline_monotonic,
    )
    fingerprint_sha = str(fingerprint["fingerprint_sha256"])
    log_path = context.log_root / f"guard-{phase}-attempt-{attempt:03d}.log"
    context.guards, started = append_guard_event(
        context.guards,
        transition_event_sha256=transition_tail_sha256(context.transition),
        phase=phase,
        status="started",
        attempt=attempt,
        duration_seconds=0.0,
        physical_fingerprint=fingerprint_sha,
        physical_fingerprint_path=str(fingerprint["evidence_path"]),
        recorded_at=_utc_now(context.clock),
        log_path=str(log_path),
        log_sha256=None,
    )
    context.persist_guards()
    context.transition_event(
        "guard_started",
        phase=phase,
        guard_event_sha256=str(started["event_sha256"]),
        detail={"attempt": attempt, "physical_fingerprint": fingerprint_sha},
    )
    guard = context.plan["guard"]
    if not isinstance(guard, dict) or not isinstance(guard.get("argv"), list):
        raise DeployError("reviewed guard command is malformed")
    terminal_status = "succeeded"
    result: ProcessResult | None = None
    terminal_fingerprint = fingerprint
    try:
        remaining = min(
            context.budget.require(f"{phase} guard"),
            phase_deadline_monotonic - time.monotonic(),
        )
        if remaining <= 0:
            raise DeployError(f"{phase} guard deadline expired before guard execution")
        environment = os.environ.copy()
        environment.update(
            {
                "ESPN_DEPLOY_GUARD_PHASE": phase,
                "ESPN_DEPLOY_GUARD_ATTEMPT": str(attempt),
                "ESPN_DEPLOY_TRANSITION_ID": str(context.plan["transition_id"]),
                "ESPN_DEPLOY_PLAN_SHA256": str(context.plan["plan_sha256"]),
            }
        )
        # Rebind the actual executable and every reviewed guard file at the
        # last possible moment; a multi-hour transition never relies solely
        # on the initial preflight hash.
        _validate_guard_runtime(context.plan)
        result = _run_process(
            context,
            guard["argv"],
            operation=f"{phase} quiescence guard",
            log_path=log_path,
            timeout_seconds=min(remaining, GUARD_PROCESS_TIMEOUT_SECONDS),
            env=environment,
        )
        _validate_guard_success_report(
            context.plan,
            phase=phase,
            attempt=attempt,
            log_path=log_path,
            log_sha256=result.log_sha256,
        )
        terminal_fingerprint = _capture_physical_fingerprint(
            context,
            phase=phase,
            attempt=attempt,
            stage="after",
            phase_deadline_monotonic=phase_deadline_monotonic,
        )
        if phase == "post_deploy":
            _assert_postdeploy_fingerprint(context.plan, terminal_fingerprint)
    except BaseException as exc:
        terminal_status = "failed"
        duration = max(0.0, time.monotonic() - phase_started_monotonic)
        if result is None:
            if not log_path.exists():
                _exclusive_regular_write(
                    log_path,
                    (f"pre-spawn guard failure: {type(exc).__name__}: {exc}\n").encode(
                        "utf-8"
                    ),
                )
            log_sha = _file_sha256(log_path)
        else:
            log_sha = result.log_sha256
        context.guards, terminal = append_guard_event(
            context.guards,
            transition_event_sha256=transition_tail_sha256(context.transition),
            phase=phase,
            status=terminal_status,
            attempt=attempt,
            duration_seconds=duration,
            physical_fingerprint=str(terminal_fingerprint["fingerprint_sha256"]),
            physical_fingerprint_path=str(terminal_fingerprint["evidence_path"]),
            recorded_at=_utc_now(context.clock),
            log_path=str(log_path),
            log_sha256=log_sha,
        )
        context.persist_guards()
        context.transition_event(
            _guard_transition_event_name(terminal_status),
            phase=phase,
            guard_event_sha256=str(terminal["event_sha256"]),
            detail={"attempt": attempt},
        )
        raise
    if result is None:
        raise DeployError("guard execution produced no result")
    duration = max(0.0, time.monotonic() - phase_started_monotonic)
    context.guards, terminal = append_guard_event(
        context.guards,
        transition_event_sha256=transition_tail_sha256(context.transition),
        phase=phase,
        status=terminal_status,
        attempt=attempt,
        duration_seconds=duration,
        physical_fingerprint=str(terminal_fingerprint["fingerprint_sha256"]),
        physical_fingerprint_path=str(terminal_fingerprint["evidence_path"]),
        recorded_at=_utc_now(context.clock),
        log_path=str(log_path),
        log_sha256=result.log_sha256,
    )
    context.persist_guards()
    context.transition_event(
        _guard_transition_event_name(terminal_status),
        phase=phase,
        guard_event_sha256=str(terminal["event_sha256"]),
        detail={"attempt": attempt},
    )


def _action_events(
    transition: Mapping[str, object], action: str
) -> list[dict[str, object]]:
    value = _validated_transition_journal(transition)
    return [
        event
        for event in value["events"]
        if event["event"] in {"action_started", "action_succeeded", "action_failed"}
        and event.get("detail", {}).get("action") == action
    ]


def _action_succeeded(transition: Mapping[str, object], action: str) -> bool:
    return any(
        event["event"] == "action_succeeded"
        for event in _action_events(transition, action)
    )


def _next_action_attempt(transition: Mapping[str, object], action: str) -> int:
    attempts = [
        int(event.get("detail", {}).get("attempt", 0))
        for event in _action_events(transition, action)
    ]
    return max(attempts, default=0) + 1


def _run_named_action(
    context: ExecutionContext,
    action: str,
    argv: Sequence[str],
    *,
    timeout_seconds: float | None = None,
    env: Mapping[str, str] | None = None,
) -> ProcessResult | None:
    if _action_succeeded(context.transition, action):
        return None
    attempt = _next_action_attempt(context.transition, action)
    log_path = context.log_root / f"action-{action}-attempt-{attempt:03d}.log"
    context.transition_event(
        "action_started",
        detail={"action": action, "attempt": attempt, "log_path": str(log_path)},
    )
    started = time.monotonic()
    try:
        result = _run_process(
            context,
            argv,
            operation=action,
            log_path=log_path,
            timeout_seconds=(
                context.budget.require(action)
                if timeout_seconds is None
                else min(timeout_seconds, context.budget.require(action))
            ),
            env=env,
        )
    except BaseException as exc:
        context.transition_event(
            "action_failed",
            detail={
                "action": action,
                "attempt": attempt,
                "duration_seconds": round(max(0.0, time.monotonic() - started), 3),
                "error_type": type(exc).__name__,
            },
        )
        raise
    context.transition_event(
        "action_succeeded",
        detail={
            "action": action,
            "attempt": attempt,
            "duration_seconds": round(result.duration_seconds, 3),
            "log_path": str(log_path),
            "log_sha256": result.log_sha256,
        },
    )
    return result


def _restore_commands(
    plan: Mapping[str, object], backup_path: Path
) -> dict[str, list[str]]:
    restore = plan["restore_drill"]
    if not isinstance(restore, dict):
        raise DeployError("reviewed restore drill is malformed")
    name = str(restore["container"])
    image = str(restore["image"])
    return {
        "inspect_owner": [
            "docker",
            "inspect",
            "--type",
            "container",
            "--format",
            (
                "{{json .Config.Labels}}\n"
                "{{json .HostConfig.NetworkMode}}\n"
                "{{json .Config.Image}}"
            ),
            name,
        ],
        "start": [
            "docker",
            "run",
            "-d",
            "--pull=never",
            "--name",
            name,
            "--network",
            "none",
            "--tmpfs",
            f"/var/lib/postgresql/data:{restore['tmpfs']}",
            "--security-opt",
            "no-new-privileges:true",
            "--cap-drop",
            "ALL",
            "--cap-add",
            "CHOWN",
            "--cap-add",
            "SETGID",
            "--cap-add",
            "SETUID",
            "--cap-add",
            "FOWNER",
            "--cap-add",
            "DAC_READ_SEARCH",
            "--label",
            f"org.dpf.espn.deploy.ownership={restore['ownership_label']}",
            "-e",
            "POSTGRES_USER=airflow",
            "-e",
            "POSTGRES_DB=airflow",
            "-e",
            "POSTGRES_HOST_AUTH_METHOD=trust",
            image,
        ],
        "ready": [
            "docker",
            "exec",
            name,
            "pg_isready",
            "--username=airflow",
            "--dbname=airflow",
        ],
        "restore": [
            "docker",
            "exec",
            "-i",
            name,
            "pg_restore",
            "--username=airflow",
            "--dbname=airflow",
            "--exit-on-error",
            "--single-transaction",
            "--no-owner",
            "--no-privileges",
        ],
        "verify": [
            "docker",
            "exec",
            name,
            "psql",
            "-X",
            "--set",
            "ON_ERROR_STOP=1",
            "--username=airflow",
            "--dbname=airflow",
            "--tuples-only",
            "--no-align",
            "--command",
            "SELECT count(*) FROM alembic_version",
        ],
        "remove": ["docker", "rm", "-f", name],
    }


def _remove_stale_owned_restore_container(
    context: ExecutionContext,
    commands: Mapping[str, Sequence[str]],
    *,
    attempt: int,
) -> None:
    inspect_log = context.log_root / (
        f"restore-attempt-{attempt:03d}-preclean-inspect.log"
    )
    observed = _run_process(
        context,
        commands["inspect_owner"],
        operation="stale restore container ownership inspection",
        log_path=inspect_log,
        timeout_seconds=min(
            60, context.budget.require("stale restore container inspection")
        ),
        check=False,
    )
    if observed.returncode != 0:
        return
    try:
        lines = inspect_log.read_text(encoding="utf-8").splitlines()
        labels, network_mode, image = [json.loads(line) for line in lines]
    except (OSError, UnicodeDecodeError, ValueError, json.JSONDecodeError) as exc:
        raise DeployError("stale restore container identity is malformed") from exc
    restore = context.plan["restore_drill"]
    if (
        len(lines) != 3
        or not isinstance(labels, dict)
        or labels.get("org.dpf.espn.deploy.ownership") != restore["ownership_label"]
        or network_mode != "none"
        or image != context.plan["postgres_image"]
    ):
        raise DeployError(
            "restore container name is occupied by an object not owned by this plan"
        )
    removed = _run_process(
        context,
        commands["remove"],
        operation="stale owned restore container cleanup",
        log_path=context.log_root
        / f"restore-attempt-{attempt:03d}-preclean-remove.log",
        timeout_seconds=min(
            60, context.budget.require("stale owned restore container cleanup")
        ),
        check=False,
    )
    if removed.returncode != 0:
        raise DeployError("failed to remove stale owned restore container")


def _restore_drill(
    context: ExecutionContext, backup_path: Path, *, attempt: int
) -> dict[str, object]:
    commands = _restore_commands(context.plan, backup_path)
    prefix = f"restore-attempt-{attempt:03d}"
    _remove_stale_owned_restore_container(context, commands, attempt=attempt)
    started = False
    evidence: dict[str, object] = {}
    proof: dict[str, object] | None = None
    try:
        start_log = context.log_root / f"{prefix}-start.log"
        result = _run_process(
            context,
            commands["start"],
            operation="restore drill container start",
            log_path=start_log,
            timeout_seconds=context.budget.require("restore drill start"),
        )
        evidence["start_log"] = {
            "path": str(start_log),
            "sha256": result.log_sha256,
        }
        started = True
        ready = False
        for ready_attempt in range(1, 61):
            ready_log = context.log_root / f"{prefix}-ready-{ready_attempt:03d}.log"
            probe = _run_process(
                context,
                commands["ready"],
                operation="restore drill readiness",
                log_path=ready_log,
                timeout_seconds=min(5, context.budget.require("restore readiness")),
                check=False,
            )
            if probe.returncode == 0:
                evidence["ready_log"] = {
                    "path": str(ready_log),
                    "sha256": probe.log_sha256,
                }
                ready = True
                break
            context.sleeper(1)
        if not ready:
            raise DeployError("disposable metadb restore drill did not become ready")
        restore_log = context.log_root / f"{prefix}-apply.log"
        restored = _run_process(
            context,
            commands["restore"],
            operation="full metadb restore proof",
            log_path=restore_log,
            timeout_seconds=context.budget.require("full restore proof"),
            stdin_path=backup_path,
        )
        evidence["restore_log"] = {
            "path": str(restore_log),
            "sha256": restored.log_sha256,
        }
        verify_path = context.log_root / f"{prefix}-verify.log"
        verified = _run_process(
            context,
            commands["verify"],
            operation="restored metadb verification",
            log_path=verify_path,
            timeout_seconds=context.budget.require("restored metadb verification"),
        )
        if verify_path.read_text(encoding="utf-8").strip() != "1":
            raise DeployError(
                "restored metadb has no exact Airflow alembic identity row"
            )
        evidence["verify_log"] = {
            "path": str(verify_path),
            "sha256": verified.log_sha256,
        }
        proof = {
            "passed": True,
            "container": context.plan["restore_drill"]["container"],
            "image": context.plan["postgres_image"],
            "network": "none",
        }
    finally:
        if started:
            cleanup_log = context.log_root / f"{prefix}-remove.log"
            cleanup = _run_process(
                context,
                commands["remove"],
                operation="restore drill container cleanup",
                log_path=cleanup_log,
                timeout_seconds=min(60, context.budget.require("restore cleanup")),
                check=False,
            )
            if cleanup.returncode != 0:
                raise DeployError("failed to remove the owned restore-drill container")
            evidence["cleanup_log"] = {
                "path": str(cleanup_log),
                "sha256": cleanup.log_sha256,
            }
    if proof is None:
        raise DeployError("restore drill completed without proof identity")
    return {**proof, **evidence}


def _validated_backup_evidence(
    plan: Mapping[str, object], evidence: object
) -> dict[str, object]:
    expected_fields = {
        "dump_path",
        "dump_sha256",
        "dump_bytes",
        "dump_log",
        "toc_path",
        "toc_sha256",
        "toc_log",
        "restore_proof",
    }
    if not isinstance(evidence, dict) or set(evidence) != expected_fields:
        raise DeployError("backup/restore evidence contract is malformed")
    if (
        evidence.get("dump_path") != plan["backup_path"]
        or evidence.get("toc_path") != plan["backup_toc_path"]
    ):
        raise DeployError("backup/restore evidence paths drifted from reviewed plan")
    dump_bytes = evidence.get("dump_bytes")
    if (
        not isinstance(dump_bytes, int)
        or isinstance(dump_bytes, bool)
        or dump_bytes <= 0
    ):
        raise DeployError("backup/restore evidence has an invalid dump size")
    for key in ("dump_sha256", "toc_sha256"):
        _require_digest(evidence.get(key), f"backup evidence {key}")
    for path_key, sha_key in (("dump_path", "dump_sha256"), ("toc_path", "toc_sha256")):
        artifact = Path(str(evidence[path_key]))
        _require_readable_regular(artifact, f"backup evidence {path_key}")
        details = artifact.stat()
        if (
            details.st_uid != os.geteuid()
            or stat.S_IMODE(details.st_mode) != 0o400
            or _file_sha256(artifact) != evidence[sha_key]
        ):
            raise DeployError(f"backup evidence {path_key} artifact drifted")
    if Path(str(evidence["dump_path"])).stat().st_size != dump_bytes:
        raise DeployError("backup evidence dump size drifted")
    for key in ("dump_log", "toc_log"):
        log = evidence.get(key)
        if not isinstance(log, dict) or set(log) != {"path", "sha256"}:
            raise DeployError(f"backup evidence {key} reference is malformed")
        _validate_owned_regular_hash(
            Path(str(log["path"])), log["sha256"], label=f"backup evidence {key}"
        )
    restore = evidence.get("restore_proof")
    expected_restore_fields = {
        "passed",
        "container",
        "image",
        "network",
        "start_log",
        "ready_log",
        "restore_log",
        "verify_log",
        "cleanup_log",
    }
    if (
        not isinstance(restore, dict)
        or set(restore) != expected_restore_fields
        or restore.get("passed") is not True
        or restore.get("container") != plan["restore_drill"]["container"]
        or restore.get("image") != plan["postgres_image"]
        or restore.get("network") != "none"
    ):
        raise DeployError("backup evidence lacks the exact full restore proof")
    for key in ("start_log", "ready_log", "restore_log", "verify_log", "cleanup_log"):
        log = restore.get(key)
        if not isinstance(log, dict) or set(log) != {"path", "sha256"}:
            raise DeployError(f"restore proof {key} reference is malformed")
        _validate_owned_regular_hash(
            Path(str(log["path"])), log["sha256"], label=f"restore proof {key}"
        )
    return json.loads(json.dumps(evidence))


def _backup_and_restore(context: ExecutionContext) -> dict[str, object]:
    action = "backup_restore_proof"
    if _action_succeeded(context.transition, action):
        succeeded = [
            event
            for event in _action_events(context.transition, action)
            if event["event"] == "action_succeeded"
        ]
        return _validated_backup_evidence(
            context.plan, succeeded[-1].get("detail", {}).get("evidence")
        )
    attempt = _next_action_attempt(context.transition, action)
    backup_path = Path(str(context.plan["backup_path"]))
    toc_path = Path(str(context.plan["backup_toc_path"]))
    commands = context.plan["commands"]
    if not isinstance(commands, dict):
        raise DeployError("reviewed deploy commands are malformed")
    action_events = _action_events(context.transition, action)
    started_events = [
        event for event in action_events if event["event"] == "action_started"
    ]
    if toc_path.exists() and not backup_path.exists():
        raise DeployError("uncheckpointed backup TOC exists without its dump")
    if backup_path.exists():
        if not started_events:
            raise DeployError(
                "uncheckpointed final backup has no journaled source action"
            )
        latest_start = max(
            started_events,
            key=lambda event: int(event.get("detail", {}).get("attempt", 0)),
        )
        latest_detail = latest_start.get("detail", {})
        prior_attempt = int(
            latest_detail.get("source_attempt", latest_detail.get("attempt", 0))
        )
        if prior_attempt <= 0:
            raise DeployError("uncheckpointed backup source attempt is malformed")
        context.transition_event(
            "action_started",
            detail={
                "action": action,
                "attempt": attempt,
                "recovery": True,
                "source_attempt": prior_attempt,
            },
        )
        started_monotonic = time.monotonic()
        try:
            if not toc_path.exists():
                prior_toc = toc_path.parent / (
                    f".{toc_path.name}.attempt-{prior_attempt:03d}.partial"
                )
                _require_readable_regular(prior_toc, "started-only backup TOC")
                prior_toc_details = prior_toc.stat()
                if (
                    prior_toc_details.st_uid != os.geteuid()
                    or stat.S_IMODE(prior_toc_details.st_mode) != 0o400
                    or prior_toc_details.st_size <= 0
                ):
                    raise DeployError("started-only backup TOC is unsafe")
                os.replace(prior_toc, toc_path)
            for artifact in (backup_path, toc_path):
                _require_readable_regular(artifact, "started-only backup artifact")
                details = artifact.stat()
                if (
                    details.st_uid != os.geteuid()
                    or stat.S_IMODE(details.st_mode) != 0o400
                    or details.st_size <= 0
                ):
                    raise DeployError("started-only backup artifact is unsafe")
            prior_dump_log = (
                context.log_root / f"backup-attempt-{prior_attempt:03d}.log"
            )
            prior_toc_log = (
                context.log_root / f"backup-toc-attempt-{prior_attempt:03d}.log"
            )
            for log in (prior_dump_log, prior_toc_log):
                _require_readable_regular(log, "started-only backup process log")
                details = log.stat()
                if (
                    details.st_uid != os.geteuid()
                    or stat.S_IMODE(details.st_mode) != 0o400
                ):
                    raise DeployError("started-only backup process log is unsafe")
            validation_toc = toc_path.parent / (
                f".{toc_path.name}.revalidate-{attempt:03d}.partial"
            )
            validation_log = (
                context.log_root / f"backup-revalidate-attempt-{attempt:03d}.log"
            )
            toc_result = _run_process(
                context,
                commands["archive_list"],
                operation="published metadb backup TOC revalidation",
                log_path=validation_log,
                timeout_seconds=context.budget.require("backup TOC revalidation"),
                stdin_path=backup_path,
                stdout_path=validation_toc,
            )
            if validation_toc.stat().st_size <= 0:
                raise DeployError("published metadb backup TOC revalidation is empty")
            if _file_sha256(validation_toc) != _file_sha256(toc_path):
                raise DeployError("published metadb backup TOC revalidation drifted")
            restore = _restore_drill(context, backup_path, attempt=attempt)
            evidence = {
                "dump_path": str(backup_path),
                "dump_sha256": _file_sha256(backup_path),
                "dump_bytes": backup_path.stat().st_size,
                "dump_log": {
                    "path": str(prior_dump_log),
                    "sha256": _file_sha256(prior_dump_log),
                },
                "toc_path": str(toc_path),
                "toc_sha256": _file_sha256(toc_path),
                "toc_log": {
                    "path": str(validation_log),
                    "sha256": toc_result.log_sha256,
                },
                "restore_proof": restore,
            }
        except BaseException as exc:
            context.transition_event(
                "action_failed",
                detail={
                    "action": action,
                    "attempt": attempt,
                    "duration_seconds": round(
                        max(0.0, time.monotonic() - started_monotonic), 3
                    ),
                    "error_type": type(exc).__name__,
                },
            )
            raise
        context.transition_event(
            "action_succeeded",
            detail={
                "action": action,
                "attempt": attempt,
                "duration_seconds": round(
                    max(0.0, time.monotonic() - started_monotonic), 3
                ),
                "evidence_sha256": canonical_sha256(evidence),
                "evidence": evidence,
                "recovered_started_attempt": prior_attempt,
            },
        )
        return _validated_backup_evidence(context.plan, evidence)
    # Keep temporary artifacts on the backup filesystem so the final publish
    # is an atomic same-filesystem rename even when state and backups are
    # deliberately mounted on separate durable volumes.
    temporary_backup = backup_path.parent / (
        f".{backup_path.name}.attempt-{attempt:03d}.partial"
    )
    temporary_toc = toc_path.parent / (
        f".{toc_path.name}.attempt-{attempt:03d}.partial"
    )
    context.transition_event(
        "action_started",
        detail={"action": action, "attempt": attempt},
    )
    started = time.monotonic()
    try:
        dump_log = context.log_root / f"backup-attempt-{attempt:03d}.log"
        dump_result = _run_process(
            context,
            commands["backup"],
            operation="metadb backup",
            log_path=dump_log,
            timeout_seconds=context.budget.require("metadb backup"),
            stdout_path=temporary_backup,
        )
        if temporary_backup.stat().st_size <= 0:
            raise DeployError("metadb backup is empty")
        toc_log = context.log_root / f"backup-toc-attempt-{attempt:03d}.log"
        toc_result = _run_process(
            context,
            commands["archive_list"],
            operation="metadb backup TOC validation",
            log_path=toc_log,
            timeout_seconds=context.budget.require("backup TOC validation"),
            stdin_path=temporary_backup,
            stdout_path=temporary_toc,
        )
        if temporary_toc.stat().st_size <= 0:
            raise DeployError("metadb backup TOC is empty")
        restore = _restore_drill(context, temporary_backup, attempt=attempt)
        os.replace(temporary_backup, backup_path)
        os.replace(temporary_toc, toc_path)
        os.chmod(backup_path, 0o400)
        os.chmod(toc_path, 0o400)
        directory_descriptor = os.open(backup_path.parent, os.O_RDONLY | os.O_DIRECTORY)
        try:
            os.fsync(directory_descriptor)
        finally:
            os.close(directory_descriptor)
        evidence = {
            "dump_path": str(backup_path),
            "dump_sha256": _file_sha256(backup_path),
            "dump_bytes": backup_path.stat().st_size,
            "dump_log": {"path": str(dump_log), "sha256": dump_result.log_sha256},
            "toc_path": str(toc_path),
            "toc_sha256": _file_sha256(toc_path),
            "toc_log": {"path": str(toc_log), "sha256": toc_result.log_sha256},
            "restore_proof": restore,
        }
    except BaseException as exc:
        context.transition_event(
            "action_failed",
            detail={
                "action": action,
                "attempt": attempt,
                "duration_seconds": round(max(0.0, time.monotonic() - started), 3),
                "error_type": type(exc).__name__,
            },
        )
        raise
    context.transition_event(
        "action_succeeded",
        detail={
            "action": action,
            "attempt": attempt,
            "duration_seconds": round(max(0.0, time.monotonic() - started), 3),
            "evidence_sha256": canonical_sha256(evidence),
            "evidence": evidence,
        },
    )
    return _validated_backup_evidence(context.plan, evidence)


def _validate_transition_action_evidence(
    plan: Mapping[str, object], transition: Mapping[str, object]
) -> None:
    value = _validated_transition_journal(transition)
    action_order = (
        "compose_config",
        "backup_restore_proof",
        "airflow_init",
        "airflow_recreate",
    )
    named_actions = {"compose_config", "airflow_init", "airflow_recreate"}
    introduced: list[str] = []
    succeeded: set[str] = set()
    attempts: dict[tuple[str, int], list[str]] = {}
    starts: dict[tuple[str, int], dict[str, object]] = {}
    for event in value["events"]:
        event_name = event["event"]
        if event_name not in {"action_started", "action_succeeded", "action_failed"}:
            continue
        detail = event.get("detail")
        if not isinstance(detail, dict):
            raise DeployError("transition action detail is malformed")
        action = detail.get("action")
        attempt = detail.get("attempt")
        if action not in action_order:
            raise DeployError("transition contains an unknown deploy action")
        if not isinstance(attempt, int) or isinstance(attempt, bool) or attempt <= 0:
            raise DeployError("transition action attempt is malformed")
        action_name = str(action)
        key = (action_name, attempt)
        statuses = attempts.setdefault(key, [])
        if action_name in succeeded:
            raise DeployError("completed transition action identity was extended")
        if event_name == "action_started":
            if action_name not in introduced:
                expected_action = action_order[len(introduced)]
                if action_name != expected_action or (
                    introduced and introduced[-1] not in succeeded
                ):
                    raise DeployError("transition deploy actions are out of order")
                introduced.append(action_name)
            expected_attempt = 1 + max(
                (
                    prior_attempt
                    for prior_action, prior_attempt in attempts
                    if prior_action == action_name and prior_attempt != attempt
                ),
                default=0,
            )
            if statuses or attempt != expected_attempt:
                raise DeployError("transition action started identity drifted")
            if action_name in named_actions:
                if set(detail) != {"action", "attempt", "log_path"}:
                    raise DeployError("named action start schema drifted")
                expected_log = (
                    Path(str(plan["state_root"]))
                    / "logs"
                    / f"action-{action_name}-attempt-{attempt:03d}.log"
                )
                if detail.get("log_path") != str(expected_log):
                    raise DeployError("named action start log path drifted")
            elif set(detail) == {"action", "attempt"}:
                pass
            elif set(detail) == {
                "action",
                "attempt",
                "recovery",
                "source_attempt",
            }:
                source_attempt = detail.get("source_attempt")
                if (
                    detail.get("recovery") is not True
                    or not isinstance(source_attempt, int)
                    or isinstance(source_attempt, bool)
                    or source_attempt <= 0
                    or source_attempt >= attempt
                ):
                    raise DeployError("backup recovery action identity drifted")
                source = starts.get(("backup_restore_proof", source_attempt))
                if source is None or set(source) != {"action", "attempt"}:
                    raise DeployError(
                        "backup recovery source is not an exact original start"
                    )
            else:
                raise DeployError("backup action start schema drifted")
            starts[key] = dict(detail)
        else:
            if statuses != ["action_started"]:
                raise DeployError("transition action terminal lacks exact start")
            duration = detail.get("duration_seconds")
            if (
                not isinstance(duration, (int, float))
                or isinstance(duration, bool)
                or duration < 0
            ):
                raise DeployError("transition action duration is malformed")
            if event_name == "action_failed":
                if set(detail) != {
                    "action",
                    "attempt",
                    "duration_seconds",
                    "error_type",
                } or not isinstance(detail.get("error_type"), str):
                    raise DeployError("failed action evidence schema drifted")
            elif action_name in named_actions:
                if set(detail) != {
                    "action",
                    "attempt",
                    "duration_seconds",
                    "log_path",
                    "log_sha256",
                }:
                    raise DeployError("successful named action schema drifted")
                expected_log = (
                    Path(str(plan["state_root"]))
                    / "logs"
                    / f"action-{action_name}-attempt-{attempt:03d}.log"
                )
                if detail.get("log_path") != str(expected_log):
                    raise DeployError("successful named action log path drifted")
                _validate_owned_regular_hash(
                    expected_log,
                    detail.get("log_sha256"),
                    label=f"successful {action_name} action log",
                )
                succeeded.add(action_name)
            else:
                start_detail = starts[key]
                start_is_recovery = start_detail.get("recovery") is True
                expected_fields = {
                    "action",
                    "attempt",
                    "duration_seconds",
                    "evidence_sha256",
                    "evidence",
                }
                if start_is_recovery:
                    expected_fields.add("recovered_started_attempt")
                if set(detail) != expected_fields:
                    raise DeployError("successful backup action schema drifted")
                evidence = detail.get("evidence")
                if canonical_sha256(evidence) != detail.get("evidence_sha256"):
                    raise DeployError("successful backup action evidence SHA drifted")
                if start_is_recovery:
                    recovered_attempt = detail.get("recovered_started_attempt")
                    if (
                        not isinstance(recovered_attempt, int)
                        or isinstance(recovered_attempt, bool)
                        or recovered_attempt != start_detail.get("source_attempt")
                    ):
                        raise DeployError("successful backup recovery identity drifted")
                _validated_backup_evidence(plan, evidence)
                succeeded.add(action_name)
        statuses.append(event_name)


def _validate_guard_runtime(plan: Mapping[str, object]) -> list[str]:
    guard = plan.get("guard")
    if not isinstance(guard, dict) or set(guard) != {
        "argv",
        "artifacts",
        "executable",
    }:
        raise DeployError("reviewed guard contract is malformed")
    argv = guard.get("argv")
    if not isinstance(argv, list) or not argv:
        raise DeployError("reviewed guard argv is malformed")
    if guard.get("executable") != _executable_identity(argv[0]):
        raise DeployError("reviewed guard executable identity drifted")
    artifacts = guard["artifacts"]
    if not isinstance(artifacts, list) or not artifacts:
        raise DeployError("reviewed guard artifact set is empty")
    artifact_paths: list[str] = []
    for item in artifacts:
        if not isinstance(item, dict) or set(item) != {"path", "sha256"}:
            raise DeployError("reviewed guard artifact identity is malformed")
        path = Path(str(item["path"]))
        _require_readable_regular(path, "reviewed guard artifact")
        if _file_sha256(path) != item["sha256"]:
            raise DeployError(f"reviewed guard artifact bytes drifted: {path}")
        artifact_paths.append(str(path))
    return artifact_paths


def _validate_runtime_input_bytes(plan: Mapping[str, object]) -> None:
    """Rebind every mutable filesystem input consumed by Compose/runtime."""

    root = Path(str(plan["release_root"]))
    if release_tree_sha256(root) != plan["release_tree_sha256"]:
        raise DeployError("release filesystem tree drifted after plan review")
    if dagbag_sha256(Path(str(plan["dagbag_root"]))) != plan["dagbag_sha256"]:
        raise DeployError("DagBag projection drifted after plan review")
    compose = Path(str(plan["compose_file"]))
    _require_readable_regular(compose, "reviewed Compose file")
    if _file_sha256(compose) != plan["compose_sha256"]:
        raise DeployError("reviewed Compose bytes drifted")
    env_file = Path(str(plan["env_file"]))
    _require_readable_regular(env_file, "protected Compose env file")
    env_details = env_file.stat()
    if (
        env_details.st_uid not in {0, os.geteuid()}
        or stat.S_IMODE(env_details.st_mode) & 0o077
    ):
        raise DeployError("protected Compose env file ownership/mode is unsafe")


def _validate_static_plan(plan: Mapping[str, object]) -> None:
    operator_path = Path(str(plan["operator_path"]))
    if operator_path != Path(__file__).resolve():
        raise DeployError("reviewed deploy operator path drifted")
    _require_readable_regular(operator_path, "reviewed deploy operator")
    if _file_sha256(operator_path) != plan["operator_sha256"]:
        raise DeployError("reviewed deploy operator bytes drifted")
    _validate_runtime_input_bytes(plan)
    guard = plan["guard"]
    artifact_paths = _validate_guard_runtime(plan)
    rebuilt = build_plan(
        {
            "transition_id": plan["transition_id"],
            "release_commit": plan["release_commit"],
            "release_tree_sha256": plan["release_tree_sha256"],
            "release_root": plan["release_root"],
            "dagbag_root": plan["dagbag_root"],
            "compose_file": plan["compose_file"],
            "env_file": plan["env_file"],
            "stack_lock_root": plan["stack_lock_root"],
            "state_root": plan["state_root"],
            "backup_root": plan["backup_root"],
            "airflow_image": plan["airflow_image"],
            "postgres_image": plan["postgres_image"],
            "layout_mode": plan["layout_mode"],
            "guard_argv": guard["argv"],
            "guard_artifacts": artifact_paths,
        }
    )
    if rebuilt != dict(plan):
        raise DeployError("reviewed deploy runtime contract drifted from plan builder")


def _run_verified_compose_action(
    context: ExecutionContext,
    action: str,
    argv: Sequence[str],
    *,
    env: Mapping[str, str],
) -> ProcessResult | None:
    context.budget.require(f"{action} runtime-input revalidation")
    _validate_transition_action_evidence(context.plan, context.transition)
    _validate_runtime_input_bytes(context.plan)
    context.budget.require(f"{action} runtime-input revalidation completion")
    return _run_named_action(context, action, argv, env=env)


@contextmanager
def _acquire_lock(state_root: Path) -> Iterator[TextIO]:
    path = state_root / ".deploy.lock"
    descriptor = os.open(path, os.O_RDWR | os.O_CREAT | os.O_NOFOLLOW, 0o600)
    handle = os.fdopen(descriptor, "a+", encoding="utf-8")
    try:
        details = os.fstat(handle.fileno())
        if (
            not stat.S_ISREG(details.st_mode)
            or details.st_uid != os.geteuid()
            or details.st_nlink != 1
            or stat.S_IMODE(details.st_mode) != 0o600
        ):
            raise DeployError("deployment lock is not an owned regular file")
        try:
            fcntl.flock(handle.fileno(), fcntl.LOCK_EX | fcntl.LOCK_NB)
        except BlockingIOError as exc:
            raise DeployError(
                "another ESPN deploy invocation holds the transition lock"
            ) from exc
        yield handle
    finally:
        handle.close()


def _context_paths(plan: Mapping[str, object]) -> dict[str, Path]:
    state_root = Path(str(plan["state_root"]))
    return {
        "stack_lock_root": Path(str(plan["stack_lock_root"])),
        "state_root": state_root,
        "log_root": state_root / "logs",
        "transition": state_root / "transition-journal.json",
        "guards": state_root / "guard-attempt-journal.json",
        "heartbeat": state_root / "heartbeat.json",
        "checkpoint": state_root / "checkpoint.json",
        "result": state_root / "result.json",
    }


def _load_or_initialize_context(
    plan: dict[str, object],
    mode: str,
    *,
    clock: Callable[[], float],
    sleeper: Callable[[float], None],
    apply_started_epoch: float | None = None,
    invocation_started_monotonic: float | None = None,
) -> ExecutionContext:
    paths = _context_paths(plan)
    _ensure_private_directory(paths["state_root"])
    _ensure_private_directory(paths["log_root"])
    _ensure_private_directory(Path(str(plan["backup_root"])))
    transition_exists = (
        paths["transition"].exists()
        or paths["transition"].with_name(paths["transition"].name + ".sha256").exists()
    )
    guard_exists = (
        paths["guards"].exists()
        or paths["guards"].with_name(paths["guards"].name + ".sha256").exists()
    )
    if mode == "resume" and not transition_exists and guard_exists:
        initial_guards = _validated_guard_journal(
            _read_checksummed_json(paths["guards"], repair_valid_main_first_crash=True)
        )
        budget_started_at = initial_guards.get("budget_started_at")
        if (
            initial_guards.get("transition_id") != plan["transition_id"]
            or initial_guards.get("plan_sha256") != plan["plan_sha256"]
            or initial_guards.get("events") != []
            or not isinstance(budget_started_at, str)
        ):
            raise DeployError(
                "guard-first initialization crash is not an exact empty journal"
            )
        _parse_utc(budget_started_at)
        initial_transition = new_transition_journal(plan, budget_started_at)
        initial_transition["guard_attempt_journal_sha256"] = initial_guards[
            "journal_sha256"
        ]
        initial_transition = _seal_transition(initial_transition)
        _write_checksummed_json(paths["transition"], initial_transition)
        transition_exists = True
    if mode == "apply":
        if (
            transition_exists
            or guard_exists
            or paths["checkpoint"].exists()
            or paths["checkpoint"]
            .with_name(paths["checkpoint"].name + ".sha256")
            .exists()
            or paths["result"].exists()
            or paths["result"].with_name(paths["result"].name + ".sha256").exists()
            or paths["heartbeat"].exists()
            or paths["heartbeat"]
            .with_name(paths["heartbeat"].name + ".sha256")
            .exists()
        ):
            raise DeployError(
                "apply requires a fresh transition; use resume for existing state"
            )
        started_at = _utc_now(
            clock if apply_started_epoch is None else lambda: apply_started_epoch
        )
        transition = new_transition_journal(plan, started_at)
        guards = new_guard_journal(plan, budget_started_at=started_at)
        transition["guard_attempt_journal_sha256"] = guards["journal_sha256"]
        transition = _seal_transition(transition)
        _write_checksummed_json(paths["guards"], guards)
        _write_checksummed_json(paths["transition"], transition)
    elif mode == "resume":
        if not transition_exists or not guard_exists:
            raise DeployError(
                "resume requires both durable transition and guard journals"
            )
        transition = _validated_transition_journal(
            _read_checksummed_json(
                paths["transition"], repair_valid_main_first_crash=True
            )
        )
        guards = _validated_guard_journal(
            _read_checksummed_json(paths["guards"], repair_valid_main_first_crash=True)
        )
        if (
            transition.get("transition_id") != plan["transition_id"]
            or transition.get("plan_sha256") != plan["plan_sha256"]
            or guards.get("transition_id") != plan["transition_id"]
            or guards.get("plan_sha256") != plan["plan_sha256"]
            or guards.get("budget_started_at") != transition.get("budget_started_at")
        ):
            raise DeployError(
                "resume journal identity does not match the exact reviewed plan"
            )
        transition, guards, repaired = _reconcile_torn_cross_binding(transition, guards)
        if repaired:
            _write_checksummed_json(paths["transition"], transition)
    else:
        raise DeployError(f"unsupported mutation mode: {mode}")
    return ExecutionContext(
        plan=plan,
        state_root=paths["state_root"],
        log_root=paths["log_root"],
        transition_path=paths["transition"],
        guard_path=paths["guards"],
        heartbeat_path=paths["heartbeat"],
        transition=transition,
        guards=guards,
        budget=Budget(
            _parse_utc(transition["budget_started_at"]),
            clock=clock,
            monotonic_clock=time.monotonic,
            invocation_started_epoch=apply_started_epoch,
            invocation_started_monotonic=invocation_started_monotonic,
        ),
        clock=clock,
        sleeper=sleeper,
    )


def _checkpoint_payload(
    context: ExecutionContext, backup_evidence: Mapping[str, object]
) -> dict[str, object]:
    return {
        "kind": CHECKPOINT_VERSION,
        "transition_id": context.plan["transition_id"],
        "plan_sha256": context.plan["plan_sha256"],
        "release_commit": context.plan["release_commit"],
        "release_tree_sha256": context.plan["release_tree_sha256"],
        "transition_event_sha256": transition_tail_sha256(context.transition),
        "guard_attempt_journal_sha256": context.guards["journal_sha256"],
        "backup": dict(backup_evidence),
        "sealed_at": _utc_now(context.clock),
    }


def _read_immutable_document(
    path: Path, *, label: str
) -> tuple[dict[str, object], str]:
    _require_readable_regular(path, label)
    details = path.stat()
    if details.st_uid != os.geteuid() or stat.S_IMODE(details.st_mode) != 0o400:
        raise DeployError(f"{label} ownership/mode drifted")
    payload = path.read_bytes()
    digest = hashlib.sha256(payload).hexdigest()
    try:
        value = json.loads(payload)
    except (UnicodeDecodeError, json.JSONDecodeError) as exc:
        raise DeployError(f"{label} JSON is invalid") from exc
    if not isinstance(value, dict) or payload != canonical_bytes(value):
        raise DeployError(f"{label} is not canonical")
    return value, digest


def _validate_checkpoint_document(
    value: Mapping[str, object], plan: Mapping[str, object]
) -> None:
    expected_fields = {
        "kind",
        "transition_id",
        "plan_sha256",
        "release_commit",
        "release_tree_sha256",
        "transition_event_sha256",
        "guard_attempt_journal_sha256",
        "backup",
        "sealed_at",
    }
    if (
        set(value) != expected_fields
        or value.get("kind") != CHECKPOINT_VERSION
        or value.get("transition_id") != plan["transition_id"]
        or value.get("plan_sha256") != plan["plan_sha256"]
        or value.get("release_commit") != plan["release_commit"]
        or value.get("release_tree_sha256") != plan["release_tree_sha256"]
    ):
        raise DeployError("immutable deployment checkpoint identity drifted")
    _require_digest(
        value.get("transition_event_sha256"), "checkpoint transition event SHA-256"
    )
    _require_digest(
        value.get("guard_attempt_journal_sha256"),
        "checkpoint guard-attempt journal SHA-256",
    )
    _parse_utc(value.get("sealed_at"))
    _validated_backup_evidence(plan, value.get("backup"))


def _validate_result_document(
    value: Mapping[str, object], plan: Mapping[str, object]
) -> None:
    expected_fields = {
        "kind",
        "status",
        "transition_id",
        "plan_sha256",
        "release_commit",
        "release_tree_sha256",
        "checkpoint_sha256",
        "guard_attempt_journal_sha256",
        "heartbeat_path",
        "heartbeat_sha256",
        "completed_at",
    }
    if (
        set(value) != expected_fields
        or value.get("kind") != RESULT_VERSION
        or value.get("status") != "deployed"
        or value.get("transition_id") != plan["transition_id"]
        or value.get("plan_sha256") != plan["plan_sha256"]
        or value.get("release_commit") != plan["release_commit"]
        or value.get("release_tree_sha256") != plan["release_tree_sha256"]
    ):
        raise DeployError("immutable deployment result identity drifted")
    _require_digest(value.get("checkpoint_sha256"), "result checkpoint SHA-256")
    _require_digest(
        value.get("guard_attempt_journal_sha256"),
        "result guard-attempt journal SHA-256",
    )
    if value.get("heartbeat_path") != str(
        Path(str(plan["state_root"])) / "heartbeat.json"
    ):
        raise DeployError("immutable deployment result heartbeat path drifted")
    _require_digest(value.get("heartbeat_sha256"), "result heartbeat SHA-256")
    _parse_utc(value.get("completed_at"))


def _require_immutable_sidecar(path: Path, digest: str, *, label: str) -> None:
    checksum_path = path.with_name(path.name + ".sha256")
    _require_readable_regular(checksum_path, f"{label} checksum")
    details = checksum_path.stat()
    if details.st_uid != os.geteuid() or stat.S_IMODE(details.st_mode) != 0o400:
        raise DeployError(f"{label} checksum ownership/mode drifted")
    if checksum_path.read_bytes() != f"{digest}  {path.name}\n".encode("ascii"):
        raise DeployError(f"{label} checksum drifted")


def _repair_valid_immutable_main_first_crash(
    path: Path, plan: Mapping[str, object], *, kind: str
) -> bool:
    checksum_path = path.with_name(path.name + ".sha256")
    if not path.exists() or checksum_path.exists():
        return False
    label = f"immutable deployment {kind}"
    value, digest = _read_immutable_document(path, label=label)
    if kind == "checkpoint":
        _validate_checkpoint_document(value, plan)
    elif kind == "result":
        _validate_result_document(value, plan)
    else:
        raise DeployError("unsupported immutable sidecar recovery kind")
    _exclusive_regular_write(checksum_path, f"{digest}  {path.name}\n".encode("ascii"))
    return True


def _read_checkpoint(path: Path, plan: Mapping[str, object]) -> dict[str, object]:
    value, digest = _read_immutable_document(
        path, label="immutable deployment checkpoint"
    )
    _require_immutable_sidecar(path, digest, label="immutable deployment checkpoint")
    _validate_checkpoint_document(value, plan)
    return value


def _guard_prefix_for_sha256(
    guards: Mapping[str, object], target_sha256: object
) -> dict[str, object]:
    target = _require_digest(target_sha256, "checkpoint guard journal SHA-256")
    value = _validated_guard_journal(guards)
    prefix: dict[str, object] = {
        "kind": value["kind"],
        "transition_id": value["transition_id"],
        "plan_sha256": value["plan_sha256"],
        "budget_started_at": value.get("budget_started_at"),
        "events": [],
    }
    sealed = _seal_guards(prefix)
    if sealed["journal_sha256"] == target:
        return sealed
    for event in value["events"]:
        prefix["events"] = [*prefix["events"], event]
        sealed = _seal_guards(prefix)
        if sealed["journal_sha256"] == target:
            return sealed
    raise DeployError("checkpoint guard journal SHA is not an exact durable prefix")


def _ensure_checkpoint_journal_binding(
    context: ExecutionContext, checkpoint: Mapping[str, object]
) -> None:
    transition = _validated_transition_journal(context.transition)
    guard_prefix = _guard_prefix_for_sha256(
        context.guards, checkpoint["guard_attempt_journal_sha256"]
    )
    if not guard_phase_succeeded(guard_prefix, "pre_checkpoint_mutation"):
        raise DeployError("checkpoint guard prefix lacks pre-checkpoint success")
    if any(
        event["phase"] in {"pre_airflow_init", "pre_recreate", "post_deploy"}
        for event in guard_prefix["events"]
    ):
        raise DeployError("checkpoint guard prefix contains post-checkpoint phases")
    transition_sha = str(checkpoint["transition_event_sha256"])
    referenced = [
        event
        for event in transition["events"]
        if event["event_sha256"] == transition_sha
    ]
    if (
        len(referenced) != 1
        or referenced[0].get("event") != "guard_succeeded"
        or referenced[0].get("phase") != "pre_checkpoint_mutation"
    ):
        raise DeployError(
            "checkpoint transition event is not exact pre-checkpoint guard"
        )
    guard_hashes = {event["event_sha256"] for event in guard_prefix["events"]}
    if referenced[0].get("guard_event_sha256") not in guard_hashes:
        raise DeployError("checkpoint transition event is outside its guard prefix")
    checkpoint_sha = hashlib.sha256(canonical_bytes(dict(checkpoint))).hexdigest()
    markers = [
        event for event in transition["events"] if event["event"] == "checkpoint_sealed"
    ]
    if len(markers) > 1:
        raise DeployError("checkpoint has multiple transition markers")
    if markers:
        marker = markers[0]
        detail = marker.get("detail")
        if (
            marker.get("phase") != "pre_checkpoint_mutation"
            or marker.get("previous_event_sha256") != transition_sha
            or not isinstance(detail, dict)
            or detail.get("checkpoint_sha256") != checkpoint_sha
            or set(detail)
            not in (
                {"checkpoint_sha256"},
                {"checkpoint_sha256", "recovered"},
            )
            or ("recovered" in detail and detail["recovered"] is not True)
        ):
            raise DeployError("checkpoint transition marker binding drifted")
        return
    if (
        transition_tail_sha256(transition) != transition_sha
        or context.guards["journal_sha256"]
        != checkpoint["guard_attempt_journal_sha256"]
    ):
        raise DeployError("unmarked checkpoint is not at the current journal tails")
    context.transition_event(
        "checkpoint_sealed",
        phase="pre_checkpoint_mutation",
        detail={"checkpoint_sha256": checkpoint_sha, "recovered": True},
    )


def _completed(transition: Mapping[str, object]) -> bool:
    value = _validated_transition_journal(transition)
    return any(event["event"] == "deployed" for event in value["events"])


def _validate_deployed_transition(
    transition: Mapping[str, object], result_sha256: object
) -> None:
    result_sha = _require_digest(result_sha256, "sealed result SHA-256")
    value = _validated_transition_journal(transition)
    deployed = [event for event in value["events"] if event["event"] == "deployed"]
    if len(deployed) != 1 or deployed[0] is not value["events"][-1]:
        raise DeployError("deployed transition event must be unique and terminal")
    detail = deployed[0].get("detail")
    if (
        not isinstance(detail, dict)
        or detail.get("result_sha256") != result_sha
        or set(detail) not in ({"result_sha256"}, {"result_sha256", "recovered"})
        or ("recovered" in detail and detail["recovered"] is not True)
    ):
        raise DeployError("deployed transition result binding drifted")


def _read_result(
    path: Path, plan: Mapping[str, object]
) -> tuple[dict[str, object], str]:
    value, digest = _read_immutable_document(path, label="immutable deployment result")
    _require_immutable_sidecar(path, digest, label="immutable deployment result")
    _validate_result_document(value, plan)
    return value, digest


def execute_plan(
    plan: dict[str, object],
    *,
    mode: str,
    clock: Callable[[], float] = time.time,
    sleeper: Callable[[float], None] = time.sleep,
) -> dict[str, object]:
    """Apply or resume one exact reviewed plan; never used by ``plan``."""

    invocation_started_epoch = clock()
    invocation_started_monotonic = time.monotonic()
    plan = _validate_plan_document(plan)
    if mode not in {"apply", "resume"}:
        raise DeployError(f"unsupported mutation mode: {mode}")
    # Validate every deterministic command/path/artifact before creating the
    # state directory or lock.  The original invocation timestamp is then
    # persisted so this read-only preflight still consumes the total budget.
    _validate_static_plan(plan)
    if (
        time.monotonic() - invocation_started_monotonic >= TOTAL_TIMEOUT_SECONDS
        or clock() - invocation_started_epoch >= TOTAL_TIMEOUT_SECONDS
    ):
        raise DeployError("three-hour total deployment deadline expired in preflight")
    paths = _context_paths(plan)
    _ensure_private_directory(paths["stack_lock_root"])
    with _acquire_lock(paths["stack_lock_root"]):
        _ensure_private_directory(paths["state_root"])
        context = _load_or_initialize_context(
            plan,
            mode,
            clock=clock,
            sleeper=sleeper,
            apply_started_epoch=invocation_started_epoch,
            invocation_started_monotonic=invocation_started_monotonic,
        )
        context.budget.require("post-preflight mutation boundary")
        _validate_transition_action_evidence(plan, context.transition)
        heartbeat_reference = _heartbeat_evidence_reference(
            plan, paths["heartbeat"], required=False
        )
        if mode == "resume":
            _validate_physical_fingerprint_evidence(plan, context.guards)
            _repair_valid_immutable_main_first_crash(
                paths["checkpoint"], plan, kind="checkpoint"
            )
            _repair_valid_immutable_main_first_crash(
                paths["result"], plan, kind="result"
            )
        result_pair = (
            paths["result"].exists(),
            paths["result"].with_name(paths["result"].name + ".sha256").exists(),
        )
        if result_pair not in {(False, False), (True, True)}:
            raise DeployError("immutable deployment result is only partially sealed")
        sealed_result: dict[str, object] | None = None
        sealed_result_sha: str | None = None
        if result_pair == (True, True):
            sealed_result, sealed_result_sha = _read_result(paths["result"], plan)
            heartbeat_reference = _heartbeat_evidence_reference(
                plan, paths["heartbeat"], required=True
            )
            if heartbeat_reference != {
                "path": sealed_result["heartbeat_path"],
                "sha256": sealed_result["heartbeat_sha256"],
            }:
                raise DeployError("deployment result heartbeat binding drifted")
            result_checkpoint_pair = (
                paths["checkpoint"].exists(),
                paths["checkpoint"]
                .with_name(paths["checkpoint"].name + ".sha256")
                .exists(),
            )
            if result_checkpoint_pair != (True, True):
                raise DeployError("immutable result requires an exact checkpoint pair")
            result_checkpoint = _read_checkpoint(paths["checkpoint"], plan)
            if (
                sealed_result["checkpoint_sha256"]
                != hashlib.sha256(canonical_bytes(result_checkpoint)).hexdigest()
            ):
                raise DeployError("immutable result checkpoint binding drifted")
            _ensure_checkpoint_journal_binding(context, result_checkpoint)
            if (
                sealed_result["guard_attempt_journal_sha256"]
                != context.guards["journal_sha256"]
            ):
                raise DeployError("deployment result guard journal binding drifted")
        if _completed(context.transition):
            if sealed_result is None:
                raise DeployError(
                    "deployed transition has no immutable deployment result"
                )
            if sealed_result_sha is None:
                raise DeployError("sealed deployment result has no SHA-256")
            _validate_deployed_transition(context.transition, sealed_result_sha)
            validate_cross_bound_journals(context.transition, context.guards)
            return {
                "kind": RESULT_VERSION,
                "status": "already_converged",
                "transition_id": plan["transition_id"],
                "plan_sha256": plan["plan_sha256"],
            }
        if sealed_result is not None:
            # Lost response after the immutable result was sealed but before
            # the final transition event: append the missing event only.
            context.transition_event(
                "deployed",
                detail={"result_sha256": sealed_result_sha, "recovered": True},
            )
            _validate_deployed_transition(context.transition, sealed_result_sha)
            validate_cross_bound_journals(context.transition, context.guards)
            return sealed_result

        _run_guard_phase(context, "initial_state")
        commands = plan["commands"]
        if not isinstance(commands, dict):
            raise DeployError("reviewed command set is malformed")
        _run_verified_compose_action(
            context,
            "compose_config",
            commands["compose_config"],
            env=_compose_environment(plan),
        )
        _run_guard_phase(context, "pre_backup")

        checkpoint_pair = (
            paths["checkpoint"].exists(),
            paths["checkpoint"]
            .with_name(paths["checkpoint"].name + ".sha256")
            .exists(),
        )
        if checkpoint_pair == (True, True):
            checkpoint = _read_checkpoint(paths["checkpoint"], plan)
        elif checkpoint_pair != (False, False):
            raise DeployError("deployment checkpoint is only partially sealed")
        else:
            backup_evidence = _backup_and_restore(context)
            _run_guard_phase(context, "pre_checkpoint_mutation")
            checkpoint_body = _checkpoint_payload(context, backup_evidence)
            checkpoint_sha = _write_immutable_checksummed_json(
                paths["checkpoint"], checkpoint_body
            )
            context.transition_event(
                "checkpoint_sealed",
                phase="pre_checkpoint_mutation",
                detail={"checkpoint_sha256": checkpoint_sha},
            )
            checkpoint = _read_checkpoint(paths["checkpoint"], plan)

        _ensure_checkpoint_journal_binding(context, checkpoint)
        _run_guard_phase(context, "pre_airflow_init")
        _run_verified_compose_action(
            context,
            "airflow_init",
            commands["airflow_init"],
            env=_compose_environment(plan),
        )
        _run_guard_phase(context, "pre_recreate")
        _run_verified_compose_action(
            context,
            "airflow_recreate",
            commands["recreate"],
            env=_compose_environment(plan),
        )
        _run_guard_phase(context, "post_deploy")
        context.budget.require("final durable result")
        _validate_physical_fingerprint_evidence(plan, context.guards)
        validate_cross_bound_journals(context.transition, context.guards)
        _validate_runtime_input_bytes(plan)
        _validate_transition_action_evidence(plan, context.transition)
        heartbeat_reference = _heartbeat_evidence_reference(
            plan, paths["heartbeat"], required=True
        )
        if heartbeat_reference is None:
            raise DeployError("final deployment heartbeat evidence is missing")
        context.budget.require("final runtime-input revalidation completion")
        result = {
            "kind": RESULT_VERSION,
            "status": "deployed",
            "transition_id": plan["transition_id"],
            "plan_sha256": plan["plan_sha256"],
            "release_commit": plan["release_commit"],
            "release_tree_sha256": plan["release_tree_sha256"],
            "checkpoint_sha256": hashlib.sha256(
                canonical_bytes(checkpoint)
            ).hexdigest(),
            "guard_attempt_journal_sha256": context.guards["journal_sha256"],
            "heartbeat_path": heartbeat_reference["path"],
            "heartbeat_sha256": heartbeat_reference["sha256"],
            "completed_at": _utc_now(clock),
        }
        result_sha = _write_immutable_checksummed_json(paths["result"], result)
        context.transition_event("deployed", detail={"result_sha256": result_sha})
        _validate_deployed_transition(context.transition, result_sha)
        return result


def _plan_parser(
    subparsers: argparse._SubParsersAction[argparse.ArgumentParser],
) -> None:
    parser = subparsers.add_parser("plan", help="emit a strictly non-mutating plan")
    parser.add_argument("--transition-id", required=True)
    parser.add_argument("--release-commit", required=True)
    parser.add_argument("--release-tree-sha256", required=True)
    parser.add_argument("--release-root", type=Path, required=True)
    parser.add_argument("--dagbag-root", type=Path, required=True)
    parser.add_argument("--compose-file", type=Path, required=True)
    parser.add_argument("--env-file", type=Path, required=True)
    parser.add_argument("--stack-lock-root", type=Path, required=True)
    parser.add_argument("--state-root", type=Path, required=True)
    parser.add_argument("--backup-root", type=Path, required=True)
    parser.add_argument("--airflow-image", required=True)
    parser.add_argument("--postgres-image", required=True)
    parser.add_argument(
        "--layout-mode", choices=("legacy14", "compact6"), required=True
    )
    parser.add_argument(
        "--guard-argv-json",
        required=True,
        help="reviewed JSON argv array; no shell evaluation is performed",
    )
    parser.add_argument(
        "--guard-artifact",
        action="append",
        type=Path,
        required=True,
        help="file whose exact SHA-256 is sealed into the plan (repeatable)",
    )


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description=__doc__)
    subparsers = parser.add_subparsers(dest="mode", required=True)
    _plan_parser(subparsers)
    for mode in ("apply", "resume"):
        mutation = subparsers.add_parser(mode, help=f"{mode} an exact reviewed plan")
        mutation.add_argument("--plan", type=Path, required=True)
        mutation.add_argument("--plan-sha256", required=True)
    return parser


def _spec_from_args(args: argparse.Namespace) -> dict[str, object]:
    try:
        guard_argv = json.loads(args.guard_argv_json)
    except json.JSONDecodeError as exc:
        raise DeployError("--guard-argv-json is not valid JSON") from exc
    return {
        "transition_id": args.transition_id,
        "release_commit": args.release_commit,
        "release_tree_sha256": args.release_tree_sha256,
        "release_root": str(args.release_root),
        "dagbag_root": str(args.dagbag_root),
        "compose_file": str(args.compose_file),
        "env_file": str(args.env_file),
        "stack_lock_root": str(args.stack_lock_root),
        "state_root": str(args.state_root),
        "backup_root": str(args.backup_root),
        "airflow_image": args.airflow_image,
        "postgres_image": args.postgres_image,
        "layout_mode": args.layout_mode,
        "guard_argv": guard_argv,
        "guard_artifacts": [str(path) for path in args.guard_artifact],
    }


def main(argv: Sequence[str] | None = None) -> int:
    args = build_parser().parse_args(argv)
    try:
        if args.mode == "plan":
            # Keep this branch pure: no lock, mkdir, journal, guard, backup, or
            # Compose call is reachable from build_plan.
            emit_console(build_plan(_spec_from_args(args)))
            return 0
        plan = load_reviewed_plan(args.plan, args.plan_sha256)
        result = execute_plan(plan, mode=args.mode)
        emit_console(result)
        return 0
    except (DeployError, OSError, subprocess.SubprocessError) as exc:
        emit_console(
            {
                "kind": RESULT_VERSION,
                "status": "failed",
                "error": f"{type(exc).__name__}: {exc}",
            },
            stream=sys.stderr,
        )
        return 1


if __name__ == "__main__":
    raise SystemExit(main())
