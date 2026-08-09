#!/usr/bin/env python3
"""Versioned, strictly read-only ESPN release deployment quiescence guard.

The host process consumes only the identity injected by ``deploy/espn/deploy.py``
and an exact reviewed Docker executable.  It asks the digest-pinned ESPN metadata
container for one narrow PostgreSQL snapshot in an explicit read-only
transaction.  It owns no filesystem, database, Airflow, or container writer.

Only canonical, secret-safe JSON is written to stdout.  Child stderr, database
connection details, environment secrets, and raw exception messages are never
included in the report.
"""

from __future__ import annotations

import argparse
from collections.abc import Callable, Mapping, Sequence
from dataclasses import dataclass
import json
import os
from pathlib import Path
import re
import stat
import subprocess
import sys
import time
from typing import Any


GUARD_KIND = "espn-release-guard-v1"
SNAPSHOT_KIND = "espn-release-guard-snapshot-v1"
SCHEMA_VERSION = 1
EXPECTED_METADB_CONTAINER = "espn-airflow-airflow-metadb-1"
DEFAULT_DOCKER_PATH = Path("/usr/bin/docker")
QUERY_TIMEOUT_SECONDS = 30
DEFAULT_POLL_SECONDS = 15
MAX_POLL_SECONDS = 60
MAX_WAIT_SECONDS = 1_740

EXPECTED_DAG_IDS = (
    "dag_backfill_espn",
    "dag_discover_espn_registry",
    "dag_ingest_espn",
    "dag_monitor_espn",
    "dag_repair_espn",
    "dag_replay_espn",
    "dag_trigger_espn_daily",
)
EXPECTED_PHASES = (
    "initial_state",
    "pre_backup",
    "pre_checkpoint_mutation",
    "pre_airflow_init",
    "pre_recreate",
    "post_deploy",
)

_SHA256 = re.compile(r"[0-9a-f]{64}")
_TRANSITION_ID = re.compile(r"[a-z0-9][a-z0-9._-]{2,95}")
_ATTEMPT = re.compile(r"[1-9][0-9]*")

READONLY_SQL = """
BEGIN TRANSACTION READ ONLY;
SELECT json_build_object(
    'kind', 'espn-release-guard-snapshot-v1',
    'schema_version', 1,
    'transaction_read_only', current_setting('transaction_read_only'),
    'dag_ids', COALESCE(
        (SELECT json_agg(dag_id ORDER BY dag_id) FROM public.dag),
        '[]'::json
    ),
    'inactive_dag_ids', COALESCE(
        (
            SELECT json_agg(dag_id ORDER BY dag_id)
            FROM public.dag
            WHERE is_active IS DISTINCT FROM TRUE
        ),
        '[]'::json
    ),
    'unpaused_dag_ids', COALESCE(
        (
            SELECT json_agg(dag_id ORDER BY dag_id)
            FROM public.dag
            WHERE is_paused IS DISTINCT FROM TRUE
        ),
        '[]'::json
    ),
    'active_dagrun_count', (
        SELECT count(*)
        FROM public.dag_run
        WHERE state IN ('queued', 'running')
    ),
    'active_dag_ids', COALESCE(
        (
            SELECT json_agg(dag_id ORDER BY dag_id)
            FROM (
                SELECT DISTINCT dag_id
                FROM public.dag_run
                WHERE state IN ('queued', 'running')
            ) active
        ),
        '[]'::json
    )
);
ROLLBACK;
""".strip()


class GuardError(RuntimeError):
    """A fail-closed, secret-safe guard contract error."""

    def __init__(self, code: str, message: str):
        super().__init__(message)
        self.code = code


@dataclass(frozen=True, slots=True)
class DeployInvocation:
    phase: str
    attempt: int
    transition_id: str
    plan_sha256: str


def _string_list(value: object, field: str) -> tuple[str, ...]:
    if not isinstance(value, list) or any(
        not isinstance(item, str) or not item for item in value
    ):
        raise GuardError("snapshot_invalid", f"{field} must be a string list")
    items = tuple(value)
    if items != tuple(sorted(set(items))):
        raise GuardError("snapshot_invalid", f"{field} must be sorted and unique")
    return items


@dataclass(frozen=True, slots=True)
class GuardSnapshot:
    dag_ids: tuple[str, ...]
    inactive_dag_ids: tuple[str, ...]
    unpaused_dag_ids: tuple[str, ...]
    active_dagrun_count: int
    active_dag_ids: tuple[str, ...]
    transaction_read_only: str

    @classmethod
    def from_mapping(cls, value: object) -> "GuardSnapshot":
        expected_fields = {
            "kind",
            "schema_version",
            "transaction_read_only",
            "dag_ids",
            "inactive_dag_ids",
            "unpaused_dag_ids",
            "active_dagrun_count",
            "active_dag_ids",
        }
        if (
            not isinstance(value, Mapping)
            or set(value) != expected_fields
            or value.get("kind") != SNAPSHOT_KIND
            or type(value.get("schema_version")) is not int
            or value.get("schema_version") != SCHEMA_VERSION
            or value.get("transaction_read_only") not in {"on", "off"}
        ):
            raise GuardError("snapshot_invalid", "snapshot envelope is malformed")
        dag_ids = _string_list(value["dag_ids"], "dag_ids")
        inactive = _string_list(value["inactive_dag_ids"], "inactive_dag_ids")
        unpaused = _string_list(value["unpaused_dag_ids"], "unpaused_dag_ids")
        active_ids = _string_list(value["active_dag_ids"], "active_dag_ids")
        active_count = value["active_dagrun_count"]
        if (
            type(active_count) is not int
            or active_count < 0
            or not set(inactive).issubset(dag_ids)
            or not set(unpaused).issubset(dag_ids)
            or (active_count == 0) != (not active_ids)
            or active_count < len(active_ids)
        ):
            raise GuardError("snapshot_invalid", "snapshot values are inconsistent")
        return cls(
            dag_ids=dag_ids,
            inactive_dag_ids=inactive,
            unpaused_dag_ids=unpaused,
            active_dagrun_count=active_count,
            active_dag_ids=active_ids,
            transaction_read_only=str(value["transaction_read_only"]),
        )

    def checks(self) -> dict[str, bool]:
        return {
            "exact_dag_inventory": (
                self.dag_ids == EXPECTED_DAG_IDS and not self.inactive_dag_ids
            ),
            "all_dags_paused": not self.unpaused_dag_ids,
            "zero_active_dagruns": self.active_dagrun_count == 0,
            "transaction_read_only": self.transaction_read_only == "on",
        }


CommandRunner = Callable[..., subprocess.CompletedProcess[str]]


def canonical_bytes(value: object) -> bytes:
    try:
        encoded = json.dumps(
            value,
            ensure_ascii=False,
            sort_keys=True,
            separators=(",", ":"),
            allow_nan=False,
        )
    except (TypeError, ValueError) as exc:
        raise GuardError("report_invalid", "report is not canonical JSON") from exc
    return (encoded + "\n").encode("utf-8")


def load_invocation(environ: Mapping[str, str]) -> DeployInvocation:
    phase = environ.get("ESPN_DEPLOY_GUARD_PHASE", "")
    attempt_text = environ.get("ESPN_DEPLOY_GUARD_ATTEMPT", "")
    transition_id = environ.get("ESPN_DEPLOY_TRANSITION_ID", "")
    plan_sha256 = environ.get("ESPN_DEPLOY_PLAN_SHA256", "")
    if phase not in EXPECTED_PHASES:
        raise GuardError("invalid_invocation", "guard phase is not reviewed")
    if _ATTEMPT.fullmatch(attempt_text) is None:
        raise GuardError(
            "invalid_invocation", "guard attempt is not a positive integer"
        )
    if _TRANSITION_ID.fullmatch(transition_id) is None:
        raise GuardError("invalid_invocation", "transition ID is malformed")
    if _SHA256.fullmatch(plan_sha256) is None:
        raise GuardError("invalid_invocation", "plan SHA-256 is malformed")
    return DeployInvocation(
        phase=phase,
        attempt=int(attempt_text),
        transition_id=transition_id,
        plan_sha256=plan_sha256,
    )


def _validated_docker_path(value: str | Path) -> Path:
    path = Path(value)
    if path != DEFAULT_DOCKER_PATH:
        raise GuardError("docker_invalid", "Docker path differs from reviewed path")
    try:
        details = path.lstat()
    except OSError as exc:
        raise GuardError(
            "docker_invalid", "reviewed Docker executable is absent"
        ) from exc
    if (
        path.is_symlink()
        or not stat.S_ISREG(details.st_mode)
        or not os.access(path, os.X_OK)
        or details.st_uid != 0
        or stat.S_IMODE(details.st_mode) & 0o022
    ):
        raise GuardError("docker_invalid", "reviewed Docker executable is unsafe")
    return path


def _strict_json_loads(raw: str) -> Any:
    def object_without_duplicates(pairs):  # noqa: ANN001
        value: dict[str, Any] = {}
        for key, item in pairs:
            if key in value:
                raise GuardError("snapshot_invalid", "snapshot has duplicate fields")
            value[key] = item
        return value

    def reject_constant(_value: str) -> None:
        raise GuardError("snapshot_invalid", "snapshot has a non-finite number")

    try:
        return json.loads(
            raw,
            object_pairs_hook=object_without_duplicates,
            parse_constant=reject_constant,
        )
    except GuardError:
        raise
    except (json.JSONDecodeError, TypeError, ValueError) as exc:
        raise GuardError("snapshot_invalid", "snapshot is not strict JSON") from exc


def _run_command(
    argv: Sequence[str],
    *,
    capture_output: bool,
    text: bool,
    timeout: float,
    check: bool,
) -> subprocess.CompletedProcess[str]:
    return subprocess.run(
        list(argv),
        capture_output=capture_output,
        text=text,
        timeout=timeout,
        check=check,
        shell=False,
    )


def read_snapshot(
    docker_path: str | Path,
    *,
    run_command: CommandRunner = _run_command,
    timeout_seconds: float = QUERY_TIMEOUT_SECONDS,
) -> GuardSnapshot:
    if (
        isinstance(timeout_seconds, bool)
        or not isinstance(timeout_seconds, (int, float))
        or not 0 < timeout_seconds <= QUERY_TIMEOUT_SECONDS
    ):
        raise GuardError("invalid_invocation", "snapshot timeout is unsafe")
    docker = _validated_docker_path(docker_path)
    command = [
        str(docker),
        "exec",
        "--env",
        "PGOPTIONS=--default_transaction_read_only=on",
        EXPECTED_METADB_CONTAINER,
        "psql",
        "--username=airflow",
        "--dbname=airflow",
        "--no-password",
        "--no-psqlrc",
        "--quiet",
        "--tuples-only",
        "--no-align",
        "--set=ON_ERROR_STOP=1",
        "--command",
        READONLY_SQL,
    ]
    try:
        completed = run_command(
            command,
            capture_output=True,
            text=True,
            timeout=float(timeout_seconds),
            check=False,
        )
    except (OSError, subprocess.SubprocessError) as exc:
        raise GuardError(
            "snapshot_command_failed", "snapshot command was unavailable"
        ) from exc
    if completed.returncode != 0:
        raise GuardError("snapshot_command_failed", "snapshot command failed")
    lines = completed.stdout.splitlines()
    if len(lines) != 1 or not lines[0]:
        raise GuardError("snapshot_invalid", "snapshot output is not one JSON line")
    return GuardSnapshot.from_mapping(_strict_json_loads(lines[0]))


def _identity_report(invocation: DeployInvocation) -> dict[str, object]:
    return {
        "kind": GUARD_KIND,
        "schema_version": SCHEMA_VERSION,
        "phase": invocation.phase,
        "attempt": invocation.attempt,
        "transition_id": invocation.transition_id,
        "plan_sha256": invocation.plan_sha256,
    }


def _validate_limits(poll_seconds: int, max_wait_seconds: int) -> None:
    if (
        type(poll_seconds) is not int
        or not 1 <= poll_seconds <= MAX_POLL_SECONDS
        or type(max_wait_seconds) is not int
        or not 0 <= max_wait_seconds <= MAX_WAIT_SECONDS
    ):
        raise GuardError("invalid_invocation", "poll or wait limit is unsafe")


def run_guard(
    invocation: DeployInvocation,
    *,
    docker_path: str | Path,
    poll_seconds: int = DEFAULT_POLL_SECONDS,
    max_wait_seconds: int = MAX_WAIT_SECONDS,
    run_command: CommandRunner | None = None,
    clock: Callable[[], float] = time.monotonic,
    sleeper: Callable[[float], None] = time.sleep,
) -> dict[str, object]:
    """Poll the read-only snapshot until quiescent or the bounded deadline."""

    _validate_limits(poll_seconds, max_wait_seconds)
    command_runner = _run_command if run_command is None else run_command
    started = clock()
    deadline = started + max_wait_seconds
    last_error = "snapshot_unavailable"
    last_checks: dict[str, bool] | None = None
    first_query = True
    while True:
        remaining_before_query = deadline - clock()
        if not first_query and remaining_before_query <= 0:
            report: dict[str, object] = {
                **_identity_report(invocation),
                "status": "failed",
                "error_code": last_error,
            }
            if last_checks is not None:
                report["checks"] = last_checks
            return report
        # ``max_wait_seconds=0`` is a test/operator one-shot mode. Production
        # is pinned to 1,740 seconds; every production query consumes only the
        # budget remaining inside that whole-process deadline.
        query_timeout = (
            QUERY_TIMEOUT_SECONDS
            if max_wait_seconds == 0
            else min(float(QUERY_TIMEOUT_SECONDS), remaining_before_query)
        )
        try:
            snapshot = read_snapshot(
                docker_path,
                run_command=command_runner,
                timeout_seconds=query_timeout,
            )
        except GuardError as exc:
            last_error = exc.code
            last_checks = None
        else:
            last_checks = snapshot.checks()
            completed_within_deadline = max_wait_seconds == 0 or clock() <= deadline
            if all(last_checks.values()) and completed_within_deadline:
                return {
                    **_identity_report(invocation),
                    "status": "ok",
                    "checks": last_checks,
                }
            last_error = "quiescence_timeout"
        first_query = False
        remaining = deadline - clock()
        if remaining <= 0:
            report: dict[str, object] = {
                **_identity_report(invocation),
                "status": "failed",
                "error_code": last_error,
            }
            if last_checks is not None:
                report["checks"] = last_checks
            return report
        sleeper(min(float(poll_seconds), remaining))


class _GuardArgumentParser(argparse.ArgumentParser):
    def error(self, _message: str) -> None:
        raise GuardError("invalid_invocation", "guard arguments are malformed")


def build_parser() -> argparse.ArgumentParser:
    parser = _GuardArgumentParser(description=__doc__)
    commands = parser.add_subparsers(dest="command", required=True)
    guard = commands.add_parser("guard", help="wait for exact read-only quiescence")
    guard.add_argument("--docker-path", type=Path, required=True)
    guard.add_argument("--poll-seconds", type=int, default=DEFAULT_POLL_SECONDS)
    guard.add_argument("--max-wait-seconds", type=int, default=MAX_WAIT_SECONDS)
    return parser


def _emit(value: object) -> None:
    sys.stdout.buffer.write(canonical_bytes(value))
    sys.stdout.buffer.flush()


def main(
    argv: Sequence[str] | None = None,
    *,
    environ: Mapping[str, str] | None = None,
    run_command: CommandRunner | None = None,
) -> int:
    try:
        args = build_parser().parse_args(argv)
        environment = os.environ if environ is None else environ
        invocation = load_invocation(environment)
        report = run_guard(
            invocation,
            docker_path=args.docker_path,
            poll_seconds=args.poll_seconds,
            max_wait_seconds=args.max_wait_seconds,
            run_command=run_command,
        )
    except Exception:
        report = {
            "kind": GUARD_KIND,
            "schema_version": SCHEMA_VERSION,
            "status": "failed",
            "error_code": "invalid_invocation",
        }
    _emit(report)
    return 0 if report.get("status") == "ok" else 1


if __name__ == "__main__":
    raise SystemExit(main())


__all__ = [
    "DEFAULT_DOCKER_PATH",
    "DeployInvocation",
    "EXPECTED_DAG_IDS",
    "EXPECTED_METADB_CONTAINER",
    "EXPECTED_PHASES",
    "GUARD_KIND",
    "GuardError",
    "GuardSnapshot",
    "MAX_WAIT_SECONDS",
    "QUERY_TIMEOUT_SECONDS",
    "READONLY_SQL",
    "SNAPSHOT_KIND",
    "canonical_bytes",
    "load_invocation",
    "main",
    "read_snapshot",
    "run_guard",
]
