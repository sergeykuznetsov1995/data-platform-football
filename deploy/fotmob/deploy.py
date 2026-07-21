#!/usr/bin/env python3
"""Deploy and admit the isolated FotMob Airflow stack.

Admission is deliberately fail-closed: the scheduler must be healthy, its
DagBag must contain exactly the three expected DAGs, and import errors must be
empty before any DAG is unpaused.  A JSON report is written for every attempt.
"""

from __future__ import annotations

import argparse
import fcntl
import hashlib
import json
import os
import re
import secrets
import shutil
import stat
import subprocess
import tempfile
import time
from contextlib import contextmanager
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import Any, Callable, Mapping, Sequence


EXPECTED_DAGS = frozenset(
    {
        "dag_ingest_fotmob",
        "dag_transform_fotmob_silver",
        "dag_trigger_fotmob_daily",
    }
)
EXPECTED_DAG_FILES = {
    "dag_ingest_fotmob": "/opt/airflow/dags/dag_ingest_fotmob.py",
    "dag_transform_fotmob_silver": ("/opt/airflow/dags/dag_transform_fotmob_silver.py"),
    "dag_trigger_fotmob_daily": "/opt/airflow/dags/dag_trigger_fotmob_daily.py",
}
EXPECTED_SCHEDULES = {
    "dag_ingest_fotmob": "None",
    "dag_transform_fotmob_silver": "None",
    "dag_trigger_fotmob_daily": "0 14 * * *",
}
ACTIVE_STATES = ("running", "queued")
# This proves a real scheduled DagRun identity, not business/data success.  A
# failed terminal run still owns the exact admitted interval and must never be
# mistaken for an absent run that is safe to recreate.
EXACT_SCHEDULED_RUN_STATES = frozenset({"queued", "running", "success", "failed"})
SCHEDULE_BOUNDARY_FIELDS = (
    "logical_date",
    "data_interval_start",
    "data_interval_end",
    "run_after",
)
PENDING_BOUNDARY_NAMES = (
    "shared_initial",
    "shared_final",
    "isolated_initial",
    "isolated_final",
    "shared_commit",
    "isolated_commit",
)
PENDING_PROOF_FIELDS = frozenset(
    {
        "shared_dag_id",
        "isolated_dag_id",
        *PENDING_BOUNDARY_NAMES,
        "exact_match",
    }
)
PENDING_ACTIVATION_FIELDS = frozenset(
    {"status", "producer_dag_id", "consumer_dag_id", "resume_required"}
)
PENDING_SAFETY_FIELDS = frozenset(
    {
        "checked_at",
        "next_boundary",
        "remaining_seconds",
        "required_seconds",
        "timeout_seconds",
        "passed",
    }
)
SHARED_CONSUMER_DAG_ID = "dag_sofascore_pipeline"
ISOLATED_DAILY_DAG_ID = "dag_trigger_fotmob_daily"
MIN_ACTIVATION_SAFETY_SECONDS = 15 * 60
ACTIVATION_TIMEOUT_MARGIN_SECONDS = 5 * 60
SCHEDULE_PERIOD = timedelta(days=1)
RUNTIME_MARKER_TABLE = "iceberg.bronze.fotmob_runtime_deployments"
SHARED_RUNTIME_ROOTS = {
    "dags": "/opt/airflow/dags",
    "scrapers": "/opt/airflow/scrapers",
    "scripts": "/opt/airflow/scripts",
    "configs/medallion": "/opt/airflow/configs/medallion",
    "configs/fotmob": "/opt/airflow/configs/fotmob",
}
SHARED_RUNTIME_SUFFIXES = (
    ".py",
    ".pyi",
    ".sql",
    ".j2",
    ".json",
    ".yaml",
    ".yml",
    ".lock",
    ".sh",
    ".txt",
)
ISOLATED_DAG_ROOT_PATHS = {
    "dags/dag_ingest_fotmob.py",
    "dags/dag_transform_fotmob_silver.py",
    "dags/dag_trigger_fotmob_daily.py",
}
ISOLATED_DAG_PREFIXES = (
    "dags/scripts/",
    "dags/sql/",
    "dags/utils/",
)
ISOLATED_AIRFLOWIGNORE_PATH = "dags/.airflowignore"
CONTAINER_EVIDENCE_ROOT = Path("/opt/airflow/logs/fotmob")
SHARED_CONTAINER_EVIDENCE_ROOT = Path("/opt/airflow/fotmob-admission")
SHARED_DEPLOYMENT_REPORT_PATH_ENV = "FOTMOB_SHARED_DEPLOYMENT_REPORT_PATH"
SHARED_REQUIRED_RUNTIME_PATHS = {
    "configs/fotmob/competitions.json",
    "configs/fotmob/issue-930-player-source-refresh.json",
    "configs/fotmob/issue-930-scopes.txt",
    "dags/.airflowignore",
    "dags/dag_ingest_fotmob.py",
    "dags/dag_master_pipeline.py",
    "dags/dag_sofascore_pipeline.py",
    "dags/dag_trigger_fotmob_daily.py",
    "dags/dag_transform_e3.py",
    "dags/dag_transform_e4.py",
    "dags/dag_transform_fbref_gold.py",
    "dags/dag_transform_fotmob_silver.py",
    "dags/dag_transform_xref.py",
    "dags/scripts/run_fotmob_scraper.py",
    "dags/sql/silver/fotmob_keeper_profile.sql",
    "dags/sql/silver/fotmob_manager_profile.sql",
    "dags/sql/silver/fotmob_player_profile.sql",
    "dags/sql/silver/fotmob_player_season_profile.sql",
    "dags/sql/silver/xref_manager.sql.j2",
    "dags/utils/fotmob_publication.py",
    "dags/utils/maintenance_tasks.py",
    "dags/utils/silver_tasks.py",
    "dags/utils/xref_player_resolver.py",
    "scrapers/base/iceberg_writer.py",
    "scrapers/base/trino_manager.py",
    "scrapers/fbref/control/store.py",
    "scrapers/fotmob/constants.py",
    "scrapers/fotmob/raw_store.py",
    "scrapers/fotmob/repository.py",
    "scrapers/fotmob/service.py",
    "scrapers/fotmob/source_refresh.py",
    "scrapers/fotmob/transport.py",
}
MASTER_RUNTIME_PATH = "dags/dag_master_pipeline.py"
APPROVED_SCOPE_PATH = "configs/fotmob/issue-930-scopes.txt"
APPROVED_SCOPE_SHA256 = (
    "f1d95f916c78ed80e5784e2cd5bda7263cece37d9fde6d52fb2a1a4d9e97cb58"
)
PLAYER_SOURCE_REFRESH_PATH = "configs/fotmob/issue-930-player-source-refresh.json"
PLAYER_SOURCE_REFRESH_SHA256 = (
    "f6cb854c6d60463c899fd9077b61a71d8d0f817741c3a9d6423925b32949045b"
)
# The report is a non-secret admission certificate consumed by Airflow uid
# 50000 from a host bind mount.  Deploy commonly runs as root, so relying on
# the caller's umask/ownership would leave NamedTemporaryFile's 0600 mode in
# place and make every scheduled attestation fail.  World-read-only is
# deliberate: the report contains image/container IDs, hashes and credential
# presence booleans, never credential values.
DEPLOYMENT_REPORT_MODE = 0o444
EVIDENCE_DIRECTORY_MODE = 0o755
_RUNTIME_MUTATION_STARTED_ATTR = "_fotmob_runtime_mutation_started"


class DeploymentError(RuntimeError):
    pass


class PendingConsumerError(DeploymentError):
    """Activation needs an idempotent resume; producer must stay running."""

    def __init__(
        self,
        report: Mapping[str, Any],
        cause: BaseException,
        *,
        operator_action: str = ("rerun the exact deploy command with --resume-pending"),
    ):
        self.report = dict(report)
        self.cause = cause
        self.operator_action = operator_action
        super().__init__(
            "FotMob producer activation is pending its exact shared consumer: "
            f"{type(cause).__name__}: {cause}"
        )


class ConcurrentInvocationError(DeploymentError):
    pass


def _now() -> str:
    return datetime.now(timezone.utc).isoformat().replace("+00:00", "Z")


@contextmanager
def _deployment_invocation_lock(evidence_dir: Path):
    """Serialize deploy/resume on one durable evidence directory."""

    absolute = Path(os.path.abspath(evidence_dir))
    try:
        absolute.mkdir(parents=True, mode=EVIDENCE_DIRECTORY_MODE, exist_ok=True)
    except OSError as exc:
        raise DeploymentError("cannot create or inspect evidence directory") from exc
    resolved = Path(os.path.realpath(absolute))
    if resolved != absolute:
        raise DeploymentError("evidence directory must not contain symlinks")
    try:
        directory = os.stat(resolved, follow_symlinks=False)
    except OSError as exc:
        raise DeploymentError("cannot inspect evidence directory") from exc
    trusted_uids = {0, os.geteuid()}
    if (
        not stat.S_ISDIR(directory.st_mode)
        or directory.st_uid not in trusted_uids
        or stat.S_IMODE(directory.st_mode) & 0o022
    ):
        raise DeploymentError(
            "evidence directory must be owner-controlled and not group/world writable"
        )

    lock_path = resolved / ".fotmob-deploy.lock"
    base_flags = os.O_RDWR | getattr(os, "O_NOFOLLOW", 0) | getattr(os, "O_CLOEXEC", 0)
    created = False
    try:
        descriptor = os.open(
            lock_path,
            base_flags | os.O_CREAT | os.O_EXCL,
            0o600,
        )
        created = True
    except FileExistsError:
        try:
            descriptor = os.open(lock_path, base_flags)
        except OSError as exc:
            raise DeploymentError("deployment lock is not a safe regular file") from exc
    except OSError as exc:
        raise DeploymentError("cannot create the deployment lock") from exc
    try:
        if created:
            try:
                os.fchmod(descriptor, 0o600)
            except OSError as exc:
                raise DeploymentError("cannot secure the deployment lock") from exc
        try:
            lock_stat = os.fstat(descriptor)
            path_stat = os.stat(lock_path, follow_symlinks=False)
        except OSError as exc:
            raise DeploymentError("cannot attest the deployment lock") from exc
        if (
            not stat.S_ISREG(lock_stat.st_mode)
            or lock_stat.st_nlink != 1
            or lock_stat.st_uid not in trusted_uids
            or stat.S_IMODE(lock_stat.st_mode) != 0o600
            or (lock_stat.st_dev, lock_stat.st_ino)
            != (path_stat.st_dev, path_stat.st_ino)
        ):
            raise DeploymentError(
                "deployment lock must be one owner-controlled 0600 regular file"
            )
        try:
            fcntl.flock(descriptor, fcntl.LOCK_EX | fcntl.LOCK_NB)
        except BlockingIOError as exc:
            raise ConcurrentInvocationError(
                "another FotMob deploy/resume invocation holds the evidence lock"
            ) from exc
        yield lock_path
    finally:
        try:
            fcntl.flock(descriptor, fcntl.LOCK_UN)
        finally:
            os.close(descriptor)


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
            os.fchmod(stream.fileno(), DEPLOYMENT_REPORT_MODE)
            os.fsync(stream.fileno())
            temporary = Path(stream.name)
        temporary.replace(path)
        directory_fd = os.open(path.parent, os.O_RDONLY)
        try:
            os.fsync(directory_fd)
        finally:
            os.close(directory_fd)
    finally:
        if temporary is not None and temporary.exists():
            temporary.unlink()


def _prepare_evidence_report_path(evidence_dir: Path, report_path: Path) -> None:
    """Create deterministic traversable directories for a public report."""

    try:
        relative_parent = report_path.parent.relative_to(evidence_dir)
    except ValueError as exc:  # defensive; deploy validates this first
        raise DeploymentError(
            "deployment report is outside evidence directory"
        ) from exc
    evidence_dir.mkdir(parents=True, exist_ok=True)
    evidence_dir.chmod(EVIDENCE_DIRECTORY_MODE)
    current = evidence_dir
    for component in relative_parent.parts:
        current = current / component
        current.mkdir(exist_ok=True)
        current.chmod(EVIDENCE_DIRECTORY_MODE)


def _commit_trigger_activation(
    report_path: Path,
    report: Mapping[str, Any],
    *,
    isolated_container: str,
    shared_container: str,
    timeout_seconds: int,
    run: Callable[..., subprocess.CompletedProcess[str]],
    sleeper: Callable[[float], None],
    now: datetime | None = None,
) -> dict[str, Any]:
    """Commit pending, start producer, prove its consumer, then commit active."""

    shared_commit = read_schedule_boundary(
        shared_container, SHARED_CONSUMER_DAG_ID, run=run
    )
    isolated_commit = read_schedule_boundary(
        isolated_container, ISOLATED_DAILY_DAG_ID, run=run
    )
    previous_boundary = report.get("schedule_boundary")
    if not isinstance(previous_boundary, Mapping):
        raise DeploymentError("deployment report has no pre-activation schedule proof")
    schedule_boundary = validate_matching_schedule_boundaries(
        shared_initial=previous_boundary.get("shared_initial"),
        shared_final=previous_boundary.get("shared_final"),
        isolated_initial=previous_boundary.get("isolated_initial"),
        isolated_final=previous_boundary.get("isolated_final"),
        shared_commit=shared_commit,
        isolated_commit=isolated_commit,
    )
    safety_window = validate_activation_safety_window(
        schedule_boundary["shared_commit"],
        timeout_seconds=timeout_seconds,
        now=now,
    )
    pending = {
        **report,
        "generated_at": _now(),
        "activation_state": "pending_consumer",
        "kept_paused": False,
        # This is the exact state at the durable transition cut.  Resume owns
        # the subsequent idempotent unpauses; pending is not a false active
        # snapshot while the daily trigger is still paused.
        "paused": [ISOLATED_DAILY_DAG_ID],
        "unpaused": sorted(EXPECTED_DAGS - {ISOLATED_DAILY_DAG_ID}),
        "schedule_boundary": schedule_boundary,
        "activation_safety_window": safety_window,
        "scheduled_activation": {
            "status": "pending",
            "producer_dag_id": ISOLATED_DAILY_DAG_ID,
            "consumer_dag_id": SHARED_CONSUMER_DAG_ID,
            "resume_required": True,
        },
    }
    _atomic_json(report_path, pending)
    return _continue_pending_consumer_activation(
        report_path,
        pending,
        isolated_container=isolated_container,
        shared_container=shared_container,
        timeout_seconds=timeout_seconds,
        run=run,
        sleeper=sleeper,
    )


def validate_image_reference(image: str, *, label: str = "image") -> None:
    value = image.strip()
    if not re.fullmatch(r"[^\s@]+@sha256:[0-9a-fA-F]{64}", value):
        raise DeploymentError(f"{label} must be pinned by a full sha256 digest")


def validate_database_password(env_file: Path, environment: Mapping[str, str]) -> None:
    value = _configured_env_value(env_file, environment, "FOTMOB_AIRFLOW_DB_PASSWORD")

    # This secret is interpolated into a SQLAlchemy URI. Requiring the
    # RFC-3986 unreserved alphabet prevents reserved characters from changing
    # URI structure; operators should use a generated base64url/hex secret.
    if not re.fullmatch(r"[A-Za-z0-9._~-]+", value):
        raise DeploymentError(
            "FOTMOB_AIRFLOW_DB_PASSWORD must use only URL-safe unreserved characters"
        )


def _configured_env_value(
    env_file: Path,
    environment: Mapping[str, str],
    key_name: str,
) -> str:
    value = str(environment.get(key_name, ""))
    if not value:
        try:
            lines = env_file.read_text(encoding="utf-8").splitlines()
        except OSError as exc:
            raise DeploymentError(f"cannot read --env-file: {exc}") from exc
        for raw in lines:
            line = raw.strip()
            if not line or line.startswith("#") or "=" not in line:
                continue
            key, candidate = line.split("=", 1)
            if key.strip() != key_name:
                continue
            value = candidate.strip()
            if value[:1] in {"'", '"'} and value[-1:] == value[:1]:
                value = value[1:-1]
    return value


def validate_delivery_credentials(
    env_file: Path, environment: Mapping[str, str]
) -> None:
    missing = [
        key
        for key in ("TELEGRAM_BOT_TOKEN", "TELEGRAM_CHAT_ID")
        if not _configured_env_value(env_file, environment, key).strip()
    ]
    if missing:
        raise DeploymentError(
            f"required delivery credentials are absent: {sorted(missing)!r}"
        )


def release_sha(
    root: Path, run: Callable[..., subprocess.CompletedProcess[str]]
) -> str:
    if not root.is_absolute() or not root.is_dir():
        raise DeploymentError("--release-root must be an existing absolute directory")
    result = run(
        ("git", "-C", str(root), "rev-parse", "HEAD"),
        check=True,
        capture_output=True,
        text=True,
    )
    sha = result.stdout.strip()
    if len(sha) != 40 or any(character not in "0123456789abcdef" for character in sha):
        raise DeploymentError(f"release checkout returned invalid Git SHA: {sha!r}")
    dirty = run(
        ("git", "-C", str(root), "status", "--porcelain"),
        check=True,
        capture_output=True,
        text=True,
    ).stdout.strip()
    if dirty:
        raise DeploymentError("release checkout is dirty; deploy an immutable checkout")
    ignored_runtime = run(
        (
            "git",
            "-C",
            str(root),
            "ls-files",
            "--others",
            "--ignored",
            "--exclude-standard",
            "--",
            "dags",
            "scrapers",
            "scripts",
            "configs",
        ),
        check=True,
        capture_output=True,
        text=True,
    ).stdout.strip()
    if ignored_runtime:
        raise DeploymentError(
            "release runtime trees contain ignored/untracked files; use a pristine worktree"
        )
    return sha


def prepare_dagbag(release_root: Path, evidence_dir: Path, sha: str) -> Path:
    """Create/reuse an exact read-only projection that masks image-baked DAGs."""

    sources = {
        "dag_ingest_fotmob.py": release_root / "dags/dag_ingest_fotmob.py",
        "dag_transform_fotmob_silver.py": release_root
        / "dags/dag_transform_fotmob_silver.py",
        "dag_trigger_fotmob_daily.py": release_root
        / "dags/dag_trigger_fotmob_daily.py",
        ".airflowignore": release_root / "deploy/fotmob/.airflowignore",
    }
    missing = [str(path) for path in sources.values() if not path.is_file()]
    if missing:
        raise DeploymentError(f"release misses DagBag source files: {missing!r}")
    destination = evidence_dir / "runtime" / sha / "dags"

    def verify_existing() -> None:
        observed_files = {item.name for item in destination.iterdir() if item.is_file()}
        observed_dirs = {item.name for item in destination.iterdir() if item.is_dir()}
        if observed_files != set(sources) or observed_dirs != {
            "utils",
            "sql",
            "scripts",
        }:
            raise DeploymentError("existing DagBag projection has unexpected entries")
        for name, source in sources.items():
            if destination.joinpath(name).read_bytes() != source.read_bytes():
                raise DeploymentError(f"existing DagBag projection drifted: {name}")

    if destination.exists():
        if not destination.is_dir():
            raise DeploymentError("DagBag projection path is not a directory")
        verify_existing()
        return destination

    destination.parent.mkdir(parents=True, exist_ok=True)
    temporary = Path(tempfile.mkdtemp(prefix=".fotmob-dagbag-", dir=destination.parent))
    try:
        for name, source in sources.items():
            shutil.copyfile(source, temporary / name)
            (temporary / name).chmod(0o444)
        for name in ("utils", "sql", "scripts"):
            (temporary / name).mkdir(mode=0o555)
        temporary.chmod(0o555)
        temporary.replace(destination)
    except Exception:
        shutil.rmtree(temporary, ignore_errors=True)
        raise
    verify_existing()
    return destination


def parse_airflow_json(output: str) -> list[dict[str, Any]]:
    """Parse Airflow JSON while tolerating log prefixes emitted by some images."""

    decoder = json.JSONDecoder()
    for index, character in enumerate(output):
        if character != "[":
            continue
        try:
            payload, _ = decoder.raw_decode(output[index:])
        except json.JSONDecodeError:
            continue
        if isinstance(payload, list) and all(isinstance(row, dict) for row in payload):
            return payload
    raise DeploymentError("Airflow command did not emit a JSON array of objects")


def parse_marker_json(output: str, marker: str) -> Any:
    for line in reversed(output.splitlines()):
        if line.startswith(marker):
            try:
                return json.loads(line.removeprefix(marker))
            except json.JSONDecodeError as exc:
                raise DeploymentError(f"invalid {marker} payload") from exc
    raise DeploymentError(f"command did not emit required {marker} evidence")


def validate_schedule_boundary(raw: Any, *, label: str) -> dict[str, str]:
    """Canonicalize one paused DAG's exact next automated data interval."""

    if not isinstance(raw, Mapping) or set(raw) != set(SCHEDULE_BOUNDARY_FIELDS):
        raise DeploymentError(f"{label} next scheduled interval is incomplete")
    parsed: dict[str, datetime] = {}
    for field in SCHEDULE_BOUNDARY_FIELDS:
        value = raw.get(field)
        if not isinstance(value, str) or not value.strip():
            raise DeploymentError(f"{label} {field} is missing")
        try:
            instant = datetime.fromisoformat(value.strip().replace("Z", "+00:00"))
        except ValueError as exc:
            raise DeploymentError(
                f"{label} {field} is not an ISO-8601 instant"
            ) from exc
        if instant.tzinfo is None or instant.utcoffset() is None:
            raise DeploymentError(f"{label} {field} has no timezone")
        parsed[field] = instant.astimezone(timezone.utc)
    if parsed["logical_date"] != parsed["data_interval_start"]:
        raise DeploymentError(f"{label} logical date differs from interval start")
    if parsed["data_interval_start"] >= parsed["data_interval_end"]:
        raise DeploymentError(f"{label} next scheduled interval is empty or inverted")
    if parsed["run_after"] != parsed["data_interval_end"]:
        raise DeploymentError(f"{label} run-after differs from interval end")
    return {
        field: parsed[field].isoformat(timespec="microseconds")
        for field in SCHEDULE_BOUNDARY_FIELDS
    }


def _scheduled_run_id(logical_date: Any) -> str:
    """Mirror Airflow 2.11 ``DagRunType.SCHEDULED.generate_run_id`` exactly."""

    try:
        instant = datetime.fromisoformat(
            str(logical_date).strip().replace("Z", "+00:00")
        )
    except ValueError as exc:
        raise DeploymentError(
            "scheduled logical date is not an ISO-8601 instant"
        ) from exc
    if instant.tzinfo is None or instant.utcoffset() is None:
        raise DeploymentError("scheduled logical date has no timezone")
    return f"scheduled__{instant.astimezone(timezone.utc).isoformat()}"


def read_schedule_boundary(
    container: str,
    dag_id: str,
    *,
    run: Callable[..., subprocess.CompletedProcess[str]],
    require_paused: bool = True,
) -> dict[str, str]:
    """Read pause state and next interval in one Airflow metadata snapshot."""

    marker = "FOTMOB_SCHEDULE_BOUNDARY_JSON="
    code = (
        "import json; from airflow.models import DagModel; "
        "from airflow.settings import Session; "
        f"s=Session(); m=s.query(DagModel).filter(DagModel.dag_id=={dag_id!r}).one_or_none(); "
        "iso=lambda v: v.isoformat() if v is not None else None; "
        "p=None if m is None else {'is_paused':bool(m.is_paused),'boundary':{"
        "'logical_date':iso(m.next_dagrun),"
        "'data_interval_start':iso(m.next_dagrun_data_interval_start),"
        "'data_interval_end':iso(m.next_dagrun_data_interval_end),"
        "'run_after':iso(m.next_dagrun_create_after)}}; "
        f"print('{marker}'+json.dumps(p,sort_keys=True)); s.close()"
    )
    output = run(
        ("docker", "exec", container, "python", "-c", code),
        check=True,
        capture_output=True,
        text=True,
    ).stdout
    payload = parse_marker_json(output, marker)
    if (
        not isinstance(payload, Mapping)
        or set(payload) != {"is_paused", "boundary"}
        or not isinstance(payload.get("is_paused"), bool)
    ):
        raise DeploymentError(f"{dag_id} pause/boundary evidence is invalid")
    if require_paused and payload["is_paused"] is not True:
        raise DeploymentError(f"{dag_id} is not paused at the schedule commit edge")
    return validate_schedule_boundary(payload.get("boundary"), label=dag_id)


def validate_matching_schedule_boundaries(
    *,
    shared_initial: Any,
    shared_final: Any,
    isolated_initial: Any,
    isolated_final: Any,
    shared_commit: Any = None,
    isolated_commit: Any = None,
) -> dict[str, Any]:
    """Fail closed unless producer and consumer will create the same run."""

    boundaries = {
        "shared_initial": validate_schedule_boundary(
            shared_initial, label=f"initial {SHARED_CONSUMER_DAG_ID}"
        ),
        "shared_final": validate_schedule_boundary(
            shared_final, label=f"final {SHARED_CONSUMER_DAG_ID}"
        ),
        "isolated_initial": validate_schedule_boundary(
            isolated_initial, label=f"initial {ISOLATED_DAILY_DAG_ID}"
        ),
        "isolated_final": validate_schedule_boundary(
            isolated_final, label=f"final {ISOLATED_DAILY_DAG_ID}"
        ),
    }
    if (shared_commit is None) != (isolated_commit is None):
        raise DeploymentError("schedule commit-edge proof is incomplete")
    if shared_commit is not None:
        boundaries.update(
            {
                "shared_commit": validate_schedule_boundary(
                    shared_commit, label=f"commit {SHARED_CONSUMER_DAG_ID}"
                ),
                "isolated_commit": validate_schedule_boundary(
                    isolated_commit, label=f"commit {ISOLATED_DAILY_DAG_ID}"
                ),
            }
        )
    expected = boundaries["shared_initial"]
    if any(boundary != expected for boundary in boundaries.values()):
        raise DeploymentError(
            "shared SofaScore consumer and isolated FotMob producer have "
            "different next scheduled intervals"
        )
    return {
        "shared_dag_id": SHARED_CONSUMER_DAG_ID,
        "isolated_dag_id": ISOLATED_DAILY_DAG_ID,
        **boundaries,
        "exact_match": True,
    }


def validate_activation_safety_window(
    boundary: Any,
    *,
    timeout_seconds: int,
    now: datetime | None = None,
) -> dict[str, Any]:
    """Require enough time to finish handoff before the next 14:00 boundary."""

    normalized = validate_schedule_boundary(boundary, label="activation commit")
    checked_at = now or datetime.now(timezone.utc)
    if checked_at.tzinfo is None or checked_at.utcoffset() is None:
        raise DeploymentError("activation safety timestamp has no timezone")
    checked_at = checked_at.astimezone(timezone.utc)
    next_boundary = datetime.fromisoformat(normalized["run_after"])
    while next_boundary <= checked_at:
        next_boundary += SCHEDULE_PERIOD
    required_seconds = max(
        MIN_ACTIVATION_SAFETY_SECONDS,
        max(1, timeout_seconds) + ACTIVATION_TIMEOUT_MARGIN_SECONDS,
    )
    remaining_seconds = int((next_boundary - checked_at).total_seconds())
    if remaining_seconds < required_seconds:
        raise DeploymentError(
            "schedule activation is too close to the next 14:00 UTC boundary: "
            f"remaining={remaining_seconds}s required={required_seconds}s"
        )
    return {
        "checked_at": checked_at.isoformat(timespec="microseconds"),
        "next_boundary": next_boundary.isoformat(timespec="microseconds"),
        "remaining_seconds": remaining_seconds,
        "required_seconds": required_seconds,
        "timeout_seconds": max(1, timeout_seconds),
        "passed": True,
    }


def read_exact_scheduled_run(
    container: str,
    dag_id: str,
    expected_boundary: Any,
    *,
    run: Callable[..., subprocess.CompletedProcess[str]],
) -> dict[str, str] | None:
    """Return the exact scheduled DagRun for one admitted interval, if created."""

    expected = validate_schedule_boundary(expected_boundary, label=dag_id)
    marker = "FOTMOB_SCHEDULED_RUNS_JSON="
    code = (
        "import json; from airflow.models import DagRun; "
        "from airflow.settings import Session; "
        "from airflow.utils.types import DagRunType; "
        f"s=Session(); rows=s.query(DagRun).filter(DagRun.dag_id=={dag_id!r})"
        ".order_by(DagRun.logical_date.desc()).limit(20).all(); "
        "iso=lambda v: v.isoformat() if v is not None else None; "
        "p=[{'run_id':str(r.run_id),'expected_run_id':DagRun.generate_run_id(DagRunType.SCHEDULED,r.logical_date),"
        "'run_type':str(getattr(r.run_type,'value',r.run_type)),"
        "'logical_date':iso(r.logical_date),'data_interval_start':iso(r.data_interval_start),"
        "'data_interval_end':iso(r.data_interval_end),'state':str(getattr(r.state,'value',r.state))} "
        "for r in rows]; "
        f"print('{marker}'+json.dumps(p,sort_keys=True)); s.close()"
    )
    output = run(
        ("docker", "exec", container, "python", "-c", code),
        check=True,
        capture_output=True,
        text=True,
    ).stdout
    payload = parse_marker_json(output, marker)
    if not isinstance(payload, list) or any(
        not isinstance(row, Mapping) for row in payload
    ):
        raise DeploymentError(f"{dag_id} scheduled-run evidence is invalid")
    matches: list[dict[str, str]] = []
    for row in payload:
        raw_boundary = {
            "logical_date": row.get("logical_date"),
            "data_interval_start": row.get("data_interval_start"),
            "data_interval_end": row.get("data_interval_end"),
            "run_after": row.get("data_interval_end"),
        }
        try:
            observed = validate_schedule_boundary(raw_boundary, label=f"{dag_id} run")
        except DeploymentError:
            continue
        if observed != expected:
            continue
        run_type = str(row.get("run_type") or "").casefold()
        run_id = str(row.get("run_id") or "")
        if (
            run_type != "scheduled"
            or run_id != str(row.get("expected_run_id") or "")
            or run_id != _scheduled_run_id(observed["logical_date"])
        ):
            raise DeploymentError(
                f"{dag_id} exact interval exists without a scheduled DagRun identity"
            )
        state = str(row.get("state") or "").casefold()
        if state not in EXACT_SCHEDULED_RUN_STATES:
            raise DeploymentError(f"{dag_id} exact scheduled DagRun has invalid state")
        matches.append(
            {
                "dag_id": dag_id,
                "run_id": run_id,
                "run_type": run_type,
                "logical_date": observed["logical_date"],
                "data_interval_start": observed["data_interval_start"],
                "data_interval_end": observed["data_interval_end"],
                "state": state,
            }
        )
    if len(matches) > 1:
        raise DeploymentError(f"{dag_id} has duplicate exact scheduled DagRuns")
    return matches[0] if matches else None


def poll_exact_scheduled_handoff(
    *,
    isolated_container: str,
    shared_container: str,
    boundary: Any,
    timeout_seconds: int,
    run: Callable[..., subprocess.CompletedProcess[str]],
    sleeper: Callable[[float], None],
) -> dict[str, Any]:
    deadline = time.monotonic() + max(1, timeout_seconds)
    producer: dict[str, str] | None = None
    consumer: dict[str, str] | None = None
    while time.monotonic() < deadline:
        producer = read_exact_scheduled_run(
            isolated_container, ISOLATED_DAILY_DAG_ID, boundary, run=run
        )
        consumer = read_exact_scheduled_run(
            shared_container, SHARED_CONSUMER_DAG_ID, boundary, run=run
        )
        if producer is not None and consumer is not None:
            break
        sleeper(2)
    if producer is None or consumer is None:
        raise DeploymentError(
            "timed out waiting for exact scheduled FotMob producer and SofaScore consumer"
        )
    identity_fields = (
        "run_id",
        "run_type",
        "logical_date",
        "data_interval_start",
        "data_interval_end",
    )
    if any(producer[field] != consumer[field] for field in identity_fields):
        raise DeploymentError(
            "producer and consumer scheduled DagRun identities differ"
        )
    return {
        "status": "proved",
        "producer": producer,
        "consumer": consumer,
        "exact_identity_match": True,
    }


def _docker_unpause(
    container: str,
    dag_id: str,
    *,
    run: Callable[..., subprocess.CompletedProcess[str]],
) -> None:
    run(
        ("docker", "exec", container, "airflow", "dags", "unpause", dag_id),
        check=True,
        capture_output=True,
        text=True,
    )


def _continue_pending_consumer_activation(
    report_path: Path,
    pending: Mapping[str, Any],
    *,
    isolated_container: str,
    shared_container: str,
    timeout_seconds: int,
    run: Callable[..., subprocess.CompletedProcess[str]],
    sleeper: Callable[[float], None],
) -> dict[str, Any]:
    boundary_proof = pending.get("schedule_boundary")
    if not isinstance(boundary_proof, Mapping):
        raise DeploymentError("pending activation has no schedule boundary proof")
    boundary = boundary_proof.get("isolated_commit")
    try:
        for dag_id in ("dag_ingest_fotmob", "dag_transform_fotmob_silver"):
            _docker_unpause(isolated_container, dag_id, run=run)
        _docker_unpause(isolated_container, ISOLATED_DAILY_DAG_ID, run=run)
        _docker_unpause(shared_container, SHARED_CONSUMER_DAG_ID, run=run)
        activation = poll_exact_scheduled_handoff(
            isolated_container=isolated_container,
            shared_container=shared_container,
            boundary=boundary,
            timeout_seconds=timeout_seconds,
            run=run,
            sleeper=sleeper,
        )
        active = {
            **pending,
            "generated_at": _now(),
            "activation_state": "active",
            "paused": [],
            "unpaused": sorted(EXPECTED_DAGS),
            "scheduled_activation": activation,
        }
        _validate_active_scheduled_proof(
            active,
            validate_schedule_boundary(boundary, label="activation commit"),
        )
        _atomic_json(report_path, active)
        return active
    except Exception as exc:
        raise _pending_report_error(report_path, pending, exc) from exc


def validate_delivery_runtime(
    container: str,
    *,
    run: Callable[..., subprocess.CompletedProcess[str]],
) -> dict[str, bool]:
    marker = "FOTMOB_DELIVERY_ENV_JSON="
    code = (
        "import json,os; "
        "r={'telegram_bot_token_configured':"
        "bool(os.environ.get('TELEGRAM_BOT_TOKEN','').strip()),"
        "'telegram_chat_id_configured':"
        "bool(os.environ.get('TELEGRAM_CHAT_ID','').strip())}; "
        f"print('{marker}'+json.dumps(r,sort_keys=True))"
    )
    output = run(
        ("docker", "exec", container, "python", "-c", code),
        check=True,
        capture_output=True,
        text=True,
    ).stdout
    payload = parse_marker_json(output, marker)
    expected = {
        "telegram_bot_token_configured": True,
        "telegram_chat_id_configured": True,
    }
    if payload != expected:
        raise DeploymentError("admitted scheduler misses Telegram delivery credentials")
    return expected


def validate_control_database(
    container: str,
    expected_uri: str,
    *,
    run: Callable[..., subprocess.CompletedProcess[str]],
) -> dict[str, Any]:
    """Prove a scheduler uses the shared, fully-migrated control database."""

    if not expected_uri or "airflow-metadb" in expected_uri.lower():
        raise DeploymentError(
            "FBREF_CONTROL_DB_URI must reference the shared production control DB"
        )
    observed_uri = run(
        ("docker", "exec", container, "printenv", "FBREF_CONTROL_DB_URI"),
        check=True,
        capture_output=True,
        text=True,
    ).stdout.rstrip("\n")
    if observed_uri != expected_uri:
        raise DeploymentError(
            "scheduler FBREF_CONTROL_DB_URI differs from the admitted shared value"
        )
    code = (
        "import json; from scrapers.fbref.control import ControlStore; "
        "r=ControlStore.from_env().validate_migrations(); "
        "print('FOTMOB_CONTROL_DB_JSON='+json.dumps(r,sort_keys=True))"
    )
    output = run(
        ("docker", "exec", container, "python", "-c", code),
        check=True,
        capture_output=True,
        text=True,
    ).stdout
    payload = parse_marker_json(output, "FOTMOB_CONTROL_DB_JSON=")
    if (
        not isinstance(payload, Mapping)
        or payload.get("status") != "passed"
        or payload.get("checksum_verified") is not True
        or not isinstance(payload.get("versions"), list)
        or not payload["versions"]
    ):
        raise DeploymentError("shared control database migration preflight failed")
    return {
        "same_shared_database": True,
        "migrations": payload,
    }


def validate_dagbag(
    dag_rows: Sequence[Mapping[str, Any]], errors: Sequence[Any]
) -> None:
    dag_ids = {str(row.get("dag_id")) for row in dag_rows}
    if dag_ids != EXPECTED_DAGS:
        raise DeploymentError(
            f"unexpected DagBag: expected={sorted(EXPECTED_DAGS)!r}, "
            f"observed={sorted(dag_ids)!r}"
        )
    if errors:
        raise DeploymentError(f"DagBag has {len(errors)} import error(s)")


def validate_fresh_dagbag(payload: Mapping[str, Any]) -> None:
    dags = payload.get("dags")
    errors = payload.get("import_errors")
    if not isinstance(dags, Mapping) or set(dags) != EXPECTED_DAGS:
        raise DeploymentError("fresh DagBag does not contain exactly the admitted DAGs")
    if not isinstance(errors, Mapping) or errors:
        raise DeploymentError("fresh DagBag contains import errors")
    for dag_id in EXPECTED_DAGS:
        row = dags.get(dag_id)
        if not isinstance(row, Mapping):
            raise DeploymentError(f"fresh DagBag misses metadata for {dag_id}")
        if row.get("fileloc") != EXPECTED_DAG_FILES[dag_id]:
            raise DeploymentError(
                f"{dag_id} loaded from unexpected file: {row.get('fileloc')!r}"
            )
        if row.get("schedule") != EXPECTED_SCHEDULES[dag_id]:
            raise DeploymentError(
                f"{dag_id} has unexpected schedule: {row.get('schedule')!r}"
            )


def _paused_ids(rows: Sequence[Mapping[str, Any]]) -> set[str]:
    return {
        str(row.get("dag_id"))
        for row in rows
        if row.get("is_paused") in (True, "True", "true", "1", 1)
    }


def _sha256(path: Path) -> str:
    return hashlib.sha256(path.read_bytes()).hexdigest()


def shared_runtime_manifest(release_root: Path) -> dict[str, str]:
    """Hash the exact regular-file inventory exposed by shared bind mounts."""

    manifest: dict[str, str] = {}
    for relative_root in SHARED_RUNTIME_ROOTS:
        root = release_root / relative_root
        if not root.is_dir():
            raise DeploymentError(f"shared runtime root is absent: {relative_root}")
        for path in sorted(root.rglob("*")):
            if path.is_symlink():
                raise DeploymentError(
                    f"shared runtime manifest rejects symlink: {path}"
                )
            if (
                path.is_file()
                and "__pycache__" not in path.parts
                and (
                    path.name == ".airflowignore"
                    or path.name.endswith(SHARED_RUNTIME_SUFFIXES)
                )
            ):
                relative_path = path.relative_to(release_root).as_posix()
                manifest[relative_path] = _sha256(path)
    missing = SHARED_REQUIRED_RUNTIME_PATHS - set(manifest)
    if missing:
        raise DeploymentError(
            f"shared runtime manifest misses required files: {sorted(missing)!r}"
        )
    if manifest[APPROVED_SCOPE_PATH] != APPROVED_SCOPE_SHA256:
        raise DeploymentError("issue-930 scope artifact differs from approved SHA-256")
    if manifest[PLAYER_SOURCE_REFRESH_PATH] != PLAYER_SOURCE_REFRESH_SHA256:
        raise DeploymentError(
            "issue-930 player source-refresh artifact differs from approved SHA-256"
        )
    return manifest


def expected_isolated_runtime_manifest(
    release_root: Path, dagbag_root: Path
) -> dict[str, str]:
    """Return the exact effective file inventory mounted in the isolated stack."""

    shared = shared_runtime_manifest(release_root)
    manifest = {
        path: digest
        for path, digest in shared.items()
        if not path.startswith("dags/")
        or path in ISOLATED_DAG_ROOT_PATHS
        or path.startswith(ISOLATED_DAG_PREFIXES)
    }
    missing = ISOLATED_DAG_ROOT_PATHS - set(manifest)
    if missing:
        raise DeploymentError(
            f"isolated runtime manifest misses root DAGs: {sorted(missing)!r}"
        )
    airflowignore = dagbag_root / ".airflowignore"
    if not airflowignore.is_file() or airflowignore.is_symlink():
        raise DeploymentError("isolated DagBag projection misses .airflowignore")
    manifest[ISOLATED_AIRFLOWIGNORE_PATH] = _sha256(airflowignore)
    return dict(sorted(manifest.items()))


def validate_isolated_runtime_manifest(
    container_id: str,
    expected: Mapping[str, str],
    *,
    run: Callable[..., subprocess.CompletedProcess[str]],
) -> dict[str, str]:
    """Re-hash the effective bind mounts inside the admitted scheduler."""

    code = (
        "import json,sys; sys.path.insert(0,'/opt/airflow/dags'); "
        "from utils.fotmob_publication import isolated_runtime_manifest; "
        "print('FOTMOB_ISOLATED_RUNTIME_MANIFEST_JSON='+"
        "json.dumps(isolated_runtime_manifest(),sort_keys=True))"
    )
    output = run(
        ("docker", "exec", container_id, "python", "-c", code),
        check=True,
        capture_output=True,
        text=True,
    ).stdout
    observed = parse_marker_json(output, "FOTMOB_ISOLATED_RUNTIME_MANIFEST_JSON=")
    if not isinstance(observed, Mapping) or any(
        not isinstance(path, str) or re.fullmatch(r"[0-9a-f]{64}", str(digest)) is None
        for path, digest in observed.items()
    ):
        raise DeploymentError("isolated runtime manifest evidence is invalid")
    normalized = {str(path): str(digest) for path, digest in observed.items()}
    if normalized != dict(expected):
        raise DeploymentError(
            "isolated scheduler bind-mounted runtime differs from release manifest"
        )
    return normalized


def validate_shared_admission_mount(
    shared_container: str,
    evidence_dir: Path,
    report_relative_path: Path,
    *,
    run: Callable[..., subprocess.CompletedProcess[str]],
) -> dict[str, Any]:
    """Bind the shared certificate path to one exact read-only host mount."""

    relative_report = Path(report_relative_path)
    if (
        relative_report.is_absolute()
        or not relative_report.parts
        or ".." in relative_report.parts
    ):
        raise DeploymentError("shared deployment report path must be relative")
    try:
        expected_source = evidence_dir.resolve(strict=True)
    except OSError as exc:
        raise DeploymentError("shared evidence directory is unavailable") from exc
    expected_destination = str(SHARED_CONTAINER_EVIDENCE_ROOT)
    expected_report = str(SHARED_CONTAINER_EVIDENCE_ROOT / relative_report)

    mounts_output = run(
        (
            "docker",
            "inspect",
            "--format",
            "{{json .Mounts}}",
            shared_container,
        ),
        check=True,
        capture_output=True,
        text=True,
    ).stdout.strip()
    try:
        mounts = json.loads(mounts_output)
    except json.JSONDecodeError as exc:
        raise DeploymentError("shared scheduler mount evidence is invalid") from exc
    if not isinstance(mounts, list):
        raise DeploymentError("shared scheduler mount evidence is invalid")
    matching = [
        mount
        for mount in mounts
        if isinstance(mount, Mapping)
        and mount.get("Destination") == expected_destination
    ]
    if len(matching) != 1:
        raise DeploymentError(
            "shared scheduler must have one exact FotMob admission mount"
        )
    mount = matching[0]
    source_value = str(mount.get("Source", "")).strip()
    try:
        observed_source = Path(source_value).resolve(strict=True)
    except OSError as exc:
        raise DeploymentError(
            "shared scheduler FotMob admission mount source is unavailable"
        ) from exc
    if (
        mount.get("Type") != "bind"
        or mount.get("RW") is not False
        or observed_source != expected_source
    ):
        raise DeploymentError(
            "shared scheduler FotMob admission mount is not the exact read-only "
            "evidence directory"
        )

    evidence_stat = observed_source.stat()
    for other in mounts:
        if (
            other is mount
            or not isinstance(other, Mapping)
            or other.get("Type") not in {"bind", "volume"}
            or other.get("RW") is not True
        ):
            continue
        other_source_value = str(other.get("Source", "")).strip()
        try:
            other_source = Path(other_source_value).resolve(strict=True)
            other_stat = other_source.stat()
        except OSError as exc:
            raise DeploymentError(
                "shared scheduler writable mount source is unavailable"
            ) from exc
        if (
            (evidence_stat.st_dev, evidence_stat.st_ino)
            == (other_stat.st_dev, other_stat.st_ino)
            or observed_source in other_source.parents
            or other_source in observed_source.parents
        ):
            raise DeploymentError(
                "shared FotMob evidence aliases or nests with a writable "
                "scheduler mount"
            )

    observed_report = run(
        (
            "docker",
            "exec",
            shared_container,
            "printenv",
            SHARED_DEPLOYMENT_REPORT_PATH_ENV,
        ),
        check=True,
        capture_output=True,
        text=True,
    ).stdout.rstrip("\n")
    if observed_report != expected_report:
        raise DeploymentError(
            "shared scheduler deployment report path differs from admission mount"
        )
    return {
        "type": "bind",
        "source": str(expected_source),
        "destination": expected_destination,
        "read_only": True,
        "report_path": expected_report,
    }


def validate_shared_handoff(
    release_root: Path,
    shared_container: str,
    expected_control_uri: str,
    *,
    evidence_dir: Path,
    report_relative_path: Path,
    run: Callable[..., subprocess.CompletedProcess[str]],
) -> dict[str, Any]:
    """Prove the shared scheduler has surrendered FotMob schedule ownership."""

    shared_container_id = run(
        ("docker", "inspect", "--format", "{{.Id}}", shared_container),
        check=True,
        capture_output=True,
        text=True,
    ).stdout.strip()
    if re.fullmatch(r"[0-9a-f]{64}", shared_container_id) is None:
        raise DeploymentError("cannot resolve full shared scheduler container ID")
    shared_container = shared_container_id
    shared_admission_mount = validate_shared_admission_mount(
        shared_container,
        evidence_dir,
        report_relative_path,
        run=run,
    )
    control_database = validate_control_database(
        shared_container, expected_control_uri, run=run
    )
    shared_runtime_hashes = shared_runtime_manifest(release_root)
    manifest_code = (
        "import hashlib,json\n"
        "from pathlib import Path\n"
        f"roots={SHARED_RUNTIME_ROOTS!r}\n"
        f"suffixes={SHARED_RUNTIME_SUFFIXES!r}\n"
        "manifest={}\n"
        "for prefix, root_name in roots.items():\n"
        "    root=Path(root_name)\n"
        "    if not root.is_dir():\n"
        "        raise RuntimeError('shared runtime root is absent: '+prefix)\n"
        "    for path in sorted(root.rglob('*')):\n"
        "        if path.is_symlink():\n"
        "            raise RuntimeError('shared runtime symlink: '+str(path))\n"
        "        if (path.is_file() and '__pycache__' not in path.parts "
        "and (path.name == '.airflowignore' or "
        "path.name.endswith(suffixes))):\n"
        "            key=prefix+'/'+path.relative_to(root).as_posix()\n"
        "            manifest[key]=hashlib.sha256(path.read_bytes()).hexdigest()\n"
        "print('FOTMOB_SHARED_RUNTIME_MANIFEST_JSON='+"
        "json.dumps(manifest,sort_keys=True))\n"
    )
    remote_manifest_output = run(
        (
            "docker",
            "exec",
            shared_container,
            "python",
            "-c",
            manifest_code,
        ),
        check=True,
        capture_output=True,
        text=True,
    ).stdout
    remote_manifest = parse_marker_json(
        remote_manifest_output, "FOTMOB_SHARED_RUNTIME_MANIFEST_JSON="
    )
    if (
        not isinstance(remote_manifest, Mapping)
        or remote_manifest != shared_runtime_hashes
    ):
        raise DeploymentError(
            "shared scheduler bind-mounted runtime differs from the exact release manifest"
        )
    expected_hash = shared_runtime_hashes[MASTER_RUNTIME_PATH]
    remote_hash = str(remote_manifest[MASTER_RUNTIME_PATH])
    shared_runtime_sha = run(
        (
            "docker",
            "exec",
            shared_container,
            "printenv",
            "FOTMOB_DEPLOY_GIT_SHA",
        ),
        check=True,
        capture_output=True,
        text=True,
    ).stdout.rstrip("\n")
    expected_runtime_sha = run(
        ("git", "-C", str(release_root), "rev-parse", "HEAD"),
        check=True,
        capture_output=True,
        text=True,
    ).stdout.strip()
    if not re.fullmatch(r"[0-9a-f]{40}", expected_runtime_sha):
        raise DeploymentError("release checkout returned invalid Git SHA")
    if shared_runtime_sha != expected_runtime_sha:
        raise DeploymentError(
            "shared scheduler FOTMOB_DEPLOY_GIT_SHA differs from release HEAD"
        )
    serialized_code = """
import json
import os
from airflow.models import DagModel, DagRun, Variable
from airflow.models.serialized_dag import SerializedDagModel
from airflow.settings import Session
from sqlalchemy import text

s = Session()
s.execute(text('SET TRANSACTION ISOLATION LEVEL REPEATABLE READ, READ ONLY'))
serialized_ids = (
    'dag_master_pipeline',
    'dag_sofascore_pipeline',
    'dag_transform_xref',
    'dag_transform_e3',
    'dag_transform_e4',
    'dag_transform_fbref_gold',
    'dag_trigger_fotmob_daily',
)
serialized_rows = s.query(SerializedDagModel).filter(
    SerializedDagModel.dag_id.in_(serialized_ids)
).all()
dags = {row.dag_id: row.dag for row in serialized_rows}
master = dags.get('dag_master_pipeline')
master_tasks = getattr(master, 'task_dict', {}) if master is not None else {}
master_gate = master_tasks.get('ingestion_triggers.fotmob_shared_schedule_owner')
master_trigger = master_tasks.get('ingestion_triggers.trigger_fotmob')
sofa = dags.get('dag_sofascore_pipeline')
sofa_tasks = getattr(sofa, 'task_dict', {}) if sofa is not None else {}
sensor = sofa_tasks.get('wait_for_fotmob_publication')
xref = sofa_tasks.get('trigger_xref_transforms')
e4 = sofa_tasks.get('trigger_e4_transforms')
finalizer = sofa_tasks.get('finalize_fotmob_publication')
xref_dag = dags.get('dag_transform_xref')
xref_tasks = getattr(xref_dag, 'task_dict', {}) if xref_dag is not None else {}
xref_start = xref_tasks.get('start_marker')
xref_preflight = xref_tasks.get('validate_fotmob_publication_consumer')
isolated_daily = dags.get('dag_trigger_fotmob_daily')

def descendants(task):
    pending = list(getattr(task, 'downstream_task_ids', [])) if task else []
    observed = set()
    while pending:
        task_id = pending.pop()
        if task_id in observed:
            continue
        observed.add(task_id)
        child = xref_tasks.get(task_id)
        pending.extend(getattr(child, 'downstream_task_ids', []))
    return sorted(observed)

def fenced_downstream(dag_id):
    downstream_dag = dags.get(dag_id)
    tasks = (
        getattr(downstream_dag, 'task_dict', {})
        if downstream_dag is not None else {}
    )
    start = tasks.get('start_marker')
    preflight = tasks.get('validate_fotmob_publication_consumer')
    pending = list(getattr(preflight, 'downstream_task_ids', [])) if preflight else []
    observed = set()
    while pending:
        task_id = pending.pop()
        if task_id in observed:
            continue
        observed.add(task_id)
        child = tasks.get(task_id)
        pending.extend(getattr(child, 'downstream_task_ids', []))
    direct_downstream = sorted(getattr(preflight, 'downstream_task_ids', []))
    return {
        'present': downstream_dag is not None,
        'fileloc': getattr(downstream_dag, 'fileloc', None),
        'task_ids': sorted(tasks),
        'start_present': start is not None,
        'start_downstream': sorted(getattr(start, 'downstream_task_ids', [])),
        'preflight_present': preflight is not None,
        'preflight_upstream': sorted(getattr(preflight, 'upstream_task_ids', [])),
        'preflight_downstream': direct_downstream,
        'preflight_descendants': sorted(observed),
        'preflight_trigger_rule': str(getattr(preflight, 'trigger_rule', '')),
        'direct_downstream_trigger_rules': {
            task_id: str(getattr(tasks.get(task_id), 'trigger_rule', ''))
            for task_id in direct_downstream
        },
    }

pause_ids = (
    'dag_master_pipeline',
    'dag_sofascore_pipeline',
    'dag_ingest_fotmob',
    'dag_transform_fotmob_silver',
)
active_ids = (
    *pause_ids,
    'dag_transform_xref',
    'dag_transform_e3',
    'dag_transform_e4',
    'dag_transform_fbref_gold',
    'dag_trigger_fotmob_daily',
)
pause_rows = s.query(DagModel.dag_id, DagModel.is_paused).filter(
    DagModel.dag_id.in_(pause_ids)
).all()
sofa_model = s.query(DagModel).filter(
    DagModel.dag_id == 'dag_sofascore_pipeline'
).one_or_none()
daily_model = s.query(DagModel.dag_id, DagModel.is_paused).filter(
    DagModel.dag_id == 'dag_trigger_fotmob_daily'
).one_or_none()
run_rows = s.query(DagRun.dag_id, DagRun.run_id, DagRun.state).filter(
    DagRun.dag_id.in_(active_ids), DagRun.state.in_(('running', 'queued'))
).all()
owner_row = s.query(Variable).filter(
    Variable.key == 'fotmob_schedule_owner'
).one_or_none()

def instant(value):
    return value.isoformat() if value is not None else None

payload = {
    'master': {
        'present': master is not None,
        'fileloc': getattr(master, 'fileloc', None),
        'gate_present': master_gate is not None,
        'trigger_upstream': sorted(getattr(master_trigger, 'upstream_task_ids', [])),
    },
    'sofascore': {
        'present': sofa is not None,
        'fileloc': getattr(sofa, 'fileloc', None),
        'sensor_present': sensor is not None,
        'xref_present': xref is not None,
        'e4_present': e4 is not None,
        'finalizer_present': finalizer is not None,
        'sensor_downstream': sorted(getattr(sensor, 'downstream_task_ids', [])),
        'xref_upstream': sorted(getattr(xref, 'upstream_task_ids', [])),
        'e4_downstream': sorted(getattr(e4, 'downstream_task_ids', [])),
        'finalizer_upstream': sorted(getattr(finalizer, 'upstream_task_ids', [])),
        'finalizer_trigger_rule': str(getattr(finalizer, 'trigger_rule', '')),
    },
    'xref': {
        'present': xref_dag is not None,
        'fileloc': getattr(xref_dag, 'fileloc', None),
        'task_ids': sorted(xref_tasks),
        'start_present': xref_start is not None,
        'preflight_present': xref_preflight is not None,
        'start_downstream': sorted(
            getattr(xref_start, 'downstream_task_ids', [])
        ),
        'preflight_upstream': sorted(
            getattr(xref_preflight, 'upstream_task_ids', [])
        ),
        'preflight_descendants': descendants(xref_preflight),
        'preflight_trigger_rule': str(
            getattr(xref_preflight, 'trigger_rule', '')
        ),
        'task_trigger_rules': {
            task_id: str(getattr(task, 'trigger_rule', ''))
            for task_id, task in xref_tasks.items()
        },
    },
    'fenced_downstream': {
        dag_id: fenced_downstream(dag_id)
        for dag_id in (
            'dag_transform_e3',
            'dag_transform_e4',
            'dag_transform_fbref_gold',
        )
    },
    'pause_states': {dag_id: bool(paused) for dag_id, paused in pause_rows},
    'sofascore_schedule_boundary': (
        None
        if sofa_model is None
        else {
            'logical_date': instant(sofa_model.next_dagrun),
            'data_interval_start': instant(
                sofa_model.next_dagrun_data_interval_start
            ),
            'data_interval_end': instant(
                sofa_model.next_dagrun_data_interval_end
            ),
            'run_after': instant(sofa_model.next_dagrun_create_after),
        }
    ),
    'schedule_owner': getattr(owner_row, 'val', None),
    'shared_daily_trigger': {
        'isolated_stack_env': os.environ.get('FOTMOB_ISOLATED_STACK'),
        'serialized_present': isolated_daily is not None,
        'serialized_fileloc': getattr(isolated_daily, 'fileloc', None),
        'dag_model_present': daily_model is not None,
        'dag_model_paused': bool(daily_model[1]) if daily_model is not None else None,
    },
    'active_runs': [
        {
            'dag_id': dag_id,
            'run_id': str(run_id),
            'state': str(getattr(state, 'value', state)).lower(),
        }
        for dag_id, run_id, state in run_rows
    ],
}
print('FOTMOB_SHARED_ORCHESTRATION_JSON=' + json.dumps(payload, sort_keys=True))
s.close()
"""
    serialized_output = run(
        (
            "docker",
            "exec",
            shared_container,
            "python",
            "-c",
            serialized_code,
        ),
        check=True,
        capture_output=True,
        text=True,
    ).stdout
    orchestration = parse_marker_json(
        serialized_output, "FOTMOB_SHARED_ORCHESTRATION_JSON="
    )
    gate_id = "ingestion_triggers.fotmob_shared_schedule_owner"
    if not isinstance(orchestration, Mapping):
        raise DeploymentError("shared orchestration evidence is not an object")
    serialized = orchestration.get("master")
    if not isinstance(serialized, Mapping) or not serialized.get("present"):
        raise DeploymentError("shared metadata has no serialized master DAG")
    if serialized.get("fileloc") != "/opt/airflow/dags/dag_master_pipeline.py":
        raise DeploymentError(
            "shared serialized master DAG has unexpected file location"
        )
    if serialized.get("gate_present") is not True or gate_id not in set(
        serialized.get("trigger_upstream") or ()
    ):
        raise DeploymentError(
            "shared serialized master DAG has not admitted the FotMob ownership gate"
        )
    serialized_sofa = orchestration.get("sofascore")
    if not isinstance(serialized_sofa, Mapping) or not serialized_sofa.get("present"):
        raise DeploymentError(
            "shared metadata has no serialized SofaScore pipeline DAG"
        )
    if serialized_sofa.get("fileloc") != (
        "/opt/airflow/dags/dag_sofascore_pipeline.py"
    ):
        raise DeploymentError(
            "shared serialized SofaScore pipeline has unexpected file location"
        )
    required_sofa_tasks = (
        "sensor_present",
        "xref_present",
        "e4_present",
        "finalizer_present",
    )
    if any(serialized_sofa.get(key) is not True for key in required_sofa_tasks):
        raise DeploymentError(
            "shared serialized SofaScore pipeline misses FotMob publication tasks"
        )
    sensor_id = "wait_for_fotmob_publication"
    xref_id = "trigger_xref_transforms"
    e4_id = "trigger_e4_transforms"
    finalizer_id = "finalize_fotmob_publication"
    if (
        sensor_id not in set(serialized_sofa.get("xref_upstream") or ())
        or xref_id not in set(serialized_sofa.get("sensor_downstream") or ())
        or set(serialized_sofa.get("finalizer_upstream") or ()) != {sensor_id, e4_id}
        or finalizer_id not in set(serialized_sofa.get("e4_downstream") or ())
        or serialized_sofa.get("finalizer_trigger_rule") != "all_done"
    ):
        raise DeploymentError(
            "shared serialized SofaScore pipeline has unsafe FotMob publication edges"
        )
    serialized_xref = orchestration.get("xref")
    if not isinstance(serialized_xref, Mapping) or not serialized_xref.get("present"):
        raise DeploymentError("shared metadata has no serialized xref DAG")
    if serialized_xref.get("fileloc") != "/opt/airflow/dags/dag_transform_xref.py":
        raise DeploymentError("shared serialized xref DAG has unexpected file location")
    xref_start_id = "start_marker"
    xref_preflight_id = "validate_fotmob_publication_consumer"
    xref_writer_ids = {
        "xref_transforms.xref_team",
        "xref_transforms.xref_referee",
        "xref_transforms.xref_match",
        "xref_transforms.xref_manager",
        "xref_player",
    }
    xref_task_ids = set(serialized_xref.get("task_ids") or ())
    xref_descendants = set(serialized_xref.get("preflight_descendants") or ())
    trigger_rules = serialized_xref.get("task_trigger_rules")
    if (
        serialized_xref.get("start_present") is not True
        or serialized_xref.get("preflight_present") is not True
        or set(serialized_xref.get("start_downstream") or ()) != {xref_preflight_id}
        or set(serialized_xref.get("preflight_upstream") or ()) != {xref_start_id}
        or serialized_xref.get("preflight_trigger_rule") != "all_success"
        or not xref_writer_ids.issubset(xref_task_ids)
        or not xref_writer_ids.issubset(xref_descendants)
        or xref_task_ids - {xref_start_id, xref_preflight_id} != xref_descendants
        or not isinstance(trigger_rules, Mapping)
        or any(
            trigger_rules.get(task_id) != "all_success" for task_id in xref_writer_ids
        )
    ):
        raise DeploymentError(
            "shared serialized xref DAG does not gate every writer behind "
            "the FotMob publication preflight"
        )
    fenced_downstream = orchestration.get("fenced_downstream")
    if not isinstance(fenced_downstream, Mapping):
        raise DeploymentError("shared metadata has no downstream fence evidence")
    downstream_contracts = {
        "dag_transform_e3": {
            "fileloc": "/opt/airflow/dags/dag_transform_e3.py",
            "start": True,
            "first": {"silver_e3.whoscored_events_spadl"},
        },
        "dag_transform_e4": {
            "fileloc": "/opt/airflow/dags/dag_transform_e4.py",
            "start": True,
            "first": {"silver_e4.matchhistory_match_odds"},
        },
        "dag_transform_fbref_gold": {
            "fileloc": "/opt/airflow/dags/dag_transform_fbref_gold.py",
            "start": False,
            "first": {"transfermarkt_reader_precondition"},
        },
    }
    for dag_id, contract in downstream_contracts.items():
        proof = fenced_downstream.get(dag_id)
        if not isinstance(proof, Mapping):
            raise DeploymentError(f"shared metadata has no serialized {dag_id} DAG")
        task_ids = set(proof.get("task_ids") or ())
        descendants = set(proof.get("preflight_descendants") or ())
        preflight_id = "validate_fotmob_publication_consumer"
        excluded = {preflight_id}
        expected_upstream: set[str] = set()
        if contract["start"]:
            excluded.add("start_marker")
            expected_upstream.add("start_marker")
        direct_rules = proof.get("direct_downstream_trigger_rules")
        if (
            proof.get("present") is not True
            or proof.get("fileloc") != contract["fileloc"]
            or proof.get("preflight_present") is not True
            or proof.get("preflight_trigger_rule") != "all_success"
            or set(proof.get("preflight_upstream") or ()) != expected_upstream
            or set(proof.get("preflight_downstream") or ()) != contract["first"]
            or task_ids - excluded != descendants
            or not isinstance(direct_rules, Mapping)
            or any(
                direct_rules.get(task_id) != "all_success"
                for task_id in contract["first"]
            )
            or (
                contract["start"]
                and (
                    proof.get("start_present") is not True
                    or set(proof.get("start_downstream") or ()) != {preflight_id}
                )
            )
            or (not contract["start"] and proof.get("start_present") is True)
        ):
            raise DeploymentError(
                f"shared serialized {dag_id} does not place the FotMob "
                "publication preflight before every downstream task"
            )
    pause_states = orchestration.get("pause_states")
    expected_pause_states = {
        "dag_master_pipeline": True,
        "dag_sofascore_pipeline": True,
        "dag_ingest_fotmob": True,
        "dag_transform_fotmob_silver": True,
    }
    if (
        not isinstance(pause_states, Mapping)
        or {dag_id: pause_states.get(dag_id) for dag_id in expected_pause_states}
        != expected_pause_states
    ):
        raise DeploymentError(
            "shared orchestration must keep master/SofaScore/ingest/Silver paused"
        )
    sofascore_schedule_boundary = validate_schedule_boundary(
        orchestration.get("sofascore_schedule_boundary"),
        label=f"shared {SHARED_CONSUMER_DAG_ID}",
    )
    active_rows = orchestration.get("active_runs")
    if not isinstance(active_rows, list) or any(
        not isinstance(row, Mapping) for row in active_rows
    ):
        raise DeploymentError("shared orchestration active-run evidence is invalid")
    if active_rows:
        raise DeploymentError(
            "shared scheduler still has active master/Sofa/FotMob/xref/E3/E4/Gold/"
            "isolated-daily runs: "
            f"{active_rows!r}"
        )
    shared_daily = orchestration.get("shared_daily_trigger")
    if (
        not isinstance(shared_daily, Mapping)
        or shared_daily.get("isolated_stack_env") not in {None, ""}
        or (
            shared_daily.get("serialized_present") is True
            and shared_daily.get("serialized_fileloc")
            != "/opt/airflow/dags/dag_trigger_fotmob_daily.py"
        )
        or (
            shared_daily.get("serialized_present") is True
            and shared_daily.get("dag_model_present") is not True
        )
        or (
            shared_daily.get("dag_model_present") is True
            and shared_daily.get("dag_model_paused") is not True
        )
    ):
        raise DeploymentError(
            "shared isolated daily trigger must be absent or a paused stale row"
        )
    owner = str(orchestration.get("schedule_owner", "")).strip().lower()
    if owner != "isolated":
        raise DeploymentError(
            "shared Airflow Variable fotmob_schedule_owner must equal 'isolated'"
        )
    checked_runs = {
        dag_id: {state: [] for state in ACTIVE_STATES}
        for dag_id in (
            "dag_master_pipeline",
            "dag_sofascore_pipeline",
            "dag_ingest_fotmob",
            "dag_transform_fotmob_silver",
            "dag_transform_xref",
            "dag_transform_e3",
            "dag_transform_e4",
            "dag_transform_fbref_gold",
            "dag_trigger_fotmob_daily",
        )
    }
    return {
        "shared_scheduler_container": shared_container_id,
        "shared_admission_mount": shared_admission_mount,
        "master_dag_sha256": expected_hash,
        "remote_master_dag_sha256": remote_hash,
        "runtime_code_sha256": shared_runtime_hashes,
        "runtime_git_sha": shared_runtime_sha,
        "serialized_master": serialized,
        "serialized_sofascore": serialized_sofa,
        "serialized_xref": serialized_xref,
        "serialized_downstream": dict(fenced_downstream),
        "next_scheduled_interval": sofascore_schedule_boundary,
        "orchestration_state": {
            "pause_states": dict(pause_states),
            "expected_pause_states": expected_pause_states,
            "active_runs": [],
            "atomic_metadata_snapshot": True,
            "shared_daily_trigger": dict(shared_daily),
        },
        "schedule_owner": owner,
        "active_run_checks": checked_runs,
        "control_database": control_database,
        "passed": True,
    }


def validate_stable_shared_handoff(
    initial: Mapping[str, Any], final: Mapping[str, Any]
) -> None:
    if (
        initial.get("shared_scheduler_container")
        != final.get("shared_scheduler_container")
        or initial.get("shared_admission_mount") != final.get("shared_admission_mount")
        or initial.get("runtime_code_sha256") != final.get("runtime_code_sha256")
        or initial.get("next_scheduled_interval")
        != final.get("next_scheduled_interval")
    ):
        raise DeploymentError("shared handoff identity changed during admission")


def build_parser() -> argparse.ArgumentParser:
    default_compose = Path(__file__).resolve().with_name("airflow.compose.yaml")
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--release-root", type=Path, required=True)
    parser.add_argument("--env-file", type=Path, required=True)
    parser.add_argument("--image", required=True)
    parser.add_argument(
        "--postgres-image",
        required=True,
        help="PostgreSQL metadata image pinned by full sha256 digest",
    )
    parser.add_argument("--evidence-dir", type=Path, required=True)
    parser.add_argument("--compose-file", type=Path, default=default_compose)
    parser.add_argument("--project", default="fotmob-airflow")
    parser.add_argument(
        "--shared-scheduler-container",
        default="airflow-scheduler",
        help="Shared scheduler container whose ownership handoff must be proven",
    )
    parser.add_argument(
        "--keep-paused",
        action="store_true",
        help="Admit the release but keep every DAG paused (required for rollback)",
    )
    parser.add_argument(
        "--resume-pending",
        action="store_true",
        help="Idempotently finish an admitted pending_consumer activation",
    )
    parser.add_argument("--timeout-seconds", type=int, default=300)
    parser.add_argument("--report", type=Path)
    return parser


def _pending_report_error(
    report_path: Path,
    pending: Mapping[str, Any],
    exc: Exception,
) -> PendingConsumerError:
    failed_pending = {
        **pending,
        "generated_at": _now(),
        "activation_state": "pending_consumer",
        "scheduled_activation": {
            **dict(pending.get("scheduled_activation") or {}),
            "status": "pending",
            "resume_required": True,
            "last_error": f"{type(exc).__name__}: {exc}",
        },
    }
    try:
        _atomic_json(report_path, failed_pending)
    except Exception:
        pass
    return PendingConsumerError(failed_pending, exc)


def _validate_report_identity(
    args: argparse.Namespace,
    payload: Mapping[str, Any],
    *,
    run: Callable[..., subprocess.CompletedProcess[str]],
) -> tuple[Path, str, str]:
    report_path = (args.report or args.evidence_dir / "deployment.json").resolve()
    release_root = args.release_root.resolve()
    evidence_dir = args.evidence_dir.resolve()
    try:
        report_relative_path = report_path.relative_to(evidence_dir)
    except ValueError as exc:
        raise DeploymentError(
            "pending report is outside its admitted evidence directory"
        ) from exc
    if not report_relative_path.parts or ".." in report_relative_path.parts:
        raise DeploymentError("pending report has an invalid evidence-relative path")
    expected_container_report = str(CONTAINER_EVIDENCE_ROOT / report_relative_path)
    expected_shared_report = str(SHARED_CONTAINER_EVIDENCE_ROOT / report_relative_path)
    if (
        payload.get("container_report_path") != expected_container_report
        or payload.get("shared_container_report_path") != expected_shared_report
    ):
        raise DeploymentError(
            "pending host report path differs from its admitted container paths"
        )
    expected_shared_mount = {
        "type": "bind",
        "source": str(evidence_dir),
        "destination": str(SHARED_CONTAINER_EVIDENCE_ROOT),
        "read_only": True,
        "report_path": expected_shared_report,
    }
    for name in ("shared_handoff_initial", "shared_handoff_final"):
        handoff = payload.get(name)
        mount = (
            handoff.get("shared_admission_mount")
            if isinstance(handoff, Mapping)
            else None
        )
        if mount != expected_shared_mount:
            raise DeploymentError(
                f"pending {name} report mount differs from its admitted path"
            )
    expected = {
        "project": args.project,
        "compose_file": str(args.compose_file.resolve()),
        "release_root": str(release_root),
        "evidence_dir": str(evidence_dir),
        "image": args.image,
        "postgres_image": args.postgres_image,
        "git_sha": release_sha(release_root, run),
    }
    if any(str(payload.get(key)) != value for key, value in expected.items()):
        raise DeploymentError("pending activation arguments differ from its admission")
    isolated_container = str(payload.get("scheduler_container_id") or "")
    final_handoff = payload.get("shared_handoff_final")
    shared_container = (
        str(final_handoff.get("shared_scheduler_container") or "")
        if isinstance(final_handoff, Mapping)
        else ""
    )
    for label, container in (
        ("isolated", isolated_container),
        ("shared", shared_container),
    ):
        if re.fullmatch(r"[0-9a-f]{64}", container) is None:
            raise DeploymentError(f"pending activation has no exact {label} container")
    return report_path, isolated_container, shared_container


def _validate_resume_identity(
    args: argparse.Namespace,
    payload: Mapping[str, Any],
    *,
    run: Callable[..., subprocess.CompletedProcess[str]],
) -> tuple[Path, str, str]:
    report_path, isolated_container, shared_container = _validate_report_identity(
        args, payload, run=run
    )
    for label, container in (
        ("isolated", isolated_container),
        ("shared", shared_container),
    ):
        observed = run(
            ("docker", "inspect", "--format", "{{.Id}}", container),
            check=True,
            capture_output=True,
            text=True,
        ).stdout.strip()
        if observed != container:
            raise DeploymentError(f"pending {label} scheduler container was replaced")
    return report_path, isolated_container, shared_container


def _validated_commit_boundary(payload: Mapping[str, Any]) -> dict[str, str]:
    """Re-derive every immutable boundary before resuming a durable cut."""

    proof = payload.get("schedule_boundary")
    if (
        not isinstance(proof, Mapping)
        or set(proof) != PENDING_PROOF_FIELDS
        or proof.get("shared_dag_id") != SHARED_CONSUMER_DAG_ID
        or proof.get("isolated_dag_id") != ISOLATED_DAILY_DAG_ID
        or proof.get("exact_match") is not True
    ):
        raise DeploymentError("activation report has no commit-edge schedule proof")
    validated = validate_matching_schedule_boundaries(
        shared_initial=proof.get("shared_initial"),
        shared_final=proof.get("shared_final"),
        isolated_initial=proof.get("isolated_initial"),
        isolated_final=proof.get("isolated_final"),
        shared_commit=proof.get("shared_commit"),
        isolated_commit=proof.get("isolated_commit"),
    )
    return dict(validated["isolated_commit"])


def _validate_active_scheduled_proof(
    payload: Mapping[str, Any], boundary: Mapping[str, str]
) -> dict[str, dict[str, str]]:
    activation = payload.get("scheduled_activation")
    if (
        not isinstance(activation, Mapping)
        or set(activation) != {"status", "producer", "consumer", "exact_identity_match"}
        or activation.get("status") != "proved"
        or activation.get("exact_identity_match") is not True
    ):
        raise DeploymentError("active report has no exact scheduled handoff proof")
    expected_run_id = _scheduled_run_id(boundary["logical_date"])
    expected_dags = {
        "producer": ISOLATED_DAILY_DAG_ID,
        "consumer": SHARED_CONSUMER_DAG_ID,
    }
    normalized: dict[str, dict[str, str]] = {}
    row_fields = {
        "dag_id",
        "run_id",
        "run_type",
        "logical_date",
        "data_interval_start",
        "data_interval_end",
        "state",
    }
    for role, dag_id in expected_dags.items():
        row = activation.get(role)
        if not isinstance(row, Mapping) or set(row) != row_fields:
            raise DeploymentError(f"active {role} scheduled proof is incomplete")
        observed = validate_schedule_boundary(
            {
                "logical_date": row.get("logical_date"),
                "data_interval_start": row.get("data_interval_start"),
                "data_interval_end": row.get("data_interval_end"),
                "run_after": row.get("data_interval_end"),
            },
            label=f"active {role}",
        )
        if (
            row.get("dag_id") != dag_id
            or row.get("run_id") != expected_run_id
            or row.get("run_type") != "scheduled"
            or str(row.get("state") or "").casefold() not in EXACT_SCHEDULED_RUN_STATES
            or observed != dict(boundary)
        ):
            raise DeploymentError(f"active {role} differs from admitted schedule")
        normalized[role] = {
            "dag_id": dag_id,
            "run_id": expected_run_id,
            "run_type": "scheduled",
            "logical_date": observed["logical_date"],
            "data_interval_start": observed["data_interval_start"],
            "data_interval_end": observed["data_interval_end"],
            "state": str(row["state"]).casefold(),
        }
    identity_fields = (
        "run_id",
        "run_type",
        "logical_date",
        "data_interval_start",
        "data_interval_end",
    )
    if any(
        normalized["producer"][field] != normalized["consumer"][field]
        for field in identity_fields
    ):
        raise DeploymentError("active producer/consumer scheduled identities differ")
    return normalized


def _validate_pending_report(payload: Mapping[str, Any]) -> dict[str, str]:
    activation = payload.get("scheduled_activation")
    activation_fields = set(activation) if isinstance(activation, Mapping) else set()
    allowed_activation_fields = {
        PENDING_ACTIVATION_FIELDS,
        PENDING_ACTIVATION_FIELDS | {"last_error"},
    }
    if (
        payload.get("kept_paused") is not False
        or payload.get("paused") != [ISOLATED_DAILY_DAG_ID]
        or payload.get("unpaused") != sorted(EXPECTED_DAGS - {ISOLATED_DAILY_DAG_ID})
        or not isinstance(activation, Mapping)
        or frozenset(activation_fields) not in allowed_activation_fields
        or activation.get("status") != "pending"
        or activation.get("producer_dag_id") != ISOLATED_DAILY_DAG_ID
        or activation.get("consumer_dag_id") != SHARED_CONSUMER_DAG_ID
        or activation.get("resume_required") is not True
        or (
            "last_error" in activation
            and (
                not isinstance(activation.get("last_error"), str)
                or not activation["last_error"].strip()
            )
        )
    ):
        raise DeploymentError("pending consumer report is not an exact durable cut")
    safety = payload.get("activation_safety_window")
    if (
        not isinstance(safety, Mapping)
        or set(safety) != PENDING_SAFETY_FIELDS
        or safety.get("passed") is not True
        or type(safety.get("timeout_seconds")) is not int
        or type(safety.get("required_seconds")) is not int
        or type(safety.get("remaining_seconds")) is not int
        or safety["timeout_seconds"] < 1
        or safety["required_seconds"]
        < max(
            MIN_ACTIVATION_SAFETY_SECONDS,
            safety["timeout_seconds"] + ACTIVATION_TIMEOUT_MARGIN_SECONDS,
        )
        or safety["remaining_seconds"] < safety["required_seconds"]
    ):
        raise DeploymentError("pending consumer report has no valid safety proof")
    try:
        checked_at = datetime.fromisoformat(
            str(safety.get("checked_at")).strip().replace("Z", "+00:00")
        )
        next_boundary = datetime.fromisoformat(
            str(safety.get("next_boundary")).strip().replace("Z", "+00:00")
        )
    except ValueError as exc:
        raise DeploymentError(
            "pending consumer report has invalid safety timestamps"
        ) from exc
    if (
        not isinstance(safety.get("checked_at"), str)
        or not isinstance(safety.get("next_boundary"), str)
        or checked_at.tzinfo is None
        or checked_at.utcoffset() is None
        or next_boundary.tzinfo is None
        or next_boundary.utcoffset() is None
        or next_boundary <= checked_at
    ):
        raise DeploymentError("pending consumer report has invalid safety timestamps")
    return _validated_commit_boundary(payload)


def resume_pending_activation(
    args: argparse.Namespace,
    *,
    run: Callable[..., subprocess.CompletedProcess[str]] = subprocess.run,
    sleeper: Callable[[float], None] = time.sleep,
    now: datetime | None = None,
) -> dict[str, Any]:
    if args.keep_paused:
        raise DeploymentError("--resume-pending cannot be combined with --keep-paused")
    report_path = (args.report or args.evidence_dir / "deployment.json").resolve()
    try:
        payload = json.loads(report_path.read_text(encoding="utf-8"))
    except (OSError, json.JSONDecodeError) as exc:
        raise DeploymentError(f"cannot read pending deployment report: {exc}") from exc
    if not isinstance(payload, Mapping) or payload.get("schema_version") != (
        "fotmob-deploy-v2"
    ):
        raise DeploymentError("resume requires a fotmob-deploy-v2 report")
    state = payload.get("activation_state")
    if state == "active":
        if (
            payload.get("passed") is not True
            or payload.get("kept_paused") is not False
            or payload.get("paused") != []
            or set(payload.get("unpaused") or ()) != EXPECTED_DAGS
        ):
            raise DeploymentError("active report has no exact scheduled handoff proof")
        expected = _validated_commit_boundary(payload)
        reported = _validate_active_scheduled_proof(payload, expected)
        _report_path, isolated_container, shared_container = _validate_resume_identity(
            args, payload, run=run
        )
        live = {
            "producer": read_exact_scheduled_run(
                isolated_container, ISOLATED_DAILY_DAG_ID, expected, run=run
            ),
            "consumer": read_exact_scheduled_run(
                shared_container, SHARED_CONSUMER_DAG_ID, expected, run=run
            ),
        }
        identity_fields = (
            "dag_id",
            "run_id",
            "run_type",
            "logical_date",
            "data_interval_start",
            "data_interval_end",
        )
        if any(
            live[role] is None
            or any(
                live[role].get(field) != reported[role][field]
                for field in identity_fields
            )
            for role in ("producer", "consumer")
        ):
            raise DeploymentError(
                "active report differs from live exact scheduled handoff rows"
            )
        return dict(payload)
    if state != "pending_consumer" or payload.get("passed") is not True:
        raise DeploymentError("resume requires a green pending_consumer report")
    try:
        report_path, isolated_container, shared_container = _validate_resume_identity(
            args, payload, run=run
        )
        expected = _validate_pending_report(payload)
        producer = read_exact_scheduled_run(
            isolated_container, ISOLATED_DAILY_DAG_ID, expected, run=run
        )
        consumer = read_exact_scheduled_run(
            shared_container, SHARED_CONSUMER_DAG_ID, expected, run=run
        )
        if producer is None:
            current = read_schedule_boundary(
                isolated_container,
                ISOLATED_DAILY_DAG_ID,
                run=run,
                require_paused=False,
            )
            if current != expected:
                raise DeploymentError("pending producer next interval advanced")
        if consumer is None:
            current = read_schedule_boundary(
                shared_container,
                SHARED_CONSUMER_DAG_ID,
                run=run,
                require_paused=False,
            )
            if current != expected:
                raise DeploymentError("pending consumer next interval advanced")
        if producer is None or consumer is None:
            validate_activation_safety_window(
                expected, timeout_seconds=args.timeout_seconds, now=now
            )
        return _continue_pending_consumer_activation(
            report_path,
            payload,
            isolated_container=isolated_container,
            shared_container=shared_container,
            timeout_seconds=args.timeout_seconds,
            run=run,
            sleeper=sleeper,
        )
    except PendingConsumerError:
        raise
    except Exception as exc:
        raise _pending_report_error(report_path, payload, exc) from exc


def _guard_existing_pending_activation(report_path: Path) -> None:
    """Never let an ordinary deploy destroy a resumable producer admission."""

    try:
        payload = json.loads(report_path.read_text(encoding="utf-8"))
    except FileNotFoundError:
        return
    except OSError as exc:
        raise DeploymentError(
            "cannot safely read the existing deployment report; leave it unchanged"
        ) from exc
    except json.JSONDecodeError as exc:
        raise DeploymentError(
            "existing deployment report is invalid JSON; leave it unchanged for incident recovery"
        ) from exc
    if (
        not isinstance(payload, Mapping)
        or payload.get("schema_version") != "fotmob-deploy-v2"
        or payload.get("passed") is not True
        or payload.get("activation_state") != "pending_consumer"
    ):
        return
    try:
        _validate_pending_report(payload)
    except DeploymentError as exc:
        raise PendingConsumerError(
            payload,
            DeploymentError(f"existing pending proof is invalid: {exc}"),
            operator_action=(
                "leave producer and reports unchanged; follow the pending-consumer "
                "incident runbook and recovery issue #997"
            ),
        ) from exc
    raise PendingConsumerError(
        payload,
        DeploymentError("a green pending_consumer deployment already exists"),
    )


def _existing_report_before_upgrade(report_path: Path) -> dict[str, Any] | None:
    """Record that any report exists so pre-mutation errors cannot replace it."""

    try:
        report_path.lstat()
    except FileNotFoundError:
        return None
    except OSError:
        return {"activation_state": None}
    try:
        payload = json.loads(report_path.read_text(encoding="utf-8"))
    except (OSError, json.JSONDecodeError):
        return {"activation_state": None}
    return {
        "activation_state": (
            payload.get("activation_state") if isinstance(payload, Mapping) else None
        )
    }


def _mark_runtime_mutation_started(args: argparse.Namespace) -> None:
    setattr(args, _RUNTIME_MUTATION_STARTED_ATTR, True)


def _runtime_mutation_started(args: argparse.Namespace) -> bool:
    return bool(getattr(args, _RUNTIME_MUTATION_STARTED_ATTR, False))


def deploy(
    args: argparse.Namespace,
    *,
    run: Callable[..., subprocess.CompletedProcess[str]] = subprocess.run,
    sleeper: Callable[[float], None] = time.sleep,
) -> dict[str, Any]:
    setattr(args, _RUNTIME_MUTATION_STARTED_ATTR, False)
    evidence_dir = args.evidence_dir.resolve()
    configured_report = getattr(args, "report", None)
    report_path = (
        configured_report.resolve()
        if configured_report is not None
        else evidence_dir / "deployment.json"
    )
    _guard_existing_pending_activation(report_path)
    validate_image_reference(args.image, label="FOTMOB_AIRFLOW_IMAGE")
    validate_image_reference(args.postgres_image, label="FOTMOB_POSTGRES_IMAGE")
    release_root = args.release_root.resolve()
    compose_file = args.compose_file.resolve()
    try:
        report_relative_path = report_path.relative_to(evidence_dir)
    except ValueError as exc:
        raise DeploymentError(
            "--report must be inside --evidence-dir for scheduled runtime attestation"
        ) from exc
    if not report_relative_path.parts:
        raise DeploymentError("--report must name a file inside --evidence-dir")
    container_report_path = CONTAINER_EVIDENCE_ROOT / report_relative_path
    _prepare_evidence_report_path(evidence_dir, report_path)
    sha = release_sha(release_root, run)
    if not args.env_file.is_file():
        raise DeploymentError("--env-file does not exist")
    validate_database_password(args.env_file, os.environ)
    validate_delivery_credentials(args.env_file, os.environ)
    control_db_uri = _configured_env_value(
        args.env_file, os.environ, "FBREF_CONTROL_DB_URI"
    )
    if not control_db_uri:
        raise DeploymentError("FBREF_CONTROL_DB_URI is required")
    if not compose_file.is_file():
        raise DeploymentError("--compose-file does not exist")
    dagbag_root = prepare_dagbag(release_root, evidence_dir, sha)
    initial_handoff = validate_shared_handoff(
        release_root,
        args.shared_scheduler_container,
        control_db_uri,
        evidence_dir=evidence_dir,
        report_relative_path=report_relative_path,
        run=run,
    )
    deployment_id = secrets.token_hex(16)
    environment = dict(os.environ)
    environment.update(
        {
            "FOTMOB_RELEASE_ROOT": str(release_root),
            "FOTMOB_AIRFLOW_IMAGE": args.image,
            "FOTMOB_POSTGRES_IMAGE": args.postgres_image,
            "FOTMOB_EVIDENCE_DIR": str(evidence_dir),
            "FOTMOB_DAGBAG_ROOT": str(dagbag_root),
            "FOTMOB_DEPLOY_GIT_SHA": sha,
            "FOTMOB_DEPLOYMENT_ID": deployment_id,
            "FOTMOB_DEPLOYMENT_REPORT_PATH": str(container_report_path),
        }
    )
    base = (
        "docker",
        "compose",
        "-p",
        args.project,
        "-f",
        str(compose_file),
        "--env-file",
        str(args.env_file.resolve()),
    )

    def command(
        *parts: str,
        capture: bool = False,
        check: bool = True,
    ) -> subprocess.CompletedProcess[str]:
        return run(
            (*base, *parts),
            check=check,
            env=environment,
            capture_output=capture,
            text=True,
        )

    def airflow(*parts: str, check: bool = True) -> subprocess.CompletedProcess[str]:
        return command(
            "exec",
            "-T",
            "airflow-scheduler",
            "airflow",
            *parts,
            capture=True,
            check=check,
        )

    def dag_rows() -> list[dict[str, Any]]:
        return parse_airflow_json(airflow("dags", "list", "--output", "json").stdout)

    def active_runs() -> dict[str, dict[str, list[str]]]:
        active: dict[str, dict[str, list[str]]] = {}
        for dag_id in sorted(EXPECTED_DAGS):
            rows = parse_airflow_json(
                airflow(
                    "dags",
                    "list-runs",
                    "-d",
                    dag_id,
                    "--output",
                    "json",
                ).stdout
            )
            for state in ACTIVE_STATES:
                run_ids = [
                    str(row.get("run_id"))
                    for row in rows
                    if str(row.get("state", "")).lower() == state
                ]
                if run_ids:
                    active.setdefault(dag_id, {})[state] = [*run_ids]
        return active

    def assert_paused(expected: set[str]) -> list[dict[str, Any]]:
        rows = dag_rows()
        validate_dagbag(rows, ())
        observed = _paused_ids(rows)
        if observed != expected:
            raise DeploymentError(
                f"unexpected pause state: expected={sorted(expected)!r}, "
                f"observed={sorted(observed)!r}"
            )
        return rows

    command("config", "--quiet")
    existing_scheduler = command(
        "ps", "--no-trunc", "-q", "airflow-scheduler", capture=True
    ).stdout.strip()
    if existing_scheduler:
        existing_active = active_runs()
        if existing_active:
            raise DeploymentError(
                f"isolated scheduler has active runs; redeploy aborted: {existing_active!r}"
            )
        _mark_runtime_mutation_started(args)
        for dag_id in sorted(EXPECTED_DAGS):
            airflow("dags", "pause", dag_id)
        assert_paused(set(EXPECTED_DAGS))
        post_pause_active = active_runs()
        if post_pause_active:
            raise DeploymentError(
                "isolated runs appeared during redeploy handoff; leave paused and wait: "
                f"{post_pause_active!r}"
            )
        command("stop", "airflow-scheduler")

    launch_attempted = False
    try:
        # ``docker compose up`` is not transactional: it may start the
        # scheduler and still return non-zero because another service failed.
        # Mark the attempt before invoking Compose so the exception path always
        # quiesces any partially-created scheduler.
        launch_attempted = True
        _mark_runtime_mutation_started(args)
        command("up", "-d", "airflow-metadb", "airflow-init", "airflow-scheduler")
        deadline = time.monotonic() + max(1, args.timeout_seconds)
        health_error: str | None = "scheduler health check not attempted"
        while time.monotonic() < deadline:
            try:
                airflow("jobs", "check", "--job-type", "SchedulerJob")
                health_error = None
                break
            except subprocess.CalledProcessError as exc:
                health_error = str(exc)
                sleeper(2)
        if health_error is not None:
            raise DeploymentError(f"scheduler did not become healthy: {health_error}")

        cli_rows = dag_rows()
        import_errors = parse_airflow_json(
            airflow("dags", "list-import-errors", "--output", "json").stdout
        )
        validate_dagbag(cli_rows, import_errors)

        inspection_code = (
            "import json; from airflow.models import DagBag; "
            "b=DagBag(dag_folder='/opt/airflow/dags', include_examples=False, "
            "safe_mode=False); "
            "p={'dags':{i:{'fileloc':d.fileloc,'schedule':str(d.schedule_interval)} "
            "for i,d in b.dags.items()},'import_errors':b.import_errors}; "
            "print('FOTMOB_DAGBAG_JSON='+json.dumps(p,default=str,sort_keys=True))"
        )
        fresh_output = command(
            "exec",
            "-T",
            "airflow-scheduler",
            "python",
            "-c",
            inspection_code,
            capture=True,
        ).stdout
        fresh_payload = parse_marker_json(fresh_output, "FOTMOB_DAGBAG_JSON=")
        if not isinstance(fresh_payload, Mapping):
            raise DeploymentError("fresh DagBag evidence is not an object")
        validate_fresh_dagbag(fresh_payload)

        # airflow-init pauses surviving DagModel rows; prove that fact before
        # any admission transition.
        assert_paused(set(EXPECTED_DAGS))
        if active_runs():
            raise DeploymentError("isolated stack gained active runs while paused")

        # Support modules/configs are host bind mounts rather than image
        # layers. Re-attest the checkout and copied DagBag at the admission
        # edge; a host-side edit during the health wait must fail closed and
        # drive the exception path below.
        if release_sha(release_root, run) != sha:
            raise DeploymentError("release Git SHA changed during admission")
        if prepare_dagbag(release_root, evidence_dir, sha) != dagbag_root:
            raise DeploymentError("DagBag projection changed during admission")

        container_id = command(
            "ps", "--all", "--no-trunc", "-q", "airflow-scheduler", capture=True
        ).stdout.strip()
        if not re.fullmatch(r"[0-9a-f]{64}", container_id):
            raise DeploymentError("cannot resolve admitted scheduler container")
        image_id = run(
            ("docker", "inspect", "--format", "{{.Image}}", container_id),
            check=True,
            capture_output=True,
            text=True,
        ).stdout.strip()
        if not re.fullmatch(r"sha256:[0-9a-fA-F]{64}", image_id):
            raise DeploymentError("cannot resolve immutable scheduler image ID")
        metadb_container_id = command(
            "ps", "--all", "--no-trunc", "-q", "airflow-metadb", capture=True
        ).stdout.strip()
        if not re.fullmatch(r"[0-9a-f]{64}", metadb_container_id):
            raise DeploymentError("cannot resolve admitted metadata DB container")
        postgres_image_id = run(
            (
                "docker",
                "inspect",
                "--format",
                "{{.Image}}",
                metadb_container_id,
            ),
            check=True,
            capture_output=True,
            text=True,
        ).stdout.strip()
        if not re.fullmatch(r"sha256:[0-9a-fA-F]{64}", postgres_image_id):
            raise DeploymentError("cannot resolve immutable PostgreSQL image ID")
        isolated_control_database = validate_control_database(
            container_id, control_db_uri, run=run
        )
        delivery_credentials = validate_delivery_runtime(container_id, run=run)
        isolated_schedule_initial = read_schedule_boundary(
            container_id,
            ISOLATED_DAILY_DAG_ID,
            run=run,
        )
        # Both paused schedulers must already agree before any isolated DAG is
        # unpaused.  The final check below repeats this at the commit boundary.
        validate_matching_schedule_boundaries(
            shared_initial=initial_handoff.get("next_scheduled_interval"),
            shared_final=initial_handoff.get("next_scheduled_interval"),
            isolated_initial=isolated_schedule_initial,
            isolated_final=isolated_schedule_initial,
        )
        marker_create_sql = f"""CREATE TABLE IF NOT EXISTS {RUNTIME_MARKER_TABLE} (
            deployment_id VARCHAR,
            git_sha VARCHAR,
            scheduler_container_id VARCHAR,
            scheduler_image_id VARCHAR,
            admitted_at TIMESTAMP(6) WITH TIME ZONE
        )"""
        marker_insert_sql = f"""INSERT INTO {RUNTIME_MARKER_TABLE}
            (deployment_id, git_sha, scheduler_container_id, scheduler_image_id, admitted_at)
        VALUES ('{deployment_id}', '{sha}', '{container_id}', '{image_id}', current_timestamp)"""
        marker_count_sql = f"""SELECT COUNT(*) FROM {RUNTIME_MARKER_TABLE}
        WHERE deployment_id = '{deployment_id}' AND git_sha = '{sha}'
          AND scheduler_container_id = '{container_id}'
          AND scheduler_image_id = '{image_id}'"""
        marker_code = (
            "import json,os; import trino; from trino.auth import BasicAuthentication; "
            "u=os.environ.get('TRINO_USER','airflow'); p=os.environ.get('TRINO_PASSWORD',''); "
            "k={'host':os.environ['TRINO_HOST'],"
            "'port':int(os.environ.get('TRINO_PORT','8443')),"
            "'user':u,'catalog':'iceberg','schema':'bronze',"
            "'http_scheme':os.environ.get('TRINO_HTTP_SCHEME','https'),"
            "'verify':os.environ.get('TRINO_TLS_VERIFY','true').lower() not in "
            "{'0','false','no'}}; "
            "k.update({'auth':BasicAuthentication(u,p)} if p else {}); "
            "c=trino.dbapi.connect(**k); q=c.cursor(); "
            f"q.execute({marker_create_sql!r}); q.fetchall(); "
            f"q.execute({marker_insert_sql!r}); q.fetchall(); "
            f"q.execute({marker_count_sql!r}); n=int(q.fetchone()[0]); "
            "q.close(); c.close(); "
            "print('FOTMOB_RUNTIME_MARKER_JSON='+json.dumps({'count':n}))"
        )
        marker_output = command(
            "exec",
            "-T",
            "airflow-scheduler",
            "python",
            "-c",
            marker_code,
            capture=True,
        ).stdout
        marker_result = parse_marker_json(marker_output, "FOTMOB_RUNTIME_MARKER_JSON=")
        if not isinstance(marker_result, Mapping) or marker_result.get("count") != 1:
            raise DeploymentError("durable Trino deployment marker was not admitted")
        data_plane_marker = {
            "table": RUNTIME_MARKER_TABLE,
            "deployment_id": deployment_id,
            "git_sha": sha,
            "scheduler_container_id": container_id,
            "scheduler_image_id": image_id,
        }
        if not args.keep_paused:
            for dag_id in (
                "dag_ingest_fotmob",
                "dag_transform_fotmob_silver",
            ):
                airflow("dags", "unpause", dag_id)
            assert_paused({"dag_trigger_fotmob_daily"})
            if active_runs():
                raise DeploymentError(
                    "isolated stack gained an active run before schedule admission"
                )
        isolated_runtime_hashes = validate_isolated_runtime_manifest(
            container_id,
            expected_isolated_runtime_manifest(release_root, dagbag_root),
            run=run,
        )
        isolated_schedule_final = read_schedule_boundary(
            container_id,
            ISOLATED_DAILY_DAG_ID,
            run=run,
        )
        # The second shared snapshot is the final handoff edge, not a copied
        # preflight result.  Take it only after the durable marker and exact
        # isolated runtime/schedule checks have completed.
        final_handoff = validate_shared_handoff(
            release_root,
            args.shared_scheduler_container,
            control_db_uri,
            evidence_dir=evidence_dir,
            report_relative_path=report_relative_path,
            run=run,
        )
        validate_stable_shared_handoff(initial_handoff, final_handoff)
        if release_sha(release_root, run) != sha:
            raise DeploymentError("release Git SHA changed before final admission")
        if prepare_dagbag(release_root, evidence_dir, sha) != dagbag_root:
            raise DeploymentError("DagBag projection changed before final admission")
        schedule_boundary = validate_matching_schedule_boundaries(
            shared_initial=initial_handoff.get("next_scheduled_interval"),
            shared_final=final_handoff.get("next_scheduled_interval"),
            isolated_initial=isolated_schedule_initial,
            isolated_final=isolated_schedule_final,
        )
        report = {
            "schema_version": "fotmob-deploy-v2",
            "generated_at": _now(),
            "passed": True,
            "project": args.project,
            "compose_file": str(compose_file),
            "release_root": str(release_root),
            "evidence_dir": str(evidence_dir),
            "container_report_path": str(container_report_path),
            "shared_container_report_path": str(
                SHARED_CONTAINER_EVIDENCE_ROOT / report_relative_path
            ),
            "dagbag_root": str(dagbag_root),
            "git_sha": sha,
            "deployment_id": deployment_id,
            "image": args.image,
            "postgres_image": args.postgres_image,
            "resolved_image_id": image_id,
            "resolved_postgres_image_id": postgres_image_id,
            "scheduler_container_id": container_id,
            "metadb_container_id": metadb_container_id,
            "data_plane_marker": data_plane_marker,
            "delivery_credentials": delivery_credentials,
            "isolated_runtime_sha256": isolated_runtime_hashes,
            "control_database": {
                "shared": initial_handoff["control_database"],
                "isolated": isolated_control_database,
                "same_runtime_configuration": True,
            },
            "dags": sorted(EXPECTED_DAGS),
            "fresh_dagbag": fresh_payload,
            "import_errors": 0,
            "shared_handoff_initial": initial_handoff,
            "shared_handoff_final": final_handoff,
            "schedule_boundary": schedule_boundary,
        }
        if args.keep_paused:
            kept_paused = {
                **report,
                "activation_state": "kept_paused",
                "kept_paused": True,
                "paused": sorted(EXPECTED_DAGS),
                "unpaused": [],
            }
            _atomic_json(report_path, kept_paused)
            return kept_paused
        return _commit_trigger_activation(
            report_path,
            report,
            isolated_container=container_id,
            shared_container=str(final_handoff["shared_scheduler_container"]),
            timeout_seconds=args.timeout_seconds,
            run=run,
            sleeper=sleeper,
        )
    except PendingConsumerError:
        # The producer may already own/write its exact generation.  Pausing or
        # stopping it here would turn a retryable consumer handoff into an
        # ambiguous writer failure.  Resume owns this state.
        raise
    except Exception as exc:
        if launch_attempted:
            for dag_id in sorted(EXPECTED_DAGS):
                try:
                    airflow("dags", "pause", dag_id, check=False)
                except Exception:
                    pass
            try:
                # Best-effort confirmation is useful in command logs even
                # though the scheduler is stopped unconditionally below.
                assert_paused(set(EXPECTED_DAGS))
            except Exception:
                pass
            # An already-created LocalExecutor task can keep running after a
            # successful metadata pause. A failed admission never leaves the
            # scheduler alive, irrespective of pause command outcome.
            try:
                command("stop", "airflow-scheduler", check=False)
            except Exception:
                pass
        failure_report = {
            "schema_version": "fotmob-deploy-v2",
            "generated_at": _now(),
            "passed": False,
            "activation_state": "failed",
            "project": args.project,
            "compose_file": str(compose_file),
            "release_root": str(release_root),
            "evidence_dir": str(evidence_dir),
            "git_sha": sha,
            "deployment_id": deployment_id,
            "error": f"{type(exc).__name__}: {exc}",
        }
        try:
            _atomic_json(report_path, failure_report)
        except Exception:
            # Preserve the original admission failure.  main() makes a second
            # best-effort report write, while all schedulers are already safe.
            pass
        raise


def _main_locked(args: argparse.Namespace) -> int:
    report_path = args.report or args.evidence_dir / "deployment.json"
    setattr(args, _RUNTIME_MUTATION_STARTED_ATTR, False)
    previous_report = (
        None if args.resume_pending else _existing_report_before_upgrade(report_path)
    )
    try:
        if not args.resume_pending:
            _guard_existing_pending_activation(report_path)
        report = (
            resume_pending_activation(args) if args.resume_pending else deploy(args)
        )
    except PendingConsumerError as exc:
        # The durable pending report is intentionally preserved verbatim.  A
        # generic red report or scheduler stop would destroy resumability.
        output = {
            **exc.report,
            "operator_action": exc.operator_action,
        }
        print(json.dumps(output, ensure_ascii=False, sort_keys=True))
        return 1
    except Exception as exc:
        if args.resume_pending:
            print(
                json.dumps(
                    {
                        "schema_version": "fotmob-deploy-v2",
                        "generated_at": _now(),
                        "passed": False,
                        "error": f"{type(exc).__name__}: {exc}",
                    },
                    ensure_ascii=False,
                    sort_keys=True,
                )
            )
            return 1
        if previous_report is not None and not _runtime_mutation_started(args):
            print(
                json.dumps(
                    {
                        "schema_version": "fotmob-deploy-v2",
                        "generated_at": _now(),
                        "passed": False,
                        "existing_report_preserved": True,
                        "previous_activation_state": previous_report.get(
                            "activation_state"
                        ),
                        "error": f"{type(exc).__name__}: {exc}",
                    },
                    ensure_ascii=False,
                    sort_keys=True,
                )
            )
            return 1
        report = {
            "schema_version": "fotmob-deploy-v2",
            "generated_at": _now(),
            "passed": False,
            "error": f"{type(exc).__name__}: {exc}",
        }
    _atomic_json(report_path, report)
    print(json.dumps(report, ensure_ascii=False, sort_keys=True))
    return 0 if report.get("passed") is True else 1


def main(argv: Sequence[str] | None = None) -> int:
    args = build_parser().parse_args(argv)
    try:
        with _deployment_invocation_lock(args.evidence_dir):
            return _main_locked(args)
    except DeploymentError as exc:
        print(
            json.dumps(
                {
                    "schema_version": "fotmob-deploy-v2",
                    "generated_at": _now(),
                    "passed": False,
                    "existing_report_preserved": True,
                    "error": f"{type(exc).__name__}: {exc}",
                },
                ensure_ascii=False,
                sort_keys=True,
            )
        )
        return 1


if __name__ == "__main__":
    raise SystemExit(main())
