#!/usr/bin/env python3
"""Deploy and admit the isolated FotMob Airflow stack.

Admission is deliberately fail-closed: the scheduler must be healthy, its
DagBag must contain exactly the six FotMob DAGs, and import errors must be empty
before any DAG is unpaused.  A JSON report is written for every attempt.
"""

from __future__ import annotations

import argparse
import fcntl
import hashlib
import importlib
import json
import os
import re
import secrets
import shutil
import stat
import subprocess
import sys
import tempfile
import time
from contextlib import contextmanager
from datetime import date, datetime, timedelta, timezone
from pathlib import Path
from typing import Any, Callable, Mapping, Sequence

REPOSITORY_ROOT = Path(__file__).resolve().parents[2]
if str(REPOSITORY_ROOT) not in sys.path:
    sys.path.insert(0, str(REPOSITORY_ROOT))

runtime_binding = importlib.import_module("scripts.fotmob_runtime")


EXPECTED_DAGS = frozenset(
    {
        "dag_orchestrate_fotmob",
        "dag_ingest_fotmob",
        "dag_transform_fotmob_silver",
        "dag_trigger_fotmob_daily",
        "dag_refresh_fotmob",
        "dag_backfill_fotmob",
    }
)
EXPECTED_DAG_FILES = {
    "dag_orchestrate_fotmob": "/opt/airflow/dags/dag_orchestrate_fotmob.py",
    "dag_ingest_fotmob": "/opt/airflow/dags/dag_ingest_fotmob.py",
    "dag_transform_fotmob_silver": ("/opt/airflow/dags/dag_transform_fotmob_silver.py"),
    "dag_trigger_fotmob_daily": "/opt/airflow/dags/dag_trigger_fotmob_daily.py",
    "dag_refresh_fotmob": "/opt/airflow/dags/dag_refresh_fotmob.py",
    "dag_backfill_fotmob": "/opt/airflow/dags/dag_backfill_fotmob.py",
}
EXPECTED_SCHEDULES = {
    "dag_orchestrate_fotmob": "*/5 * * * *",
    "dag_ingest_fotmob": "None",
    "dag_transform_fotmob_silver": "None",
    "dag_trigger_fotmob_daily": "None",
    "dag_refresh_fotmob": "None",
    "dag_backfill_fotmob": "None",
}
AUTOMATIC_OWNER_DAG_ID = "dag_orchestrate_fotmob"
LEGACY_OWNER_DAGS = frozenset(
    {"dag_trigger_fotmob_daily", "dag_refresh_fotmob", "dag_backfill_fotmob"}
)
AUTOMATIC_ACTIVE_DAGS = frozenset(
    {AUTOMATIC_OWNER_DAG_ID, "dag_ingest_fotmob", "dag_transform_fotmob_silver"}
)
AUTOMATIC_CANARY_SCHEMA = "fotmob-automatic-canary-v1"
AUTOMATIC_ROLLOUT_SCHEMA = "fotmob-automatic-rollout-v1"
COORDINATOR_ROLLOUT_SCHEMA = "fotmob-coordinator-rollout-v1"
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
    "dags/dag_orchestrate_fotmob.py",
    "dags/dag_ingest_fotmob.py",
    "dags/dag_transform_fotmob_silver.py",
    "dags/dag_trigger_fotmob_daily.py",
    "dags/dag_refresh_fotmob.py",
    "dags/dag_backfill_fotmob.py",
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
    "dags/dag_orchestrate_fotmob.py",
    "dags/dag_refresh_fotmob.py",
    "dags/dag_backfill_fotmob.py",
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
    "dags/utils/fotmob_orchestration.py",
    "dags/utils/maintenance_tasks.py",
    "dags/utils/silver_tasks.py",
    "dags/utils/xref_player_resolver.py",
    "scrapers/base/iceberg_writer.py",
    "scrapers/base/trino_manager.py",
    "scrapers/fbref/control/store.py",
    "scrapers/fotmob/constants.py",
    "scrapers/fotmob/catalog.py",
    "scrapers/fotmob/catalog_contract.py",
    "scrapers/fotmob/domain.py",
    "scrapers/fotmob/raw_store.py",
    "scrapers/fotmob/repository.py",
    "scrapers/fotmob/service.py",
    "scrapers/fotmob/scope_codec.py",
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


def validate_automatic_catalog_admission(value: Mapping[str, Any]) -> dict[str, Any]:
    """Expose the runtime gate with deploy-specific error semantics."""

    try:
        return runtime_binding.validate_automatic_catalog_admission(value)
    except runtime_binding.RuntimeBindingError as exc:
        raise DeploymentError(str(exc)) from exc


def _unique_json_object(pairs: list[tuple[str, Any]]) -> dict[str, Any]:
    value: dict[str, Any] = {}
    for key, item in pairs:
        if key in value:
            raise DeploymentError(f"duplicate JSON key in rollout evidence: {key!r}")
        value[key] = item
    return value


def load_automatic_canary_report(
    path: Path,
    *,
    evidence_dir: Path,
    deployment: Mapping[str, Any],
) -> dict[str, Any]:
    """Load one protected canary bound to this exact deployment."""

    absolute = path.resolve()
    try:
        relative = absolute.relative_to(evidence_dir.resolve())
    except ValueError as exc:
        raise DeploymentError("automatic canary report must be inside evidence dir") from exc
    if not relative.parts or absolute == (evidence_dir / "deployment.json").resolve():
        raise DeploymentError("automatic canary report path is unsafe")
    try:
        entry = path.lstat()
        raw = path.read_bytes()
        after = path.lstat()
    except OSError as exc:
        raise DeploymentError(f"cannot read automatic canary report: {exc}") from exc
    if (
        stat.S_ISLNK(entry.st_mode)
        or not stat.S_ISREG(entry.st_mode)
        or entry.st_nlink != 1
        or entry.st_dev != after.st_dev
        or entry.st_ino != after.st_ino
        or entry.st_size != after.st_size
        or entry.st_mtime_ns != after.st_mtime_ns
        or not raw
        or len(raw) != entry.st_size
        or len(raw) > 64 * 1024 * 1024
        or stat.S_IMODE(entry.st_mode) & 0o022
    ):
        raise DeploymentError("automatic canary report is not a protected regular file")
    try:
        payload = json.loads(raw.decode("utf-8"), object_pairs_hook=_unique_json_object)
    except (UnicodeDecodeError, json.JSONDecodeError) as exc:
        raise DeploymentError(f"automatic canary report is invalid JSON: {exc}") from exc
    publication = payload.get("publication") if isinstance(payload, Mapping) else None
    binding = publication.get("binding") if isinstance(publication, Mapping) else None
    final_publication = (
        payload.get("final_publication") if isinstance(payload, Mapping) else None
    )
    current_run_reports = (
        payload.get("current_run_reports") if isinstance(payload, Mapping) else None
    )
    current_run_report = (
        current_run_reports[0]
        if isinstance(current_run_reports, list)
        and len(current_run_reports) == 1
        and isinstance(current_run_reports[0], Mapping)
        else None
    )
    if (
        not isinstance(payload, dict)
        or payload.get("schema_version") != AUTOMATIC_CANARY_SCHEMA
        or payload.get("passed") is not True
        or payload.get("phase") != "abandoned"
        or payload.get("recovery_required") is not False
        or payload.get("mode") != "automatic-canary"
        or payload.get("deployment_id") != deployment.get("deployment_id")
        or payload.get("git_sha") != deployment.get("git_sha")
        or payload.get("scheduler_container_id")
        != deployment.get("scheduler_container_id")
        or not isinstance(binding, Mapping)
        or binding.get("runtime_fingerprint") != deployment.get("git_sha")
        or publication.get("generation_id") != payload.get("generation_id")
        or payload.get("ingest_run_state") != "success"
        or payload.get("silver_run_state") != "success"
        or not isinstance(final_publication, Mapping)
        or final_publication.get("generation_id") != payload.get("generation_id")
        or final_publication.get("phase") != "abandoned"
        or final_publication.get("active") is not False
        or final_publication.get("released") is not True
        or final_publication.get("published") is not False
        or not isinstance(current_run_report, Mapping)
        or current_run_report.get("run_id") != payload.get("generation_id")
    ):
        raise DeploymentError("automatic canary is not bound to this deployment")
    return payload


def build_automatic_catalog_admission(
    deployment: Mapping[str, Any],
    canary: Mapping[str, Any],
    *,
    writer_snapshot: Mapping[str, Any],
    scope_observations: Mapping[str, Any],
    validated_at: str | None = None,
) -> dict[str, Any]:
    """Compose and validate the only certificate that may enable the owner."""

    candidate = canary.get("candidate")
    if not isinstance(candidate, Mapping):
        raise DeploymentError("automatic canary has no Silver candidate")
    if any(
        canary.get(key) != deployment.get(key)
        for key in ("deployment_id", "git_sha", "scheduler_container_id")
    ):
        raise DeploymentError("automatic canary deployment identity differs")
    admission = {
        "schema_version": runtime_binding.AUTOMATIC_ADMISSION_SCHEMA,
        "validated_at": validated_at or _now(),
        "classifier_version": runtime_binding.AUTOMATIC_CLASSIFIER_VERSION,
        "contract_schema": runtime_binding.AUTOMATIC_CONTRACT_SCHEMA,
        "writer_snapshot": dict(writer_snapshot),
        "scope_observations": dict(scope_observations),
        "legacy_owners": {
            dag_id: {"schedule": None, "is_paused": True}
            for dag_id in sorted(LEGACY_OWNER_DAGS)
        },
        "lane_budgets": {
            lane: {"max_proxy_mib": 0, "max_proxy_bytes": 0}
            for lane in sorted(runtime_binding.AUTOMATIC_LANES)
        },
        "active_writers": [],
        "current_run_reports": [dict(canary["current_run_reports"][0])],
        "canary": {
            "schema_version": AUTOMATIC_CANARY_SCHEMA,
            "deployment_id": canary["deployment_id"],
            "git_sha": canary["git_sha"],
            "scheduler_container_id": canary["scheduler_container_id"],
            "generation_id": canary["generation_id"],
            "ingest_run_state": canary["ingest_run_state"],
            "silver_run_state": canary["silver_run_state"],
            "candidate_digest": canary["candidate_digest"],
            "runner_report_sha256": canary["runner_report_sha256"],
            "publication": dict(canary["publication"]),
            "final_publication": dict(canary["final_publication"]),
        },
    }
    validate_automatic_catalog_admission(admission)
    return admission


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
        "dag_orchestrate_fotmob.py": release_root
        / "dags/dag_orchestrate_fotmob.py",
        "dag_ingest_fotmob.py": release_root / "dags/dag_ingest_fotmob.py",
        "dag_transform_fotmob_silver.py": release_root
        / "dags/dag_transform_fotmob_silver.py",
        "dag_trigger_fotmob_daily.py": release_root
        / "dags/dag_trigger_fotmob_daily.py",
        "dag_refresh_fotmob.py": release_root / "dags/dag_refresh_fotmob.py",
        "dag_backfill_fotmob.py": release_root / "dags/dag_backfill_fotmob.py",
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


def bootstrap_automatic_scope_observations(
    container_id: str,
    *,
    run: Callable[..., subprocess.CompletedProcess[str]],
) -> dict[str, Any]:
    """Create and prove the durable structural-decision table and current view."""

    marker = "FOTMOB_AUTOMATIC_SCOPE_JSON="
    table = runtime_binding.SCOPE_OBSERVATIONS_TABLE
    current_view = runtime_binding.SCOPE_OBSERVATIONS_CURRENT_VIEW
    code = (
        "import json; "
        "from scrapers.fotmob.repository import FotMobRepository; "
        "r=FotMobRepository(); r.ensure_schema(); created=r.ensure_current_views(); "
        "t=r.writer._get_trino_manager(); "
        f"p={{'table':{table!r},'table_exists':bool(t.table_exists(r.schema,{table!r})),"
        f"'current_view':{current_view!r},"
        f"'current_view_exists':f'{{r.catalog}}.{{r.schema}}.{current_view}' in created}}; "
        f"print('{marker}'+json.dumps(p,sort_keys=True))"
    )
    output = run(
        ("docker", "exec", container_id, "python", "-c", code),
        check=True,
        capture_output=True,
        text=True,
    ).stdout
    payload = parse_marker_json(output, marker)
    if (
        not isinstance(payload, Mapping)
        or payload.get("table") != table
        or payload.get("current_view") != current_view
        or payload.get("table_exists") is not True
        or payload.get("current_view_exists") is not True
    ):
        raise DeploymentError(
            "automatic scope-observation table/current view bootstrap failed"
        )
    return dict(payload)


def collect_automatic_scope_observations(
    container_id: str,
    runner_report: Mapping[str, Any],
    *,
    run: Callable[..., subprocess.CompletedProcess[str]],
) -> dict[str, Any]:
    """Read the durable current catalog and decision content for one canary."""

    selection = runner_report.get("selection")
    if not isinstance(selection, Mapping):
        raise DeploymentError("automatic canary has no selection evidence")
    try:
        from scrapers.fotmob.catalog_contract import catalog_contract_from_dict

        contract = catalog_contract_from_dict(selection.get("catalog_contract"))
        catalog_ids = [int(value) for value in selection.get("catalog_ids") or ()]
    except (TypeError, ValueError) as exc:
        raise DeploymentError("automatic canary contract is invalid") from exc
    if catalog_ids != sorted(set(catalog_ids)) or not catalog_ids:
        raise DeploymentError("automatic canary catalog IDs are not canonical")
    generation_id = str(runner_report.get("run_id") or "")
    if re.fullmatch(r"[0-9a-f]{8}(?:-[0-9a-f]{4}){3}-[0-9a-f]{12}", generation_id) is None:
        raise DeploymentError("automatic canary generation identity is invalid")
    marker = "FOTMOB_AUTOMATIC_SCOPE_SNAPSHOT_JSON="
    fields = runtime_binding.AUTOMATIC_DECISION_DIGEST_FIELDS
    code = f"""
import hashlib
import json
from scrapers.fotmob.repository import FotMobRepository

repo = FotMobRepository()
trino = repo.writer._get_trino_manager()
catalog_ids = {catalog_ids!r}
wanted = set(catalog_ids)
generation_id = {generation_id!r}
batch_id = {contract.catalog_batch_id!r}
content_hash = {contract.catalog_content_hash!r}
fields = {fields!r}
catalog_rows = trino.execute_query(
    f"SELECT competition_id FROM {{repo.catalog}}.{{repo.schema}}.fotmob_competitions_current "
    "WHERE discovery_run_id = '" + generation_id.replace("'", "''") + "'"
)
observed_catalog = sorted(int(row[0]) for row in catalog_rows)
decision_rows = trino.execute_query(
    "SELECT " + ",".join(fields) + " FROM "
    f"{{repo.catalog}}.{{repo.schema}}.{runtime_binding.SCOPE_OBSERVATIONS_CURRENT_VIEW}"
)
decisions = []
for row in decision_rows:
    item = dict(zip(fields, row))
    try:
        item['competition_id'] = int(item['competition_id'])
    except (TypeError, ValueError):
        continue
    if item['competition_id'] in wanted:
        decisions.append(item)
decisions.sort(key=lambda item: item['competition_id'])
decision_ids = [item['competition_id'] for item in decisions]
canonical = json.dumps(
    decisions, ensure_ascii=False, sort_keys=True, separators=(',', ':')
).encode('utf-8')
manifest_rows = trino.execute_query(
    "SELECT batch_id,content_hash FROM "
    f"{{repo.catalog}}.{{repo.schema}}.fotmob_ingest_manifest "
    "WHERE target_type='all_leagues' AND batch_id='" + batch_id.replace("'", "''")
    + "' AND content_hash='" + content_hash.replace("'", "''")
    + "' AND run_id='" + generation_id.replace("'", "''")
    + "' AND status IN ('success','not_modified')"
)
def id_digest(values):
    raw = ''.join(str(value) + '\\n' for value in values).encode('ascii')
    return hashlib.sha256(raw).hexdigest()
included = [item['competition_id'] for item in decisions if item['decision'] == 'included']
payload = {{
    'table': {runtime_binding.SCOPE_OBSERVATIONS_TABLE!r},
    'table_exists': True,
    'current_view': {runtime_binding.SCOPE_OBSERVATIONS_CURRENT_VIEW!r},
    'current_view_exists': True,
    'snapshot_run_id': generation_id,
    'catalog_batch_id': batch_id,
    'catalog_content_hash': content_hash,
    'catalog_manifest_match_count': len(manifest_rows),
    'catalog_id_count': len(observed_catalog),
    'catalog_ids_sha256': id_digest(observed_catalog),
    'decision_count': len(decisions),
    'decision_ids_sha256': id_digest(decision_ids),
    'decision_evidence_sha256': hashlib.sha256(canonical).hexdigest(),
    'duplicate_decision_count': len(decisions) - len(set(decision_ids)),
    'classifier_version': (
        next(iter(set(item['classifier_version'] for item in decisions)), None)
        if len(set(item['classifier_version'] for item in decisions)) <= 1 else None
    ),
    'included_id_count': len(included),
    'included_ids_sha256': id_digest(included),
}}
print({marker!r} + json.dumps(payload, sort_keys=True))
"""
    output = run(
        ("docker", "exec", container_id, "python", "-c", code),
        check=True,
        capture_output=True,
        text=True,
    ).stdout
    payload = parse_marker_json(output, marker)
    if not isinstance(payload, Mapping) or payload.get("catalog_manifest_match_count") != 1:
        raise DeploymentError("automatic catalog manifest snapshot is incomplete")
    normalized = dict(payload)
    normalized.pop("catalog_manifest_match_count", None)
    return normalized


def validate_live_automatic_canary(
    container_id: str,
    canary: Mapping[str, Any],
    *,
    run: Callable[..., subprocess.CompletedProcess[str]],
) -> dict[str, Any]:
    """Recheck exact DagRuns, XCom bytes, and released ControlStore state."""

    runner_path = str(canary.get("runner_report_path") or "")
    if re.fullmatch(r"/tmp/fotmob_result_[A-Za-z0-9_.:+-]+\.json", runner_path) is None:
        raise DeploymentError("automatic canary runner path is invalid")
    marker = "FOTMOB_AUTOMATIC_CANARY_LIVE_JSON="
    code = f"""
import hashlib
import json
from pathlib import Path
from airflow.models import DagRun
from airflow.models.xcom import XCom
from airflow.settings import Session
from scrapers.fbref.control import ControlStore

s = Session()
expected = {{
    'dag_ingest_fotmob': {str(canary.get('ingest_run_id'))!r},
    'dag_transform_fotmob_silver': {str(canary.get('silver_run_id'))!r},
}}
rows = s.query(DagRun.dag_id, DagRun.run_id, DagRun.state).filter(
    DagRun.dag_id.in_(tuple(expected)), DagRun.run_id.in_(tuple(expected.values()))
).all()
xcom = XCom.get_one(
    run_id=expected['dag_ingest_fotmob'], dag_id='dag_ingest_fotmob',
    task_id='validate_data', key='return_value', session=s
)
s.close()
raw = Path({runner_path!r}).read_bytes()
runner = json.loads(raw.decode('utf-8'))
control = ControlStore.from_env().get_publication_generation(
    {str(canary.get('generation_id'))!r}, source='fotmob'
)
payload = {{
    'runs': [{{'dag_id': d, 'run_id': str(r), 'state': str(getattr(st, 'value', st)).lower()}}
             for d, r, st in rows],
    'validation': xcom,
    'runner_sha256': hashlib.sha256(raw).hexdigest(),
    'runner_bytes': len(raw),
    'runner_report': runner,
    'publication': control,
}}
print({marker!r} + json.dumps(payload, default=str, sort_keys=True))
"""
    output = run(
        ("docker", "exec", container_id, "python", "-c", code),
        check=True,
        capture_output=True,
        text=True,
    ).stdout
    payload = parse_marker_json(output, marker)
    if not isinstance(payload, Mapping):
        raise DeploymentError("automatic canary live evidence is invalid")
    expected_runs = {
        ("dag_ingest_fotmob", str(canary.get("ingest_run_id")), "success"),
        ("dag_transform_fotmob_silver", str(canary.get("silver_run_id")), "success"),
    }
    observed_runs = {
        (str(item.get("dag_id")), str(item.get("run_id")), str(item.get("state")))
        for item in payload.get("runs") or ()
        if isinstance(item, Mapping)
    }
    validation = payload.get("validation")
    publication = payload.get("publication")
    final_publication = canary.get("final_publication")
    candidate = publication.get("candidate") if isinstance(publication, Mapping) else None
    if (
        observed_runs != expected_runs
        or not isinstance(validation, Mapping)
        or validation.get("runner_report_path") != runner_path
        or validation.get("runner_report_sha256") != canary.get("runner_report_sha256")
        or validation.get("runner_report_bytes") != canary.get("runner_report_bytes")
        or payload.get("runner_sha256") != canary.get("runner_report_sha256")
        or payload.get("runner_bytes") != canary.get("runner_report_bytes")
        or payload.get("runner_report") != canary.get("current_run_reports", [None])[0]
        or not isinstance(publication, Mapping)
        or publication.get("generation_id") != canary.get("generation_id")
        or publication.get("phase") != "abandoned"
        or publication.get("active") is not False
        or not publication.get("released_at")
        or not isinstance(candidate, Mapping)
        or candidate.get("digest") != canary.get("candidate_digest")
        or not isinstance(final_publication, Mapping)
        or candidate != final_publication.get("candidate")
    ):
        raise DeploymentError("automatic canary live provenance differs")
    return dict(payload)


def atomic_automatic_writer_transition(
    container_id: str,
    *,
    phase: str,
    selected_date: str | None = None,
    run: Callable[..., subprocess.CompletedProcess[str]],
) -> dict[str, Any]:
    """Validate and update all six DagModel rows in one metadata transaction."""

    if phase not in {"children", "owner", "pause_all"}:
        raise DeploymentError("unknown automatic writer transition")
    if phase in {"children", "owner"}:
        try:
            date.fromisoformat(str(selected_date))
        except (TypeError, ValueError) as exc:
            raise DeploymentError(
                "automatic writer transition requires selected daily date"
            ) from exc
    marker = "FOTMOB_AUTOMATIC_WRITER_TX_JSON="
    code = f"""
import json
import secrets
from datetime import date, datetime, timezone
from airflow.models import DagModel, DagRun, Variable
from airflow.settings import Session
from sqlalchemy import text

ids = {sorted(EXPECTED_DAGS)!r}
active_ids = {sorted(AUTOMATIC_ACTIVE_DAGS)!r}
legacy_ids = {sorted(LEGACY_OWNER_DAGS)!r}
phase = {phase!r}
selected_date = {selected_date!r}
s = Session()
try:
    s.execute(text('SET TRANSACTION ISOLATION LEVEL SERIALIZABLE'))
    models = s.query(DagModel).filter(DagModel.dag_id.in_(ids)).with_for_update().all()
    by_id = {{model.dag_id: model for model in models}}
    if set(by_id) != set(ids):
        raise RuntimeError('exact six DagModel rows are required')
    run_rows = s.query(DagRun.dag_id, DagRun.run_id, DagRun.state).filter(
        DagRun.dag_id.in_(ids), DagRun.state.in_(('running', 'queued'))
    ).with_for_update().all()
    active = {{}}
    for dag_id, run_id, state in run_rows:
        state = str(getattr(state, 'value', state)).lower()
        active.setdefault(dag_id, {{}}).setdefault(state, []).append(str(run_id))
    before = {{dag_id: bool(by_id[dag_id].is_paused) for dag_id in ids}}
    if active:
        raise RuntimeError('queued/running FotMob writer exists')
    scheduler_state = None
    if phase in ('children', 'owner'):
        state_row = s.query(Variable).filter(
            Variable.key == 'fotmob.scheduler.state.v1'
        ).with_for_update().one_or_none()
        scheduler_state = (
            {{'next_background_lane':'refresh','daily_date':None,'generation':0,
              'updated_at':'1970-01-01T00:00:00+00:00'}}
            if state_row is None else json.loads(state_row.get_val())
        )
        try:
            parsed_daily_date = (
                None if scheduler_state.get('daily_date') is None
                else date.fromisoformat(str(scheduler_state.get('daily_date')))
            )
            parsed_updated_at = datetime.fromisoformat(
                str(scheduler_state.get('updated_at')).replace('Z','+00:00')
            )
            if parsed_updated_at.tzinfo is None or parsed_updated_at.utcoffset() is None:
                raise ValueError('naive updated_at')
        except (AttributeError, TypeError, ValueError):
            raise RuntimeError('FotMob scheduler state is malformed')
        if (
            not isinstance(scheduler_state, dict)
            or set(scheduler_state) != {{'next_background_lane','daily_date','generation','updated_at'}}
            or scheduler_state['next_background_lane'] not in ('refresh','backfill')
            or scheduler_state['daily_date'] == selected_date
            or (parsed_daily_date is not None and parsed_daily_date.isoformat() != scheduler_state['daily_date'])
            or isinstance(scheduler_state['generation'], bool)
            or not isinstance(scheduler_state['generation'], int)
            or scheduler_state['generation'] < 0
            or not isinstance(scheduler_state['updated_at'], str)
        ):
            raise RuntimeError('FotMob scheduler state blocks first automatic daily')
    all_paused = all(before.values())
    children_shape = all(
        before[dag_id] is (dag_id not in {{'dag_ingest_fotmob', 'dag_transform_fotmob_silver'}})
        for dag_id in ids
    )
    if phase == 'children' and not all_paused:
        raise RuntimeError('children transition requires all six paused')
    if phase == 'owner' and not children_shape:
        raise RuntimeError('owner transition has unexpected pause shape')
    for dag_id, model in by_id.items():
        if phase == 'pause_all':
            model.is_paused = True
        elif phase == 'children':
            model.is_paused = dag_id not in {{'dag_ingest_fotmob', 'dag_transform_fotmob_silver'}}
        else:
            model.is_paused = dag_id in set(legacy_ids)
    s.flush()
    after = {{dag_id: bool(by_id[dag_id].is_paused) for dag_id in ids}}
    observed_at = datetime.now(timezone.utc).isoformat()
    transaction_id = secrets.token_hex(16)
    s.commit()
except Exception:
    s.rollback()
    raise
finally:
    s.close()
payload = {{
    'schema_version': 'fotmob-writer-snapshot-v1',
    'transaction_id': transaction_id,
    'observed_at': observed_at,
    'pause_states': before,
    'active_runs': active,
    'pause_states_after': after,
    'phase': phase,
    'scheduler_state': scheduler_state,
}}
print({marker!r} + json.dumps(payload, sort_keys=True))
"""
    output = run(
        ("docker", "exec", container_id, "python", "-c", code),
        check=True,
        capture_output=True,
        text=True,
    ).stdout
    payload = parse_marker_json(output, marker)
    scheduler_state = payload.get("scheduler_state") if isinstance(payload, Mapping) else None
    if (
        not isinstance(payload, Mapping)
        or set(payload.get("pause_states") or {}) != EXPECTED_DAGS
        or (
            phase in {"children", "owner"}
            and (
                not isinstance(scheduler_state, Mapping)
                or scheduler_state.get("daily_date") == selected_date
            )
        )
    ):
        raise DeploymentError("automatic writer transaction returned incomplete evidence")
    return dict(payload)


def inspect_automatic_writer_pause_shape(
    container_id: str,
    *,
    expected_paused: set[str] | None,
    run: Callable[..., subprocess.CompletedProcess[str]],
) -> dict[str, Any]:
    """Read all six live DagModel rows without trusting the report snapshot."""

    if expected_paused is not None and not expected_paused.issubset(EXPECTED_DAGS):
        raise DeploymentError("automatic writer pause expectation is invalid")
    marker = "FOTMOB_AUTOMATIC_WRITER_LIVE_JSON="
    code = f"""
import json
from airflow.models import DagModel, DagRun
from airflow.settings import Session
from sqlalchemy import text

ids = {sorted(EXPECTED_DAGS)!r}
s = Session()
try:
    s.execute(text('SET TRANSACTION ISOLATION LEVEL REPEATABLE READ, READ ONLY'))
    models = s.query(DagModel.dag_id, DagModel.is_paused).filter(
        DagModel.dag_id.in_(ids)
    ).all()
    runs = s.query(DagRun.dag_id, DagRun.run_id, DagRun.state).filter(
        DagRun.dag_id.in_(ids), DagRun.state.in_(('running', 'queued'))
    ).all()
    payload = {{
        'pause_states': {{dag_id: bool(paused) for dag_id, paused in models}},
        'active_runs': [
            {{'dag_id': dag_id, 'run_id': str(run_id),
              'state': str(getattr(state, 'value', state)).lower()}}
            for dag_id, run_id, state in runs
        ],
        'atomic_metadata_snapshot': True,
    }}
finally:
    s.close()
print({marker!r} + json.dumps(payload, sort_keys=True))
"""
    output = run(
        ("docker", "exec", container_id, "python", "-c", code),
        check=True,
        capture_output=True,
        text=True,
    ).stdout
    payload = parse_marker_json(output, marker)
    pause_states = payload.get("pause_states") if isinstance(payload, Mapping) else None
    expected = (
        None
        if expected_paused is None
        else {
            dag_id: dag_id in expected_paused for dag_id in sorted(EXPECTED_DAGS)
        }
    )
    if (
        not isinstance(payload, Mapping)
        or (expected is not None and pause_states != expected)
        or payload.get("atomic_metadata_snapshot") is not True
        or not isinstance(payload.get("active_runs"), list)
    ):
        raise DeploymentError("live automatic writer pause shape differs")
    return dict(payload)


def atomic_shared_consumer_transition(
    container_id: str,
    *,
    phase: str,
    recovery_boundary: Mapping[str, Any] | None = None,
    run: Callable[..., subprocess.CompletedProcess[str]],
) -> dict[str, Any]:
    """Lock, verify and update the shared daily consumer in one transaction."""

    if phase not in {"unpause", "pause", "inspect_unpaused"}:
        raise DeploymentError("unknown shared consumer transition")
    normalized_recovery_boundary = (
        validate_schedule_boundary(
            recovery_boundary, label="shared recovery interval"
        )
        if recovery_boundary is not None
        else None
    )
    recovery_run_id = (
        _scheduled_run_id(normalized_recovery_boundary["logical_date"])
        if normalized_recovery_boundary is not None
        else None
    )
    marker = "FOTMOB_SHARED_CONSUMER_TX_JSON="
    code = f"""
import json
import secrets
from datetime import datetime, timezone
from airflow.models import DagModel, DagRun, TaskInstance, Variable
from airflow.settings import Session
from sqlalchemy import text

dag_id = {SHARED_CONSUMER_DAG_ID!r}
pause_ids = {sorted(runtime_binding.EXPECTED_SHARED_PAUSE_STATES)!r}
active_ids = {sorted(runtime_binding.SHARED_STATE_DAGS)!r}
consumer_task_ids = {[
    "wait_for_fotmob_publication",
    "trigger_xref_transforms",
    "trigger_e3_transforms",
    "trigger_e4_transforms",
    "finalize_fotmob_publication",
]!r}
phase = {phase!r}
recovery_run_id = {recovery_run_id!r}
s = Session()
try:
    s.execute(text('SET TRANSACTION ISOLATION LEVEL SERIALIZABLE'))
    models = s.query(DagModel).filter(
        DagModel.dag_id.in_(pause_ids)
    ).with_for_update().all()
    by_id = {{model.dag_id: model for model in models}}
    if set(by_id) != set(pause_ids):
        raise RuntimeError('exact shared DagModel rows are required')
    owner = s.query(Variable).filter(
        Variable.key == 'fotmob_schedule_owner'
    ).with_for_update().one_or_none()
    if owner is None or str(owner.val).strip().lower() != 'isolated':
        raise RuntimeError('shared schedule owner is not isolated')
    runs = s.query(DagRun).filter(
        DagRun.dag_id.in_(active_ids), DagRun.state.in_(('running', 'queued'))
    ).with_for_update().all()
    active = [
        {{'dag_id': item.dag_id, 'run_id': str(item.run_id),
          'state': str(getattr(item.state, 'value', item.state)).lower()}}
        for item in runs
    ]
    observed_runs = list(runs)
    if recovery_run_id is not None and not any(
        item.dag_id == dag_id and str(item.run_id) == recovery_run_id
        for item in observed_runs
    ):
        recovery_run = s.query(DagRun).filter(
            DagRun.dag_id == dag_id, DagRun.run_id == recovery_run_id
        ).with_for_update().one_or_none()
        if recovery_run is not None:
            observed_runs.append(recovery_run)
    consumer_runs = []
    for item in observed_runs:
        if item.dag_id != dag_id:
            continue
        task_rows = s.query(TaskInstance.task_id, TaskInstance.state).filter(
            TaskInstance.dag_id == dag_id,
            TaskInstance.run_id == item.run_id,
            TaskInstance.task_id.in_(consumer_task_ids),
        ).with_for_update().all()
        task_states = {{task_id: None for task_id in consumer_task_ids}}
        for task_id, state in task_rows:
            task_states[task_id] = (
                None if state is None
                else str(getattr(state, 'value', state)).lower()
            )
        iso = lambda value: value.isoformat() if value is not None else None
        consumer_runs.append({{
            'dag_id': item.dag_id,
            'run_id': str(item.run_id),
            'run_type': str(getattr(item.run_type, 'value', item.run_type)).lower(),
            'state': str(getattr(item.state, 'value', item.state)).lower(),
            'logical_date': iso(item.logical_date),
            'data_interval_start': iso(item.data_interval_start),
            'data_interval_end': iso(item.data_interval_end),
            'task_states': task_states,
        }})
    before = {{item_id: bool(by_id[item_id].is_paused) for item_id in pause_ids}}
    expected_paused = {{item_id: True for item_id in pause_ids}}
    expected_active = dict(expected_paused)
    expected_active[dag_id] = False
    if phase == 'unpause':
        if before != expected_paused or active:
            raise RuntimeError('shared consumer cutover requires paused and idle state')
        by_id[dag_id].is_paused = False
    elif phase == 'pause':
        by_id[dag_id].is_paused = True
    elif before != expected_active:
        raise RuntimeError('shared consumer pause shape differs')
    s.flush()
    after = {{item_id: bool(by_id[item_id].is_paused) for item_id in pause_ids}}
    observed_at = datetime.now(timezone.utc).isoformat()
    transaction_id = secrets.token_hex(16)
    s.commit()
except Exception:
    s.rollback()
    raise
finally:
    s.close()
payload = {{
    'schema_version': 'fotmob-shared-consumer-snapshot-v1',
    'transaction_id': transaction_id,
    'observed_at': observed_at,
    'dag_id': dag_id,
    'phase': phase,
    'pause_states_before': before,
    'pause_states_after': after,
    'schedule_owner': str(owner.val).strip().lower(),
    'active_runs': active,
    'consumer_runs': consumer_runs,
}}
print({marker!r} + json.dumps(payload, sort_keys=True))
"""
    output = run(
        ("docker", "exec", container_id, "python", "-c", code),
        check=True,
        capture_output=True,
        text=True,
    ).stdout
    payload = parse_marker_json(output, marker)
    expected_after = dict(runtime_binding.EXPECTED_SHARED_PAUSE_STATES)
    if phase != "pause":
        expected_after[SHARED_CONSUMER_DAG_ID] = False
    if (
        not isinstance(payload, Mapping)
        or payload.get("dag_id") != SHARED_CONSUMER_DAG_ID
        or payload.get("phase") != phase
        or payload.get("pause_states_after") != expected_after
        or payload.get("schedule_owner") != "isolated"
        or not isinstance(payload.get("active_runs"), list)
    ):
        raise DeploymentError("shared consumer transition returned invalid evidence")
    return dict(payload)


def assert_no_active_control_publication(
    shared_container: str,
    *,
    run: Callable[..., subprocess.CompletedProcess[str]],
) -> dict[str, Any]:
    """Check only the shared ControlStore after the consumer is enabled."""

    marker = "FOTMOB_CONTROL_ONLY_QUIESCENCE_JSON="
    code = (
        "import json; from scrapers.fbref.control import ControlStore; "
        "p=dict(ControlStore.from_env().assert_no_active_publication_generation("
        "source='fotmob')); "
        f"print({marker!r}+json.dumps(p,default=str,sort_keys=True))"
    )
    output = run(
        ("docker", "exec", shared_container, "python", "-c", code),
        check=True,
        capture_output=True,
        text=True,
    ).stdout
    payload = parse_marker_json(output, marker)
    if (
        not isinstance(payload, Mapping)
        or payload.get("source") != "fotmob"
        or payload.get("safe") is not True
        or payload.get("active") is not False
    ):
        raise DeploymentError("FotMob ControlStore is not quiescent")
    return dict(payload)


def validate_automatic_activation_boundary(
    raw: Any,
    *,
    now: datetime | None = None,
) -> dict[str, Any]:
    """Bind cutover to the daily interval the five-minute owner will mint."""

    boundary = validate_schedule_boundary(
        raw,
        label=f"automatic {SHARED_CONSUMER_DAG_ID}",
    )
    raw_now = now or datetime.now(timezone.utc)
    if raw_now.tzinfo is None or raw_now.utcoffset() is None:
        raise DeploymentError("automatic cutover clock must include a timezone")
    checked_at = raw_now.astimezone(timezone.utc)
    start = datetime.fromisoformat(boundary["data_interval_start"])
    end = datetime.fromisoformat(boundary["data_interval_end"])
    logical_date = datetime.fromisoformat(boundary["logical_date"])
    run_after = datetime.fromisoformat(boundary["run_after"])
    daily_window_end = end + timedelta(hours=1)
    safe_cutoff = daily_window_end - timedelta(
        seconds=MIN_ACTIVATION_SAFETY_SECONDS
    )
    safe_start = end - timedelta(minutes=30)
    if (
        end - start != timedelta(days=1)
        or logical_date != start
        or run_after != end
        or (end.hour, end.minute, end.second, end.microsecond) != (14, 0, 0, 0)
        or end.date() != checked_at.date()
        or checked_at < safe_start
        or checked_at >= safe_cutoff
    ):
        raise DeploymentError(
            "automatic cutover does not match today's safe 14:00 UTC daily interval"
        )
    return {
        "schema_version": "fotmob-automatic-boundary-v1",
        "checked_at": checked_at.isoformat(),
        "selected_date": end.date().isoformat(),
        "state": "future" if checked_at < end else "daily_window_open",
        "data_interval_start": boundary["data_interval_start"],
        "data_interval_end": boundary["data_interval_end"],
        "safe_cutoff": safe_cutoff.isoformat(),
        "safe_start": safe_start.isoformat(),
        "passed": True,
    }


def validate_owner_committed_shared_recovery(
    shared_snapshot: Mapping[str, Any],
    *,
    stored_boundary: Mapping[str, Any],
    live_boundary: Mapping[str, Any],
) -> dict[str, Any]:
    """Accept only an idle edge or the exact pending-safe Sofa sensor run."""

    stored = validate_schedule_boundary(
        stored_boundary, label="stored owner-committed shared interval"
    )
    live = validate_schedule_boundary(
        live_boundary, label="live owner-committed shared interval"
    )
    active_runs = shared_snapshot.get("active_runs")
    consumer_runs = shared_snapshot.get("consumer_runs")
    if active_runs == [] and consumer_runs in (None, []):
        if live != stored:
            raise DeploymentError(
                "idle owner-committed shared daily boundary changed"
            )
        return {
            "schema_version": "fotmob-shared-recovery-v1",
            "mode": "idle_before_scheduled_run",
            "stored_boundary": stored,
            "live_boundary": live,
            "passed": True,
        }

    expected_run_id = _scheduled_run_id(stored["logical_date"])
    expected_active = {
        "dag_id": SHARED_CONSUMER_DAG_ID,
        "run_id": expected_run_id,
    }
    shifted = {
        key: (
            datetime.fromisoformat(value) + timedelta(days=1)
        ).isoformat(timespec="microseconds")
        for key, value in stored.items()
    }
    if active_runs == []:
        if not isinstance(consumer_runs, list) or len(consumer_runs) != 1:
            raise DeploymentError(
                "terminal shared recovery requires one exact consumer run"
            )
        consumer = consumer_runs[0]
        task_states = consumer.get("task_states")
        downstream = {
            "trigger_xref_transforms",
            "trigger_e3_transforms",
            "trigger_e4_transforms",
            "finalize_fotmob_publication",
        }
        expected_task_ids = downstream | {"wait_for_fotmob_publication"}
        terminal_downstream = {
            "trigger_xref_transforms": "upstream_failed",
            "trigger_e3_transforms": "upstream_failed",
            "trigger_e4_transforms": "upstream_failed",
            "finalize_fotmob_publication": "failed",
        }
        if (
            any(consumer.get(key) != value for key, value in expected_active.items())
            or consumer.get("run_type") != "scheduled"
            or consumer.get("state") != "failed"
            or validate_schedule_boundary(
                {
                    "logical_date": consumer.get("logical_date"),
                    "data_interval_start": consumer.get("data_interval_start"),
                    "data_interval_end": consumer.get("data_interval_end"),
                    "run_after": consumer.get("data_interval_end"),
                },
                label="terminal owner-committed Sofa run",
            )
            != stored
            or not isinstance(task_states, Mapping)
            or set(task_states) != expected_task_ids
            or task_states.get("wait_for_fotmob_publication") != "failed"
            or any(
                task_states.get(task_id) != state
                for task_id, state in terminal_downstream.items()
            )
            or live != shifted
        ):
            raise DeploymentError(
                "terminal owner-committed Sofa run is not wait-only failed"
            )
        return {
            "schema_version": "fotmob-shared-recovery-v1",
            "mode": "terminal_wait_sensor_failed",
            "stored_boundary": stored,
            "live_boundary": live,
            "consumer_run": dict(consumer),
            "roll_forward": True,
            "next_scheduled_boundary": live,
            "passed": True,
        }
    if (
        not isinstance(active_runs, list)
        or len(active_runs) != 1
        or any(
            active_runs[0].get(key) != value
            for key, value in expected_active.items()
        )
        or active_runs[0].get("state") not in {"queued", "running"}
        or not isinstance(consumer_runs, list)
        or len(consumer_runs) != 1
    ):
        raise DeploymentError(
            "owner-committed shared recovery has an unexpected active run"
        )
    consumer = consumer_runs[0]
    task_states = consumer.get("task_states")
    downstream = {
        "trigger_xref_transforms",
        "trigger_e3_transforms",
        "trigger_e4_transforms",
        "finalize_fotmob_publication",
    }
    expected_task_ids = downstream | {"wait_for_fotmob_publication"}
    if (
        any(consumer.get(key) != value for key, value in expected_active.items())
        or consumer.get("run_type") != "scheduled"
        or consumer.get("state") not in {"queued", "running"}
        or validate_schedule_boundary(
            {
                "logical_date": consumer.get("logical_date"),
                "data_interval_start": consumer.get("data_interval_start"),
                "data_interval_end": consumer.get("data_interval_end"),
                "run_after": consumer.get("data_interval_end"),
            },
            label="active owner-committed Sofa run",
        )
        != stored
        or not isinstance(task_states, Mapping)
        or set(task_states) != expected_task_ids
        or task_states.get("wait_for_fotmob_publication")
        not in {"queued", "running", "scheduled", "up_for_reschedule", "deferred"}
        or any(task_states.get(task_id) is not None for task_id in downstream)
    ):
        raise DeploymentError(
            "owner-committed Sofa run is not wait-sensor-only"
        )
    if live != shifted:
        raise DeploymentError(
            "owner-committed Sofa run did not advance exactly one daily interval"
        )
    return {
        "schema_version": "fotmob-shared-recovery-v1",
        "mode": "scheduled_wait_sensor",
        "stored_boundary": stored,
        "live_boundary": live,
        "consumer_run": dict(consumer),
        "passed": True,
    }


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
        ".order_by(DagRun.execution_date.desc()).limit(20).all(); "
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
    'dag_refresh_fotmob',
    'dag_backfill_fotmob',
    'dag_orchestrate_fotmob',
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
            "dag_refresh_fotmob",
            "dag_backfill_fotmob",
            "dag_orchestrate_fotmob",
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
    parser.add_argument(
        "--automatic-catalog",
        action="store_true",
        help="Prepare the six-DAG automatic catalog rollout (must stay paused)",
    )
    parser.add_argument(
        "--activate-automatic",
        action="store_true",
        help="Activate a prepared automatic rollout after one exact canary",
    )
    parser.add_argument(
        "--automatic-canary-report",
        type=Path,
        help="Durable automatic-canary report used only with --activate-automatic",
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
    if not isinstance(payload, Mapping) or payload.get("schema_version") != "fotmob-deploy-v2":
        return
    automatic_rollout = payload.get("automatic_rollout")
    if (
        payload.get("activation_state")
        in {"kept_paused", "pending_automatic", "automatic_activation_failed"}
        and isinstance(automatic_rollout, Mapping)
        and automatic_rollout.get("schema_version") == AUTOMATIC_ROLLOUT_SCHEMA
    ):
        raise DeploymentError(
            "automatic rollout evidence is resumable; only the exact "
            "--activate-automatic command may continue it"
        )
    if (
        payload.get("passed") is not True
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
    automatic_catalog = bool(getattr(args, "automatic_catalog", False))
    if automatic_catalog and not args.keep_paused:
        raise DeploymentError("--automatic-catalog requires --keep-paused")
    if not automatic_catalog and not args.keep_paused:
        raise DeploymentError(
            "legacy scheduled activation is retired; deploy coordinator-only "
            "with --keep-paused"
        )
    if automatic_catalog and args.release_root.resolve() != REPOSITORY_ROOT.resolve():
        raise DeploymentError(
            "automatic deploy must run from the same pristine --release-root checkout"
        )
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
        isolated_schedule_initial = None
        if not automatic_catalog and not args.keep_paused:
            isolated_schedule_initial = read_schedule_boundary(
                container_id,
                ISOLATED_DAILY_DAG_ID,
                run=run,
            )
            # Legacy activation binds the old daily producer to the shared
            # consumer.  Automatic rollout binds its daily generation inside
            # the orchestrator and deliberately has no legacy boundary.
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
        automatic_scope_bootstrap = (
            bootstrap_automatic_scope_observations(container_id, run=run)
            if automatic_catalog
            else None
        )
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
        isolated_schedule_final = None
        if not automatic_catalog and not args.keep_paused:
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
        schedule_boundary = None
        if not automatic_catalog and not args.keep_paused:
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
        }
        if automatic_catalog:
            report["automatic_rollout"] = {
                "schema_version": AUTOMATIC_ROLLOUT_SCHEMA,
                "phase": "awaiting_canary",
                "scope_observation_bootstrap": automatic_scope_bootstrap,
            }
        elif not args.keep_paused:
            report["schedule_boundary"] = schedule_boundary
        else:
            report["coordinator_rollout"] = {
                "schema_version": COORDINATOR_ROLLOUT_SCHEMA,
                "phase": "kept_paused",
                "legacy_activation_retired": True,
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
                assert_paused(set(EXPECTED_DAGS))
            except Exception:
                pass
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
            pass
        raise


def activate_automatic_catalog(
    args: argparse.Namespace,
    *,
    run: Callable[..., subprocess.CompletedProcess[str]] = subprocess.run,
) -> dict[str, Any]:
    """Resume-only automatic cutover for the exact prepared deployment."""

    if args.keep_paused or args.resume_pending or not args.automatic_catalog:
        raise DeploymentError(
            "--activate-automatic requires --automatic-catalog and no legacy flags"
        )
    if args.automatic_canary_report is None:
        raise DeploymentError("--activate-automatic requires --automatic-canary-report")
    report_path = (args.report or args.evidence_dir / "deployment.json").resolve()
    try:
        deployment = json.loads(
            report_path.read_text(encoding="utf-8"),
            object_pairs_hook=_unique_json_object,
        )
    except (OSError, json.JSONDecodeError) as exc:
        raise DeploymentError(f"cannot read automatic deployment report: {exc}") from exc
    if not isinstance(deployment, dict) or deployment.get("schema_version") != "fotmob-deploy-v2":
        raise DeploymentError("automatic activation requires a fotmob-deploy-v2 report")
    if (
        deployment.get("activation_state") == "active"
        and deployment.get("automatic_catalog_admission") is not None
    ):
        _report, isolated_container, shared_container = _validate_resume_identity(
            args, deployment, run=run
        )
        active_rollout = deployment.get("automatic_rollout")
        if (
            not isinstance(active_rollout, Mapping)
            or active_rollout.get("phase") != "active"
            or active_rollout.get("canary_report")
            != str(args.automatic_canary_report.resolve())
        ):
            raise DeploymentError("active automatic canary identity differs")
        load_automatic_canary_report(
            args.automatic_canary_report,
            evidence_dir=args.evidence_dir,
            deployment=deployment,
        )
        try:
            context = runtime_binding.load_deployment_context(
                report_path,
                project=args.project,
                compose_file=args.compose_file,
            )
            runtime_binding.validate_live_deployment(
                context,
                project=args.project,
                compose_file=args.compose_file,
                env_file=args.env_file,
                require_running=True,
                run=run,
            )
            runtime_binding.validate_live_shared_runtime(context, run=run)
        except runtime_binding.RuntimeBindingError as exc:
            raise DeploymentError(str(exc)) from exc
        inspect_automatic_writer_pause_shape(
            isolated_container,
            expected_paused=set(LEGACY_OWNER_DAGS),
            run=run,
        )
        atomic_shared_consumer_transition(
            shared_container,
            phase="inspect_unpaused",
            run=run,
        )
        return dict(deployment)
    allowed_states = {
        "kept_paused",
        "pending_automatic",
        "automatic_activation_failed",
    }
    if deployment.get("activation_state") not in allowed_states:
        raise DeploymentError("deployment is not awaiting automatic activation")
    rollout = deployment.get("automatic_rollout")
    if (
        not isinstance(rollout, Mapping)
        or rollout.get("schema_version") != AUTOMATIC_ROLLOUT_SCHEMA
        or rollout.get("scope_observation_bootstrap")
        != {
            "table": runtime_binding.SCOPE_OBSERVATIONS_TABLE,
            "table_exists": True,
            "current_view": runtime_binding.SCOPE_OBSERVATIONS_CURRENT_VIEW,
            "current_view_exists": True,
        }
    ):
        raise DeploymentError("deployment has no exact automatic bootstrap evidence")
    _report, isolated_container, shared_container = _validate_resume_identity(
        args, deployment, run=run
    )
    canary = load_automatic_canary_report(
        args.automatic_canary_report,
        evidence_dir=args.evidence_dir,
        deployment=deployment,
    )
    owner_committed = False
    owner_transition_attempted = False
    shared_committed = False
    shared_transition_attempted = False
    pending_report: dict[str, Any] | None = None
    owner_commit_evidence: dict[str, Any] = {}

    try:
        # A failed prior attempt stopped this exact container after pausing all
        # writers. Restarting it is resume, not a redeploy, and preserves identity.
        running = run(
            (
                "docker",
                "inspect",
                "--format",
                "{{.State.Running}}",
                isolated_container,
            ),
            check=True,
            capture_output=True,
            text=True,
        ).stdout.strip().casefold()
        if running == "false":
            _mark_runtime_mutation_started(args)
            run(
                ("docker", "start", isolated_container),
                check=True,
                capture_output=True,
                text=True,
            )
        elif running != "true":
            raise DeploymentError("cannot determine automatic scheduler state")

        try:
            context = runtime_binding.load_deployment_context(
                report_path,
                project=args.project,
                compose_file=args.compose_file,
            )
        except runtime_binding.RuntimeBindingError:
            # Pending/failure reports are not runtime authority, but their
            # immutable identities can still be re-attested before recovery.
            context = dict(deployment)
        runtime_binding.validate_live_deployment(
            context,
            project=args.project,
            compose_file=args.compose_file,
            env_file=args.env_file,
            require_running=True,
            run=run,
        )
        runtime_binding.validate_live_shared_runtime(context, run=run)

        # If the owner transaction committed but the final file write failed,
        # recover by observation. Never pause/stop a possibly scheduled owner.
        if deployment.get("activation_state") != "kept_paused":
            resume_snapshot = inspect_automatic_writer_pause_shape(
                isolated_container,
                expected_paused=None,
                run=run,
            )
            active_pause_shape = {
                dag_id: dag_id in LEGACY_OWNER_DAGS
                for dag_id in sorted(EXPECTED_DAGS)
            }
            if resume_snapshot["pause_states"] == active_pause_shape:
                owner_committed = True
                if resume_snapshot.get("active_runs") != []:
                    raise DeploymentError(
                        "owner-committed recovery has active isolated runs"
                    )
                stored_activation = deployment.get("automatic_activation")
                stored_handoff = (
                    stored_activation.get("fresh_shared_handoff")
                    if isinstance(stored_activation, Mapping)
                    else None
                )
                if not isinstance(stored_handoff, Mapping):
                    raise DeploymentError(
                        "owner-committed daily boundary evidence is missing"
                    )
                shared_readback = atomic_shared_consumer_transition(
                    shared_container,
                    phase="inspect_unpaused",
                    recovery_boundary=stored_handoff.get(
                        "next_scheduled_interval"
                    ),
                    run=run,
                )
                live_shared_boundary = read_schedule_boundary(
                    shared_container,
                    SHARED_CONSUMER_DAG_ID,
                    require_paused=False,
                    run=run,
                )
                shared_recovery = validate_owner_committed_shared_recovery(
                    shared_readback,
                    stored_boundary=stored_handoff.get("next_scheduled_interval"),
                    live_boundary=live_shared_boundary,
                )
                stored_commit_boundary = (
                    stored_activation.get("daily_boundary_commit")
                    if isinstance(stored_activation, Mapping)
                    else None
                )
                if not isinstance(stored_commit_boundary, Mapping):
                    raise DeploymentError(
                        "owner-committed boundary certificate is missing"
                    )
                recovery_boundary = dict(stored_commit_boundary)
                recovered_at = _now()
                admission = deployment.get("automatic_catalog_admission")
                try:
                    admission_time = datetime.fromisoformat(
                        str(
                            (admission or {}).get("validated_at", "")
                            if isinstance(admission, Mapping)
                            else ""
                        ).replace("Z", "+00:00")
                    )
                    runtime_binding.validate_automatic_catalog_admission(
                        admission,
                        now=admission_time,
                    )
                except Exception as exc:
                    raise DeploymentError(
                        "owner-committed admission is missing or invalid"
                    ) from exc
                control_quiescence = assert_no_active_control_publication(
                    shared_container,
                    run=run,
                )
                recovered = {
                    **deployment,
                    # Keep the admitted cutover timestamp stable.  Admission
                    # freshness is evaluated at that immutable edge; the
                    # later recovery time is recorded separately below.
                    "generated_at": deployment.get("generated_at"),
                    "passed": True,
                    "activation_state": "active",
                    "kept_paused": False,
                    "paused": sorted(LEGACY_OWNER_DAGS),
                    "unpaused": sorted(AUTOMATIC_ACTIVE_DAGS),
                    "automatic_rollout": {
                        **dict(rollout),
                        "phase": "active",
                        "canary_report": str(
                            args.automatic_canary_report.resolve()
                        ),
                    },
                    "automatic_activation": {
                        **dict(deployment.get("automatic_activation") or {}),
                        "shared_consumer_unpaused": True,
                        "shared_consumer_readback": shared_readback,
                        "owner_unpaused_last": True,
                        "owner_recovery_snapshot": resume_snapshot,
                        "shared_recovery": shared_recovery,
                        "control_quiescence_at_recovery": control_quiescence,
                        "daily_boundary_recovery": recovery_boundary,
                        "recovered_at": recovered_at,
                    },
                }
                runtime_binding.validate_automatic_rollout_activation(
                    recovered,
                    runtime_binding.validate_automatic_catalog_admission(
                        admission,
                        now=admission_time,
                    ),
                )
                _atomic_json(report_path, recovered)
                return recovered

            children_pause_shape = {
                dag_id: dag_id
                not in {"dag_ingest_fotmob", "dag_transform_fotmob_silver"}
                for dag_id in sorted(EXPECTED_DAGS)
            }
            if resume_snapshot["pause_states"] == children_pause_shape:
                shared_committed = True
                if resume_snapshot.get("active_runs") != []:
                    raise DeploymentError(
                        "shared-committed recovery has active isolated runs"
                    )
                stored_activation = deployment.get("automatic_activation")
                stored_handoff = (
                    stored_activation.get("fresh_shared_handoff")
                    if isinstance(stored_activation, Mapping)
                    else None
                )
                if not isinstance(stored_handoff, Mapping):
                    raise DeploymentError(
                        "shared-committed daily boundary evidence is missing"
                    )
                # The durable pending certificate is intentionally ambiguous
                # about whether the shared transaction response was lost.
                # Until an exact readback or a successful atomic pause proves
                # otherwise, preserve its wait-only authority.
                try:
                    shared_readback = atomic_shared_consumer_transition(
                        shared_container,
                        phase="inspect_unpaused",
                        recovery_boundary=stored_handoff.get(
                            "next_scheduled_interval"
                        ),
                        run=run,
                    )
                except Exception:
                    atomic_shared_consumer_transition(
                        shared_container,
                        phase="pause",
                        run=run,
                    )
                    shared_committed = False
                    shared_readback = None
                if isinstance(shared_readback, Mapping):
                    live_shared_boundary = read_schedule_boundary(
                        shared_container,
                        SHARED_CONSUMER_DAG_ID,
                        require_paused=False,
                        run=run,
                    )
                    shared_recovery = validate_owner_committed_shared_recovery(
                        shared_readback,
                        stored_boundary=stored_handoff.get(
                            "next_scheduled_interval"
                        ),
                        live_boundary=live_shared_boundary,
                    )
                    shared_committed = True
                    admission = deployment.get("automatic_catalog_admission")
                    try:
                        admission_time = datetime.fromisoformat(
                            str(
                                (admission or {}).get("validated_at", "")
                                if isinstance(admission, Mapping)
                                else ""
                            ).replace("Z", "+00:00")
                        )
                        normalized_admission = (
                            runtime_binding.validate_automatic_catalog_admission(
                                admission,
                                now=admission_time,
                            )
                        )
                    except Exception as exc:
                        raise DeploymentError(
                            "shared-committed admission is missing or invalid"
                        ) from exc
                    control_quiescence = assert_no_active_control_publication(
                        shared_container,
                        run=run,
                    )
                    owner_transition_attempted = True
                    owner_transition = atomic_automatic_writer_transition(
                        isolated_container,
                        phase="owner",
                        selected_date=str(
                            stored_activation.get("daily_boundary_commit", {}).get(
                                "selected_date"
                            )
                        ),
                        run=run,
                    )
                    owner_committed = True
                    owner_commit_evidence = {
                        "shared_consumer_unpaused": True,
                        "shared_consumer_readback": shared_readback,
                        "shared_recovery": shared_recovery,
                        "control_quiescence_at_recovery": control_quiescence,
                        "daily_boundary_recovery": dict(
                            stored_activation.get("daily_boundary_commit") or {}
                        ),
                        "recovered_at": _now(),
                        "owner_unpaused_last": True,
                        "owner_transaction": owner_transition,
                    }
                    recovered = {
                        **deployment,
                        "passed": True,
                        "activation_state": "active",
                        "kept_paused": False,
                        "paused": sorted(LEGACY_OWNER_DAGS),
                        "unpaused": sorted(AUTOMATIC_ACTIVE_DAGS),
                        "automatic_rollout": {
                            **dict(rollout),
                            "phase": "active",
                            "canary_report": str(
                                args.automatic_canary_report.resolve()
                            ),
                        },
                        "automatic_activation": {
                            **dict(stored_activation),
                            **owner_commit_evidence,
                        },
                    }
                    runtime_binding.validate_automatic_rollout_activation(
                        recovered,
                        normalized_admission,
                    )
                    _atomic_json(report_path, recovered)
                    return recovered

            # A pre-owner retry can be normalized back to the safe all-paused
            # edge. The transaction rejects any queued/running writer.
            _mark_runtime_mutation_started(args)
            atomic_automatic_writer_transition(
                isolated_container, phase="pause_all", run=run
            )
            atomic_shared_consumer_transition(
                shared_container, phase="pause", run=run
            )
        control_db_uri = _configured_env_value(
            args.env_file, os.environ, "FBREF_CONTROL_DB_URI"
        )
        report_relative_path = report_path.relative_to(args.evidence_dir.resolve())
        fresh_shared_handoff = validate_shared_handoff(
            args.release_root.resolve(),
            shared_container,
            control_db_uri,
            evidence_dir=args.evidence_dir.resolve(),
            report_relative_path=report_relative_path,
            run=run,
        )
        previous_shared_handoff = deployment.get("shared_handoff_final")
        immutable_shared_fields = (
            "shared_scheduler_container",
            "shared_admission_mount",
            "runtime_code_sha256",
            "runtime_git_sha",
            "control_database",
            "schedule_owner",
        )
        if (
            not isinstance(previous_shared_handoff, Mapping)
            or any(
                fresh_shared_handoff.get(field)
                != previous_shared_handoff.get(field)
                for field in immutable_shared_fields
            )
        ):
            raise DeploymentError("shared runtime identity changed before cutover")
        automatic_boundary = validate_automatic_activation_boundary(
            fresh_shared_handoff.get("next_scheduled_interval")
        )
        quiescence_before = runtime_binding.assert_no_active_fotmob_publication(
            context, run=run
        )
        live_canary = validate_live_automatic_canary(
            isolated_container, canary, run=run
        )
        scope_observations = collect_automatic_scope_observations(
            isolated_container,
            canary["current_run_reports"][0],
            run=run,
        )
        _mark_runtime_mutation_started(args)
        transition = atomic_automatic_writer_transition(
            isolated_container,
            phase="children",
            selected_date=automatic_boundary["selected_date"],
            run=run,
        )
        writer_snapshot = {
            key: transition[key]
            for key in (
                "schema_version",
                "transaction_id",
                "observed_at",
                "pause_states",
                "active_runs",
            )
        }
        admission = build_automatic_catalog_admission(
            deployment,
            canary,
            writer_snapshot=writer_snapshot,
            scope_observations=scope_observations,
        )
        commit_boundary = validate_automatic_activation_boundary(
            fresh_shared_handoff.get("next_scheduled_interval")
        )
        pending = {
            **deployment,
            "generated_at": _now(),
            "passed": True,
            "activation_state": "pending_automatic",
            "kept_paused": False,
            "paused": sorted(LEGACY_OWNER_DAGS | {AUTOMATIC_OWNER_DAG_ID}),
            "unpaused": sorted(
                AUTOMATIC_ACTIVE_DAGS - {AUTOMATIC_OWNER_DAG_ID}
            ),
            "automatic_catalog_admission": admission,
            "automatic_rollout": {
                **dict(rollout),
                "phase": "pending_owner",
                "canary_report": str(args.automatic_canary_report.resolve()),
            },
            "automatic_activation": {
                "fresh_shared_handoff": fresh_shared_handoff,
                "daily_boundary_initial": automatic_boundary,
                "daily_boundary_commit": commit_boundary,
                "quiescence_before": quiescence_before,
                "live_canary": {
                    "runner_sha256": live_canary["runner_sha256"],
                    "runner_bytes": live_canary["runner_bytes"],
                },
                "children_transaction": transition,
                "shared_consumer_unpaused": False,
                "owner_unpaused_last": False,
            },
        }
        pending_report = pending
        _atomic_json(report_path, pending)
        shared_transition_attempted = True
        shared_transition = atomic_shared_consumer_transition(
            shared_container, phase="unpause", run=run
        )
        shared_committed = True
        shared_readback = atomic_shared_consumer_transition(
            shared_container, phase="inspect_unpaused", run=run
        )
        control_quiescence_at_commit = assert_no_active_control_publication(
            shared_container, run=run
        )
        owner_transition_attempted = True
        owner_transition = atomic_automatic_writer_transition(
            isolated_container,
            phase="owner",
            selected_date=commit_boundary["selected_date"],
            run=run,
        )
        owner_committed = True
        owner_commit_evidence = {
            "shared_consumer_unpaused": True,
            "shared_consumer_transaction": shared_transition,
            "shared_consumer_readback": shared_readback,
            "control_quiescence_at_commit": control_quiescence_at_commit,
            "owner_unpaused_last": True,
            "owner_transaction": owner_transition,
        }
        active = {
            **pending,
            "generated_at": _now(),
            "activation_state": "active",
            "kept_paused": False,
            "paused": sorted(LEGACY_OWNER_DAGS),
            "unpaused": sorted(AUTOMATIC_ACTIVE_DAGS),
            "automatic_rollout": {
                **dict(pending["automatic_rollout"]),
                "phase": "active",
            },
            "automatic_activation": {
                **dict(pending["automatic_activation"]),
                **owner_commit_evidence,
            },
        }
        try:
            active_generated_at = datetime.fromisoformat(
                str(active["generated_at"]).replace("Z", "+00:00")
            )
            normalized_admission = (
                runtime_binding.validate_automatic_catalog_admission(
                    admission,
                    now=active_generated_at,
                )
            )
            runtime_binding.validate_automatic_rollout_activation(
                active,
                normalized_admission,
            )
        except (TypeError, ValueError, runtime_binding.RuntimeBindingError) as exc:
            raise DeploymentError(
                "automatic activation certificate is invalid"
            ) from exc
        _atomic_json(report_path, active)
        return active
    except Exception as exc:
        if shared_transition_attempted and not shared_committed:
            # A committed shared transaction with a lost docker response is
            # indistinguishable from a failed inspection here. Preserve the
            # durable wait-only certificate and resolve it on exact retry.
            try:
                atomic_shared_consumer_transition(
                    shared_container,
                    phase="inspect_unpaused",
                    run=run,
                )
            except Exception:
                pass
            shared_committed = True
        if owner_transition_attempted and not owner_committed:
            try:
                observed_owner = inspect_automatic_writer_pause_shape(
                    isolated_container,
                    expected_paused=None,
                    run=run,
                )
                active_pause_shape = {
                    dag_id: dag_id in LEGACY_OWNER_DAGS
                    for dag_id in sorted(EXPECTED_DAGS)
                }
                if observed_owner.get("pause_states") == active_pause_shape:
                    owner_committed = True
                    owner_commit_evidence = {
                        **owner_commit_evidence,
                        "owner_recovery_snapshot": observed_owner,
                        "owner_unpaused_last": True,
                    }
            except Exception:
                # The owner commit is ambiguous.  Never kill or overwrite the
                # wait-only pending certificate until a later exact readback.
                owner_committed = True
        if owner_committed:
            # The atomic owner cut is the point of no return.  Scheduled work
            # still fails closed on a pending report; killing the scheduler
            # here could interrupt a writer whose state is ambiguous.
            incident_base = pending_report or deployment
            incident = {
                **incident_base,
                "generated_at": _now(),
                "passed": False,
                "activation_state": "pending_automatic",
                "kept_paused": False,
                "paused": sorted(LEGACY_OWNER_DAGS),
                "unpaused": sorted(AUTOMATIC_ACTIVE_DAGS),
                "error": f"{type(exc).__name__}: {exc}",
                "recovery_required": True,
                "automatic_rollout": {
                    **dict(rollout),
                    "phase": "owner_committed_pending_report",
                    "canary_report": str(
                        args.automatic_canary_report.resolve()
                    ),
                },
                "automatic_activation": {
                    **dict(incident_base.get("automatic_activation") or {}),
                    **owner_commit_evidence,
                    "owner_unpaused_last": True,
                    "resume_required": True,
                },
            }
            # Keep the already-durable, passed=true ``pending_owner`` report
            # byte-for-byte.  It grants the exact Sofa sensor wait-only
            # authority, so replacing it with a red incident would make the
            # one scheduled consumer run fail before recovery.  The returned
            # incident is operator output; the next exact activation command
            # recovers by observing the live atomic pause shape.
            incident["durable_pending_report_preserved"] = True
            return incident

        if shared_committed:
            incident_base = pending_report or deployment
            return {
                **incident_base,
                "generated_at": _now(),
                "passed": False,
                "activation_state": "pending_automatic",
                "kept_paused": False,
                "paused": sorted(LEGACY_OWNER_DAGS | {AUTOMATIC_OWNER_DAG_ID}),
                "unpaused": sorted(
                    AUTOMATIC_ACTIVE_DAGS - {AUTOMATIC_OWNER_DAG_ID}
                ),
                "error": f"{type(exc).__name__}: {exc}",
                "recovery_required": True,
                "durable_pending_report_preserved": True,
                "automatic_rollout": {
                    **dict(rollout),
                    "phase": "shared_committed_pending_owner",
                    "canary_report": str(
                        args.automatic_canary_report.resolve()
                    ),
                },
                "automatic_activation": {
                    **dict(incident_base.get("automatic_activation") or {}),
                    "shared_consumer_unpaused": True,
                    "owner_unpaused_last": False,
                    "resume_required": True,
                },
            }

        cleanup_errors: list[str] = []
        isolated_paused = False
        try:
            atomic_automatic_writer_transition(
                isolated_container, phase="pause_all", run=run
            )
            isolated_paused = True
        except Exception as cleanup_exc:
            cleanup_errors.append(f"isolated pause: {cleanup_exc}")
        try:
            atomic_shared_consumer_transition(
                shared_container, phase="pause", run=run
            )
        except Exception as cleanup_exc:
            cleanup_errors.append(f"shared pause: {cleanup_exc}")
        if isolated_paused:
            try:
                run(
                    ("docker", "stop", isolated_container),
                    check=True,
                    capture_output=True,
                    text=True,
                )
            except Exception as cleanup_exc:
                cleanup_errors.append(f"scheduler stop: {cleanup_exc}")
        else:
            cleanup_errors.append(
                "scheduler stop skipped: writer quiescence was not proved"
            )
        failure_base = pending_report or deployment
        failed = {
            **failure_base,
            "generated_at": _now(),
            "passed": False,
            "activation_state": "automatic_activation_failed",
            "kept_paused": True,
            "paused": sorted(EXPECTED_DAGS),
            "unpaused": [],
            "error": f"{type(exc).__name__}: {exc}",
            "cleanup_errors": cleanup_errors,
            "automatic_rollout": {
                **dict(rollout),
                "phase": "activation_failed",
                "canary_report": str(args.automatic_canary_report.resolve()),
            },
        }
        _atomic_json(report_path, failed)
        return failed
def _main_locked(args: argparse.Namespace) -> int:
    report_path = args.report or args.evidence_dir / "deployment.json"
    setattr(args, _RUNTIME_MUTATION_STARTED_ATTR, False)
    previous_report = (
        None if args.resume_pending else _existing_report_before_upgrade(report_path)
    )
    try:
        activate_automatic = bool(getattr(args, "activate_automatic", False))
        if not args.resume_pending and not activate_automatic:
            _guard_existing_pending_activation(report_path)
        canary_report = getattr(args, "automatic_canary_report", None)
        if canary_report is not None and not activate_automatic:
            raise DeploymentError(
                "--automatic-canary-report requires --activate-automatic"
            )
        if activate_automatic:
            report = activate_automatic_catalog(args)
        else:
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
    if report.get("durable_pending_report_preserved") is True:
        print(json.dumps(report, ensure_ascii=False, sort_keys=True))
        return 1
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
