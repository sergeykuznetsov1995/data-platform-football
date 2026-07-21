#!/usr/bin/env python3
"""Fail-closed filesystem and health readiness for the SofaScore paid path.

The host-side ``preflight`` validates deployment-owned bind sources for the
fixed container identity (UID 50000/GID 0).  Container health modes additionally
prove the effective identity, perform a real write/unlink probe in gateway
state, load the verified workload policy against the current runtime
fingerprint, and compare its artifact ID with the credential-free gateway
health response.
"""

from __future__ import annotations

import argparse
import json
import os
import re
import stat
import subprocess
import sys
import time
import urllib.error
import urllib.parse
import urllib.request
from collections.abc import Mapping, Sequence
from pathlib import Path
from typing import Any


ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from scrapers.sofascore.workload_plan import (  # noqa: E402
    WorkloadPolicyUnavailable,
    load_verified_workload_policy,
)


RUNTIME_UID = 50_000
RUNTIME_GID = 0
MAX_HEALTH_BYTES = 64 * 1024
ARTIFACT_ID_RE = re.compile(r"\A[0-9a-f]{64}\Z")
ZERO_ARTIFACT_ID = "0" * 64


class ReadinessError(RuntimeError):
    """The paid path is not safe to admit."""


def _identity_has_permissions(
    metadata: os.stat_result, *, uid: int, gid: int, required: int
) -> bool:
    mode = stat.S_IMODE(metadata.st_mode)
    if metadata.st_uid == uid:
        granted = (mode >> 6) & 0o7
    elif metadata.st_gid == gid:
        granted = (mode >> 3) & 0o7
    else:
        granted = mode & 0o7
    return granted & required == required


def _expected_artifact_id(value: str | None) -> str:
    candidate = (
        value
        if value is not None
        else os.environ.get("SOFASCORE_PROXY_BUDGET_ARTIFACT_ID", "")
    ).strip()
    if not candidate:
        raise ReadinessError("expected SofaScore artifact ID is required")
    if ARTIFACT_ID_RE.fullmatch(candidate) is None:
        raise ReadinessError("expected SofaScore artifact ID is not a SHA-256 digest")
    if candidate == ZERO_ARTIFACT_ID:
        raise ReadinessError(
            "expected SofaScore artifact ID cannot use the CI/render-only zero "
            "placeholder"
        )
    return candidate


def require_runtime_identity() -> None:
    if os.geteuid() != RUNTIME_UID or os.getegid() != RUNTIME_GID:
        raise ReadinessError("readiness must run as container UID 50000/GID 0")


def _require_canonical_path(path: Path, *, label: str) -> None:
    try:
        resolved = path.resolve(strict=True)
    except OSError as exc:
        raise ReadinessError(f"{label} is missing") from exc
    if resolved != path:
        raise ReadinessError(f"{label} path must not contain symlinks")


def _stable_file_identity(metadata: os.stat_result) -> tuple[int, ...]:
    return (
        metadata.st_dev,
        metadata.st_ino,
        metadata.st_mode,
        metadata.st_uid,
        metadata.st_gid,
        metadata.st_nlink,
        metadata.st_size,
        metadata.st_mtime_ns,
        metadata.st_ctime_ns,
    )


def _require_root_owned_parent_chain(path: Path, *, label: str) -> None:
    """Require every existing parent, through /, to be root-owned/protected."""

    current = path.parent
    while True:
        try:
            metadata = current.lstat()
        except OSError as exc:
            raise ReadinessError(f"{label} parent chain is missing") from exc
        if (
            not stat.S_ISDIR(metadata.st_mode)
            or current.is_symlink()
            or metadata.st_uid != 0
            or metadata.st_mode & 0o022
        ):
            raise ReadinessError(
                f"{label} parent chain must be root-owned and protected"
            )
        if current.parent == current:
            return
        current = current.parent


def validate_release_root(path: Path) -> Path:
    if not path.is_absolute():
        raise ReadinessError("release root must be absolute")
    _require_canonical_path(path, label="release root")
    try:
        metadata = path.lstat()
    except OSError as exc:
        raise ReadinessError("release root is missing") from exc
    if (
        not stat.S_ISDIR(metadata.st_mode)
        or metadata.st_uid != 0
        or metadata.st_mode & 0o022
    ):
        raise ReadinessError("release root must be a root-owned protected directory")
    _require_root_owned_parent_chain(path, label="release root")
    return path


def require_outside_release_root(release_root: Path, paths: Mapping[str, Path]) -> None:
    root = validate_release_root(release_root)
    for label, path in paths.items():
        if not path.is_absolute():
            raise ReadinessError(f"{label} path must be absolute")
        _require_canonical_path(path, label=label)
        if path == root or root in path.parents:
            raise ReadinessError(f"{label} must be outside the release root")


def validate_artifact(
    path: Path,
    *,
    expected_artifact_id: str | None = None,
    require_effective_access: bool = False,
    require_protected_parents: bool = False,
    runtime_uid: int = RUNTIME_UID,
    runtime_gid: int = RUNTIME_GID,
) -> str:
    """Validate stable bytes, UID-aware readability, and signed policy."""

    if not path.is_absolute():
        raise ReadinessError("SofaScore artifact path must be absolute")
    _require_canonical_path(path, label="SofaScore artifact")
    if require_protected_parents:
        _require_root_owned_parent_chain(path, label="SofaScore artifact")
    expected = _expected_artifact_id(expected_artifact_id)
    descriptor = -1
    try:
        entry_before = path.lstat()
        descriptor = os.open(
            path,
            os.O_RDONLY | getattr(os, "O_CLOEXEC", 0) | getattr(os, "O_NOFOLLOW", 0),
        )
        before = os.fstat(descriptor)
    except OSError as exc:
        if descriptor >= 0:
            os.close(descriptor)
        raise ReadinessError("SofaScore artifact is missing or unreadable") from exc
    try:
        if (
            not stat.S_ISREG(before.st_mode)
            or path.is_symlink()
            or before.st_nlink != 1
            or _stable_file_identity(entry_before) != _stable_file_identity(before)
        ):
            raise ReadinessError(
                "SofaScore artifact must be a stable regular non-symlink file"
            )
        if before.st_mode & 0o022:
            raise ReadinessError("SofaScore artifact must not be group/world writable")
        if require_protected_parents and before.st_uid != 0:
            raise ReadinessError("host SofaScore artifact must be root-owned")
        if not _identity_has_permissions(
            before, uid=runtime_uid, gid=runtime_gid, required=0o4
        ):
            raise ReadinessError(
                f"SofaScore artifact is not readable by UID {runtime_uid}/GID {runtime_gid}"
            )
        if require_effective_access and (
            os.geteuid() != runtime_uid or os.getegid() != runtime_gid
        ):
            raise ReadinessError(
                "SofaScore artifact effective-access check uses the wrong identity"
            )
        try:
            policy = load_verified_workload_policy(Path(f"/proc/self/fd/{descriptor}"))
        except WorkloadPolicyUnavailable as exc:
            raise ReadinessError(
                "SofaScore artifact is not verified for the current runtime"
            ) from exc
        after = os.fstat(descriptor)
        try:
            entry_after = path.lstat()
        except OSError as exc:
            raise ReadinessError(
                "SofaScore artifact changed during verification"
            ) from exc
        if _stable_file_identity(before) != _stable_file_identity(
            after
        ) or _stable_file_identity(after) != _stable_file_identity(entry_after):
            raise ReadinessError("SofaScore artifact changed during verification")
        artifact_id = str(policy.artifact_id)
        if ARTIFACT_ID_RE.fullmatch(artifact_id) is None:
            raise ReadinessError("verified SofaScore artifact returned an invalid ID")
        if artifact_id != expected:
            raise ReadinessError("verified SofaScore artifact ID differs from the pin")
        return artifact_id
    finally:
        os.close(descriptor)


def _probe_state_write(path: Path) -> None:
    directory = -1
    descriptor = -1
    name = f".sofascore-readiness-{os.getpid()}-{time.monotonic_ns()}"
    created = False
    try:
        directory = os.open(
            path,
            os.O_RDONLY
            | os.O_DIRECTORY
            | getattr(os, "O_CLOEXEC", 0)
            | getattr(os, "O_NOFOLLOW", 0),
        )
        descriptor = os.open(
            name,
            os.O_WRONLY
            | os.O_CREAT
            | os.O_EXCL
            | getattr(os, "O_CLOEXEC", 0)
            | getattr(os, "O_NOFOLLOW", 0),
            0o600,
            dir_fd=directory,
        )
        created = True
    except OSError as exc:
        raise ReadinessError(
            "SofaScore gateway state is not writable by the container identity"
        ) from exc
    finally:
        if descriptor >= 0:
            os.close(descriptor)
        if directory >= 0:
            if created:
                try:
                    os.unlink(name, dir_fd=directory)
                except OSError as exc:
                    raise ReadinessError(
                        "SofaScore gateway state readiness probe cannot be removed"
                    ) from exc
            os.close(directory)


def validate_state_directory(
    path: Path,
    *,
    probe_write: bool = False,
    require_protected_parents: bool = False,
    runtime_uid: int = RUNTIME_UID,
    runtime_gid: int = RUNTIME_GID,
) -> None:
    """Require a protected directory writable/traversable by UID 50000/GID 0."""

    if not path.is_absolute():
        raise ReadinessError("SofaScore gateway state path must be absolute")
    _require_canonical_path(path, label="SofaScore gateway state")
    if require_protected_parents:
        _require_root_owned_parent_chain(path, label="SofaScore gateway state")
    try:
        metadata = path.lstat()
    except OSError as exc:
        raise ReadinessError("SofaScore gateway state directory is missing") from exc
    if not stat.S_ISDIR(metadata.st_mode) or path.is_symlink():
        raise ReadinessError("SofaScore gateway state must be a non-symlink directory")
    if metadata.st_mode & stat.S_IWOTH:
        raise ReadinessError("SofaScore gateway state must not be world writable")
    if not _identity_has_permissions(
        metadata, uid=runtime_uid, gid=runtime_gid, required=0o3
    ):
        raise ReadinessError(
            "SofaScore gateway state is not writable/traversable by "
            f"UID {runtime_uid}/GID {runtime_gid}"
        )
    if probe_write:
        _probe_state_write(path)


def read_gateway_health(url: str, *, timeout_seconds: float = 2.0) -> Mapping[str, Any]:
    parsed = urllib.parse.urlsplit(url)
    if (
        parsed.scheme != "http"
        or not parsed.hostname
        or parsed.username is not None
        or parsed.password is not None
        or parsed.query
        or parsed.fragment
    ):
        raise ReadinessError("gateway health URL must be credential-free HTTP")
    request = urllib.request.Request(url, headers={"Accept": "application/json"})
    # Internal readiness must never inherit HTTP(S)_PROXY/ALL_PROXY from the
    # host or container environment, even when NO_PROXY is misconfigured.
    opener = urllib.request.build_opener(urllib.request.ProxyHandler({}))
    try:
        with opener.open(request, timeout=timeout_seconds) as response:
            raw = response.read(MAX_HEALTH_BYTES + 1)
    except (OSError, ValueError, urllib.error.URLError) as exc:
        raise ReadinessError("gateway health endpoint is unavailable") from exc
    if len(raw) > MAX_HEALTH_BYTES:
        raise ReadinessError("gateway health response is too large")
    try:
        payload = json.loads(raw.decode("utf-8"))
    except (UnicodeDecodeError, json.JSONDecodeError) as exc:
        raise ReadinessError("gateway health response is not valid JSON") from exc
    if not isinstance(payload, Mapping):
        raise ReadinessError("gateway health response must be an object")
    return payload


def validate_gateway_health(
    payload: Mapping[str, Any],
    *,
    artifact_id: str,
    expected_artifact_id: str | None = None,
) -> None:
    expected = _expected_artifact_id(expected_artifact_id)
    observed = payload.get("sofascore_budget_artifact_id")
    budget = payload.get("sofascore_dagrun_budget_bytes")
    if payload.get("status") != "ok":
        raise ReadinessError("gateway health status is not ok")
    if payload.get("sofascore_paid_enabled") is not True:
        raise ReadinessError("gateway has SofaScore paid traffic disabled")
    if type(budget) is not int or budget <= 0:
        raise ReadinessError("gateway has no positive SofaScore paid budget")
    if observed != artifact_id:
        raise ReadinessError("gateway loaded a different SofaScore artifact")
    if observed != expected:
        raise ReadinessError("gateway SofaScore artifact differs from the pin")


def require_scheduler_job(*, timeout_seconds: float = 20.0) -> None:
    hostname = os.environ.get("HOSTNAME", "").strip()
    if not hostname:
        raise ReadinessError("scheduler hostname is unavailable")
    try:
        result = subprocess.run(
            [
                "airflow",
                "jobs",
                "check",
                "--job-type",
                "SchedulerJob",
                "--hostname",
                hostname,
            ],
            stdin=subprocess.DEVNULL,
            stdout=subprocess.DEVNULL,
            stderr=subprocess.DEVNULL,
            check=False,
            timeout=timeout_seconds,
        )
    except (OSError, subprocess.TimeoutExpired) as exc:
        raise ReadinessError("Airflow scheduler job check is unavailable") from exc
    if result.returncode != 0:
        raise ReadinessError("Airflow scheduler job is not healthy")


def _parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description=__doc__)
    commands = parser.add_subparsers(dest="mode", required=True)

    def artifact_arguments(command: argparse.ArgumentParser) -> None:
        command.add_argument("--artifact", required=True, type=Path)
        command.add_argument("--expected-artifact-id")

    preflight = commands.add_parser("preflight")
    artifact_arguments(preflight)
    preflight.add_argument("--release-root", required=True, type=Path)
    preflight.add_argument("--state-dir", required=True, type=Path)

    gateway = commands.add_parser("gateway-health")
    artifact_arguments(gateway)
    gateway.add_argument("--state-dir", required=True, type=Path)
    gateway.add_argument("--health-url", required=True)

    scheduler = commands.add_parser("scheduler-health")
    artifact_arguments(scheduler)
    scheduler.add_argument("--health-url", required=True)
    return parser


def run(arguments: Sequence[str] | None = None) -> dict[str, str]:
    args = _parser().parse_args(arguments)
    expected = _expected_artifact_id(args.expected_artifact_id)
    if args.mode == "preflight":
        require_outside_release_root(
            args.release_root,
            {
                "SofaScore artifact": args.artifact,
                "SofaScore gateway state": args.state_dir,
            },
        )
        artifact_id = validate_artifact(
            args.artifact,
            expected_artifact_id=expected,
            require_protected_parents=True,
        )
        validate_state_directory(
            args.state_dir,
            require_protected_parents=True,
        )
    else:
        require_runtime_identity()
        artifact_id = validate_artifact(
            args.artifact,
            expected_artifact_id=expected,
            require_effective_access=True,
        )
        if args.mode == "gateway-health":
            validate_state_directory(args.state_dir, probe_write=True)
        payload = read_gateway_health(args.health_url)
        validate_gateway_health(
            payload,
            artifact_id=artifact_id,
            expected_artifact_id=expected,
        )
        if args.mode == "scheduler-health":
            require_scheduler_job()
    return {"artifact_id": artifact_id, "mode": args.mode, "status": "ok"}


def main(arguments: Sequence[str] | None = None) -> int:
    try:
        report = run(arguments)
    except ReadinessError as exc:
        print(f"SofaScore readiness failed: {exc}", file=sys.stderr)
        return 1
    print(json.dumps(report, sort_keys=True, separators=(",", ":")))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
