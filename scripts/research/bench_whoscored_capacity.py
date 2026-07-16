#!/usr/bin/env python3
"""Run a sustained, four-worker, non-publishing WhoScored capacity canary.

Each worker repeatedly executes ``bench_whoscored_workflow.py`` in an
independent process.  That workflow uses a temporary local raw store and an
in-memory repository, so this supervisor cannot publish Bronze rows, manifests
or DDL.  The production-safe defaults run four workers for six hours and fail
closed unless all of these gates hold:

* projected throughput is at least 144,000 source page units/day;
* no paid route or paid byte is observed;
* harness process-tree RSS plus monitored Docker cgroup memory stays at or
  below 12 GiB;
* monitored containers do not restart, get recreated, stop, or report OOM;
* every worker completes at least one successful non-publishing workflow.

Run this supervisor on the Docker host, not inside an Airflow container: it
uses host ``docker inspect`` and ``/proc`` to bind RSS, restart, recreation and
OOM evidence to the canary window.  The JSON report contains only counters and
operational evidence.  It never copies parser payloads, raw pages, request
URLs, credentials, or child reports.
"""

from __future__ import annotations

import argparse
import ctypes
from dataclasses import dataclass, field
from datetime import datetime, timezone
import fcntl
import hashlib
import importlib
import importlib.metadata
import ipaddress
import json
import math
import os
from pathlib import Path
import re
import secrets
import signal
import stat
import subprocess
import sys
from tempfile import mkstemp, TemporaryDirectory
import time
from typing import Any, Callable, Mapping, Optional, Sequence
from urllib.parse import urlsplit

import requests
from requests.adapters import HTTPAdapter


REPO_ROOT = Path(__file__).resolve().parents[2]
WORKFLOW_SCRIPT = REPO_ROOT / "scripts" / "research" / "bench_whoscored_workflow.py"
FLARESOLVERR_EXTENSION_SCRIPT = REPO_ROOT / "scripts" / "flaresolverr_extended.py"
WORKER_EXEC_SCRIPT = (
    REPO_ROOT / "scripts" / "research" / "whoscored_capacity_worker_exec.py"
)

CANARY_VERSION = "whoscored-capacity-canary-v2"
EXPECTED_WORKFLOW_VERSION = "whoscored-workflow-benchmark-v2"
REQUIRED_CURL_CFFI_VERSION = "0.15.0"
REQUIRED_FLARESOLVERR_VERSION = "3.4.6"
REQUIRED_FLARESOLVERR_IMAGE_REFERENCE = (
    "data-platform-flaresolverr-whoscored:3.4.6"
)
REQUIRED_FLARESOLVERR_ENDPOINT = "http://127.0.0.1:8191"
REQUIRED_FLARESOLVERR_COMMAND = (
    "/usr/local/bin/whoscored-flaresolverr-entrypoint",
)
REQUIRED_FLARESOLVERR_ENTRYPOINT = ("/usr/bin/dumb-init", "--")
REQUIRED_FLARESOLVERR_TMPFS = {
    "/tmp": "rw,exec,nosuid,nodev,size=2g,uid=1000,gid=1000,mode=1770",
    "/config": "rw,noexec,nosuid,nodev,size=16m,uid=1000,gid=1000,mode=0700",
    "/app/.config": "rw,noexec,nosuid,nodev,size=64m,uid=1000,gid=1000,mode=0700",
    "/app/.local": "rw,noexec,nosuid,nodev,size=64m,uid=1000,gid=1000,mode=0700",
}
REQUIRED_COMPOSE_PROJECT = "data-platform"
PRODUCTION_COMPOSE_WRAPPER = REPO_ROOT / "scripts" / "compose.sh"
PRODUCTION_COMPOSE_FILES = (
    REPO_ROOT / "compose.yaml",
    REPO_ROOT / "compose.seaweedfs-supervised.yaml",
)
PRODUCTION_COMPOSE_ENV_FILES = (
    Path("/root/data-platform-football/.env"),
    Path("/root/data-platform-football/.env.whoscored-rollout"),
    Path("/root/data-platform-football/.env.proxy-pool.whoscored-v2"),
)
CAPACITY_SOURCE_CIRCUIT_PATH = (
    REPO_ROOT / "logs" / "whoscored" / "source-circuit-v1.json"
)
WORKER_NAMESPACE_EXECUTABLE = Path("/usr/bin/unshare")
REQUIRED_UNSHARE_VERSION = "unshare from util-linux 2.39.3"
REQUIRED_UNSHARE_SHA256 = (
    "51bcc77ba5db162c80028f861f0a2770d728c1de80773816d863f28d7a817adb"
)
WORKER_COUNT = 4
DEFAULT_DURATION_SECONDS = 6 * 60 * 60
DEFAULT_SAMPLE_INTERVAL_SECONDS = 30.0
DEFAULT_SCOPES = ("INT-World Cup=2026", "ENG-Premier League=2526")
DEFAULT_MATCH_LIMIT = 3
DEFAULT_PROFILE_LIMIT = 3
MAX_MATCH_LIMIT = 10
MAX_PROFILE_LIMIT = 20
DEFAULT_CONTAINERS = ("airflow-scheduler", "flaresolverr", "proxy_filter")
MIN_PAGE_UNITS_PER_DAY = 144_000
GIB = 1024**3
MAX_RSS_BYTES = 12 * GIB
_RUNTIME_STATIC_PATHS = (
    ".dockerignore",
    "compose.seaweedfs-supervised.yaml",
    "compose.yaml",
    "configs/seaweedfs/S3ProxyCaddyfile",
    "docker/images/flaresolverr-whoscored/Dockerfile",
    "docker/images/flaresolverr-whoscored/Dockerfile.dockerignore",
    "docker/images/flaresolverr-whoscored/entrypoint.sh",
    "scripts/research/bench_whoscored_capacity.py",
    "scripts/research/bench_whoscored_workflow.py",
    "scripts/research/whoscored_capacity_worker_exec.py",
    "scripts/flaresolverr_extended.py",
    "scripts/audit_seaweedfs_control_network.py",
    "scripts/audit_seaweedfs_runtime_container.py",
    "scripts/compose.sh",
    "scripts/seaweedfs_legacy_entrypoint.sh",
    "scripts/seaweedfs_lifecycle_lock.sh",
    "scripts/validate_seaweedfs_s3_identity_config.py",
    "scripts/proxy_filter/filter_proxy.py",
    "docker/images/airflow/requirements-scraping.txt",
    "scrapers/base/flaresolverr_client.py",
    "scrapers/base/iceberg_writer.py",
    "scrapers/base/sql_validator.py",
    "scrapers/base/trino_manager.py",
    "scrapers/utils/rate_limiter.py",
)
_POLL_SECONDS = 0.25
_TERMINATE_GRACE_SECONDS = 10.0
_KILL_CONFIRM_SECONDS = 5.0
_SESSION_OWNER_RE = re.compile(r"[a-z0-9]{16,32}")
_SESSION_PREFIX_TEMPLATE = "ws-cap-{owner}-"
_OWNER_STATE_SCHEMA_VERSION = 1
_DEFAULT_SUPERVISOR_LOCK_PATH = Path(
    "/tmp/whoscored-capacity-supervisor.lock"
)
_DEFAULT_SESSION_OWNER_PATH = Path(
    "/tmp/whoscored-capacity-session-owner.json"
)
_SESSION_API_TIMEOUT_SECONDS = 3.0
# A killed child may already have sent sessions.create.  FlareSolverr can keep
# working on that request for the child's 90 second HTTP timeout, so two quick
# empty lists are not sufficient proof that no late session can appear.
_SESSION_QUIET_SECONDS = 95.0
_SESSION_CLEANUP_DEADLINE_SECONDS = 125.0
_SESSION_SCAN_INTERVAL_SECONDS = 2.0
_SESSION_CLEANUP_MAX_SCANS = 80
_SESSION_CLEANUP_PATH = "/v1/whoscored/capacity-sessions/cleanup"
_PR_GET_PDEATHSIG = 2
_LIFECYCLE_COUNT_FIELDS = (
    "active",
    "pending_create",
    "pending_destroy",
    "failed_create",
    "failed_destroy",
)
_CAPACITY_CONTROL_SCHEMA_VERSION = 1
_CAPACITY_CONTROL_PAYLOAD_LIMIT = 512
_URL_RE = re.compile(r"https?://\S+", re.IGNORECASE)
_MEMORY_SIZE_RE = re.compile(r"^([0-9]+(?:\.[0-9]+)?)\s*([KMGT]?i?B)$")
_MEMORY_SIZE_MULTIPLIERS = {
    "B": 1,
    "KB": 1000,
    "MB": 1000**2,
    "GB": 1000**3,
    "TB": 1000**4,
    "KiB": 1024,
    "MiB": 1024**2,
    "GiB": 1024**3,
    "TiB": 1024**4,
}


@dataclass(frozen=True)
class WorkerCommand:
    """One isolated workflow invocation in a fixed four-worker round."""

    worker_id: int
    iteration: int
    scope: str
    argv: tuple[str, ...]
    browser_session_owner: Optional[str] = None
    flaresolverr_endpoint: Optional[str] = None


@dataclass(frozen=True)
class WorkerOutcome:
    """Sanitized process result; stdout payloads are reduced before retention."""

    worker_id: int
    iteration: int
    scope: str
    returncode: Optional[int]
    report: Optional[Mapping[str, Any]]
    elapsed_seconds: float
    stderr_bytes: int
    stderr_sha256: str
    termination_reason: Optional[str] = None
    launch_error: Optional[str] = None


@dataclass
class CapacityAccumulator:
    """Mutable aggregate shared by outcome and monitoring callbacks."""

    page_units: int = 0
    source_request_attempts: int = 0
    paid_bytes: int = 0
    paid_route_requests: int = 0
    completed_runs: int = 0
    completed_by_worker: dict[int, int] = field(
        default_factory=lambda: {worker_id: 0 for worker_id in range(WORKER_COUNT)}
    )
    worker_seconds: float = 0.0
    deadline_truncations: int = 0
    run_summaries: list[dict[str, Any]] = field(default_factory=list)
    worker_errors: list[str] = field(default_factory=list)
    safety_violations: list[str] = field(default_factory=list)
    traffic_evidence_violations: list[str] = field(default_factory=list)
    monitoring_errors: list[str] = field(default_factory=list)
    container_violations: list[str] = field(default_factory=list)
    samples: list[dict[str, Any]] = field(default_factory=list)
    baseline_containers: dict[str, dict[str, Any]] = field(default_factory=dict)
    max_harness_rss_bytes: int = 0
    max_container_memory_bytes: int = 0
    max_aggregate_memory_bytes: int = 0
    max_source_stage_count: int = 0
    runtime_identity: dict[str, Any] = field(default_factory=dict)
    runtime_identity_violations: list[str] = field(default_factory=list)
    interrupted: bool = False


@dataclass(frozen=True)
class CapacityDependencies:
    """Small dependency seam for deterministic supervisor unit tests."""

    monotonic: Callable[[], float]
    sleep: Callable[[float], None]
    inspect_containers: Callable[[Sequence[str]], Mapping[str, Mapping[str, Any]]]
    sample_rss: Callable[[Sequence[int]], Mapping[str, Any]]
    runtime_identity: Callable[[argparse.Namespace], Mapping[str, Any]]
    run_round: Callable[..., None]
    prepare_session_ownership: Optional[Callable[..., Any]] = None


@dataclass
class _SessionOwnershipLease:
    """One supervisor's private browser-session namespace and host lock."""

    owner: str
    preflight: Mapping[str, Any]
    finalize_callback: Callable[[], Mapping[str, Any]]
    close_callback: Callable[[], None]

    def finalize(self) -> Mapping[str, Any]:
        return self.finalize_callback()

    def close(self) -> None:
        self.close_callback()


class _SessionOwnershipPreparationError(RuntimeError):
    """Safe preparation failure carrying only sanitized cleanup evidence."""

    def __init__(self, reason: str, evidence: Mapping[str, Any]) -> None:
        super().__init__(reason)
        self.reason = reason
        self.evidence = dict(evidence)


class _SupervisorTermination(BaseException):
    """Legacy explicit termination marker used only at test seams."""

    def __init__(self, signum: int) -> None:
        super().__init__(signum)
        self.signum = signum


class _WorkerTerminationUnverified(RuntimeError):
    """A tracked worker remained alive after the bounded TERM/KILL sequence."""


@dataclass
class _RunningProcess:
    command: WorkerCommand
    process: subprocess.Popen[str]
    stdout_handle: Any
    stderr_handle: Any
    started_at: float


def _utc_now() -> str:
    return datetime.now(timezone.utc).isoformat()


def _safe_message(value: Any, *, limit: int = 500) -> str:
    """Bound errors and remove URLs, which can contain request context."""

    message = _URL_RE.sub("<url-redacted>", str(value or "")).replace("\x00", "")
    return message[:limit]


def _json_safe_document(value: Mapping[str, Any]) -> dict[str, Any]:
    """Return one detached JSON-safe document for the final report."""

    return json.loads(json.dumps(value, sort_keys=True, default=str))


def _validate_session_owner(owner: str) -> str:
    """Return an exact safe capacity owner token or fail closed."""

    if type(owner) is not str or _SESSION_OWNER_RE.fullmatch(owner) is None:
        raise ValueError("invalid capacity browser-session owner")
    return owner


def _session_prefix(owner: str) -> str:
    return _SESSION_PREFIX_TEMPLATE.format(owner=_validate_session_owner(owner))


def _canonical_flaresolverr_endpoint(value: Any) -> str:
    """Return one production-safe loopback HTTP origin or fail closed."""

    if type(value) is not str or not value or value != value.strip():
        raise ValueError("invalid FlareSolverr endpoint")
    try:
        parsed = urlsplit(value)
        port = parsed.port
    except ValueError as exc:
        raise ValueError("invalid FlareSolverr endpoint") from exc
    if (
        parsed.scheme != "http"
        or parsed.username is not None
        or parsed.password is not None
        or parsed.query
        or parsed.fragment
        or parsed.path not in {"", "/"}
        or parsed.hostname is None
        or port is None
    ):
        raise ValueError("invalid FlareSolverr endpoint")
    try:
        host = ipaddress.ip_address(parsed.hostname)
    except ValueError as exc:
        raise ValueError("FlareSolverr endpoint must use a loopback IP") from exc
    if not host.is_loopback:
        raise ValueError("FlareSolverr endpoint must use a loopback IP")
    rendered_host = f"[{host.compressed}]" if host.version == 6 else host.compressed
    canonical = f"http://{rendered_host}:{port}"
    if value != canonical:
        raise ValueError("FlareSolverr endpoint is not canonical")
    return canonical


def _worker_exec_preflight() -> Optional[str]:
    """Prove the Linux parent-death helper can arm prctl before any worker."""

    if sys.platform != "linux":
        return "capacity worker parent-death protection requires Linux"
    if not WORKER_EXEC_SCRIPT.is_file():
        return "capacity worker parent-death helper is unavailable"
    if not WORKER_NAMESPACE_EXECUTABLE.is_file():
        return "capacity worker PID-namespace helper is unavailable"
    try:
        unshare_sha256 = hashlib.sha256(
            WORKER_NAMESPACE_EXECUTABLE.read_bytes()
        ).hexdigest()
    except OSError:
        return "capacity worker PID-namespace helper is unavailable"
    if unshare_sha256 != REQUIRED_UNSHARE_SHA256:
        return "capacity worker PID-namespace helper hash mismatch"
    try:
        version_result = subprocess.run(
            [str(WORKER_NAMESPACE_EXECUTABLE), "--version"],
            check=False,
            capture_output=True,
            text=True,
            timeout=3,
            env={**os.environ, "LC_ALL": "C"},
        )
        capability_result = subprocess.run(
            [
                str(WORKER_NAMESPACE_EXECUTABLE),
                "--pid",
                "--fork",
                "--kill-child=SIGKILL",
                "--",
                sys.executable,
                "-c",
                "pass",
            ],
            check=False,
            capture_output=True,
            text=True,
            timeout=3,
        )
    except (OSError, subprocess.SubprocessError):
        return "capacity worker PID-namespace helper is unavailable"
    if (
        version_result.returncode != 0
        or version_result.stdout.strip() != REQUIRED_UNSHARE_VERSION
    ):
        return "capacity worker PID-namespace helper version mismatch"
    if capability_result.returncode != 0:
        return "capacity worker PID namespace is unavailable"
    try:
        prctl = ctypes.CDLL(None, use_errno=True).prctl
    except (AttributeError, OSError):
        return "capacity worker parent-death prctl is unavailable"
    if prctl is None:  # pragma: no cover - defensive ctypes contract
        return "capacity worker parent-death prctl is unavailable"
    current_signal = ctypes.c_int()
    prctl.argtypes = [
        ctypes.c_int,
        ctypes.c_void_p,
        ctypes.c_ulong,
        ctypes.c_ulong,
        ctypes.c_ulong,
    ]
    prctl.restype = ctypes.c_int
    if prctl(
        _PR_GET_PDEATHSIG, ctypes.byref(current_signal), 0, 0, 0
    ) != 0:
        return "capacity worker parent-death prctl is unavailable"
    return None


def _cleanup_error_hash(operation: str, exc: BaseException) -> str:
    """Hash only a generic error class; never retain an API URL or session ID."""

    material = f"{operation}:{type(exc).__module__}.{type(exc).__name__}"
    return hashlib.sha256(material.encode("ascii", errors="replace")).hexdigest()


def _empty_cleanup_result(*, required: bool, verified: bool) -> dict[str, Any]:
    return {
        "required": required,
        "verified_zero": verified,
        "quiet_window_observed": verified,
        "poll_attempts": 0,
        "successful_polls": 0,
        "zero_scans": 0,
        "final_zero_scans": 0,
        **{f"{field_name}_max": 0 for field_name in _LIFECYCLE_COUNT_FIELDS},
        "failure_generation_initial": 0,
        "failure_generation_final": 0,
        "failure_generation_changed": False,
        "deadline_exhausted": False,
        "error_count": 0,
        "error_sha256": [],
    }


def _combine_cleanup_results(
    *results: Mapping[str, Any],
) -> dict[str, Any]:
    """Combine multiple safe pre-worker sweeps without retaining identities."""

    required_results = [result for result in results if result.get("required")]
    combined = _empty_cleanup_result(
        required=bool(required_results),
        verified=all(result.get("verified_zero") is True for result in results),
    )
    combined["quiet_window_observed"] = all(
        not result.get("required")
        or result.get("quiet_window_observed") is True
        for result in results
    )
    for field_name in (
        "poll_attempts",
        "successful_polls",
        "zero_scans",
        "final_zero_scans",
        "error_count",
    ):
        combined[field_name] = sum(
            int(result.get(field_name, 0)) for result in results
        )
    for field_name in _LIFECYCLE_COUNT_FIELDS:
        maximum_name = f"{field_name}_max"
        combined[maximum_name] = max(
            (int(result.get(maximum_name, 0)) for result in results),
            default=0,
        )
    combined["failure_generation_initial"] = max(
        (
            int(result.get("failure_generation_initial", 0))
            for result in results
        ),
        default=0,
    )
    combined["failure_generation_final"] = max(
        (
            int(result.get("failure_generation_final", 0))
            for result in results
        ),
        default=0,
    )
    combined["failure_generation_changed"] = any(
        result.get("failure_generation_changed") is True for result in results
    )
    combined["deadline_exhausted"] = any(
        result.get("deadline_exhausted") is True for result in results
    )
    combined["error_sha256"] = sorted(
        {
            error_hash
            for result in results
            for error_hash in result.get("error_sha256", [])
            if type(error_hash) is str
        }
    )
    return combined


class _SessionApiProtocolError(RuntimeError):
    pass


def _current_flaresolverr_extension_sha256() -> str:
    """Hash the helper bytes that the running container must have loaded."""

    try:
        payload = FLARESOLVERR_EXTENSION_SCRIPT.read_bytes()
    except OSError as exc:
        raise _SessionApiProtocolError(
            "FlareSolverr extension helper is unavailable"
        ) from exc
    return hashlib.sha256(payload).hexdigest()


def _fresh_session_api_client() -> requests.Session:
    """Build a direct, non-retrying client used only by the host supervisor."""

    session = requests.Session()
    session.trust_env = False
    adapter = HTTPAdapter(max_retries=0)
    session.mount("http://", adapter)
    session.mount("https://", adapter)
    return session


def _session_api_post(
    session: requests.Session,
    flaresolverr_url: str,
    owner: str,
) -> Mapping[str, Any]:
    endpoint = _canonical_flaresolverr_endpoint(flaresolverr_url)
    owner = _validate_session_owner(owner)
    response = session.post(
        f"{endpoint}{_SESSION_CLEANUP_PATH}",
        json={"owner": owner},
        timeout=_SESSION_API_TIMEOUT_SECONDS,
        allow_redirects=False,
    )
    if response.status_code != 200:
        raise _SessionApiProtocolError("unexpected http status")
    try:
        body = response.json()
    except ValueError as exc:
        raise _SessionApiProtocolError("non-json response") from exc
    expected_fields = {
        "status",
        "version",
        "extension_sha256",
        *_LIFECYCLE_COUNT_FIELDS,
        "failure_generation",
        "cleanup_scheduled",
    }
    if not isinstance(body, Mapping) or set(body) != expected_fields:
        raise _SessionApiProtocolError("invalid cleanup response shape")
    if type(body["status"]) is not str or body["status"] != "ok":
        raise _SessionApiProtocolError("cleanup response status is not ok")
    if (
        type(body["version"]) is not str
        or body["version"] != REQUIRED_FLARESOLVERR_VERSION
    ):
        raise _SessionApiProtocolError("cleanup response version mismatch")
    extension_sha256 = body["extension_sha256"]
    if (
        type(extension_sha256) is not str
        or re.fullmatch(r"[0-9a-f]{64}", extension_sha256) is None
        or extension_sha256 != _current_flaresolverr_extension_sha256()
    ):
        raise _SessionApiProtocolError("cleanup response extension mismatch")
    for field_name in (*_LIFECYCLE_COUNT_FIELDS, "failure_generation"):
        value = body[field_name]
        if type(value) is not int or value < 0:
            raise _SessionApiProtocolError("invalid cleanup lifecycle counter")
    if type(body["cleanup_scheduled"]) is not bool:
        raise _SessionApiProtocolError("invalid cleanup scheduling flag")
    if body["cleanup_scheduled"] is not True:
        raise _SessionApiProtocolError("cleanup was not scheduled")
    return body


def _sweep_owned_browser_sessions(
    *,
    flaresolverr_url: str,
    owner: str,
    stale: bool = False,
    monotonic: Callable[[], float],
    sleep: Callable[[float], None],
) -> dict[str, Any]:
    """Schedule owner cleanup until a full late-create window is proven quiet."""

    endpoint = _canonical_flaresolverr_endpoint(flaresolverr_url)
    owner = _validate_session_owner(owner)
    result = _empty_cleanup_result(required=True, verified=False)
    deadline = monotonic() + _SESSION_CLEANUP_DEADLINE_SECONDS
    zero_started_at: Optional[float] = None
    baseline_generation: Optional[int] = None
    previous_generation: Optional[int] = None
    sticky_failure = False
    error_hashes: set[str] = set()

    client = _fresh_session_api_client()
    try:
        while (
            result["poll_attempts"] < _SESSION_CLEANUP_MAX_SCANS
            and monotonic() <= deadline
        ):
            result["poll_attempts"] += 1
            try:
                snapshot = _session_api_post(client, endpoint, owner)
            except Exception as exc:
                sticky_failure = True
                result["error_count"] += 1
                error_hashes.add(_cleanup_error_hash("cleanup_poll", exc))
                zero_started_at = None
                result["final_zero_scans"] = 0
            else:
                result["successful_polls"] += 1
                generation = int(snapshot["failure_generation"])
                if baseline_generation is None:
                    baseline_generation = generation
                    result["failure_generation_initial"] = generation
                if previous_generation is not None and generation < previous_generation:
                    sticky_failure = True
                    result["error_count"] += 1
                    error_hashes.add(
                        _cleanup_error_hash(
                            "failure_generation",
                            _SessionApiProtocolError("generation decreased"),
                        )
                    )
                previous_generation = generation
                result["failure_generation_final"] = generation

                if (not stale and generation != 0) or (
                    stale
                    and baseline_generation is not None
                    and generation != baseline_generation
                ):
                    sticky_failure = True
                    result["failure_generation_changed"] = True

                counts = {
                    field_name: int(snapshot[field_name])
                    for field_name in _LIFECYCLE_COUNT_FIELDS
                }
                for field_name, value in counts.items():
                    maximum_name = f"{field_name}_max"
                    result[maximum_name] = max(result[maximum_name], value)

                # A fresh owner must never have a recorded lifecycle failure.
                # A stale owner may begin with failed_destroy entries, which
                # this endpoint retries, but no other failure is acceptable.
                if counts["failed_create"] or (
                    not stale and counts["failed_destroy"]
                ):
                    sticky_failure = True

                all_zero = all(value == 0 for value in counts.values())
                now = monotonic()
                if not all_zero:
                    zero_started_at = None
                    result["final_zero_scans"] = 0
                else:
                    result["zero_scans"] += 1
                    if zero_started_at is None:
                        zero_started_at = now
                    elif now - zero_started_at >= _SESSION_QUIET_SECONDS:
                        result["quiet_window_observed"] = True
                        result["final_zero_scans"] += 1
                        if result["final_zero_scans"] >= 2:
                            result["verified_zero"] = not sticky_failure
                            break

            remaining = deadline - monotonic()
            if remaining <= 0:
                break
            sleep(min(_SESSION_SCAN_INTERVAL_SECONDS, remaining))
    finally:
        client.close()

    result["deadline_exhausted"] = bool(
        not result["verified_zero"]
        and (
            monotonic() >= deadline
            or result["poll_attempts"] >= _SESSION_CLEANUP_MAX_SCANS
        )
    )
    result["error_sha256"] = sorted(error_hashes)
    return result


def _probe_fresh_session_owner(
    *, flaresolverr_url: str, owner: str
) -> dict[str, Any]:
    """Require one exact zero lifecycle ACK before any worker can launch."""

    endpoint = _canonical_flaresolverr_endpoint(flaresolverr_url)
    owner = _validate_session_owner(owner)
    result = _empty_cleanup_result(required=True, verified=False)
    result["poll_attempts"] = 1
    client = _fresh_session_api_client()
    try:
        try:
            snapshot = _session_api_post(client, endpoint, owner)
        except Exception as exc:
            result["error_count"] = 1
            result["error_sha256"] = [
                _cleanup_error_hash("fresh_owner_probe", exc)
            ]
            return result
    finally:
        client.close()

    result["successful_polls"] = 1
    generation = int(snapshot["failure_generation"])
    result["failure_generation_initial"] = generation
    result["failure_generation_final"] = generation
    result["failure_generation_changed"] = generation != 0
    counts = {
        field_name: int(snapshot[field_name])
        for field_name in _LIFECYCLE_COUNT_FIELDS
    }
    for field_name, value in counts.items():
        result[f"{field_name}_max"] = value
    if generation == 0 and all(value == 0 for value in counts.values()):
        result["zero_scans"] = 1
        result["verified_zero"] = True
    return result


def _state_paths(args: argparse.Namespace) -> tuple[Path, Path]:
    del args
    return _DEFAULT_SUPERVISOR_LOCK_PATH, _DEFAULT_SESSION_OWNER_PATH


def _acquire_supervisor_lock(path: Path) -> int:
    flags = os.O_RDWR | os.O_CREAT
    if hasattr(os, "O_NOFOLLOW"):
        flags |= os.O_NOFOLLOW
    descriptor = os.open(path, flags, 0o600)
    try:
        os.fchmod(descriptor, 0o600)
        fcntl.flock(descriptor, fcntl.LOCK_EX | fcntl.LOCK_NB)
    except BaseException:
        os.close(descriptor)
        raise
    return descriptor


def _read_owner_state(path: Path) -> tuple[str, str]:
    flags = os.O_RDONLY
    if hasattr(os, "O_NOFOLLOW"):
        flags |= os.O_NOFOLLOW
    descriptor = os.open(path, flags)
    try:
        metadata = os.fstat(descriptor)
        if not stat.S_ISREG(metadata.st_mode):
            raise ValueError("ownership state is not a regular file")
        if stat.S_IMODE(metadata.st_mode) != 0o600 or metadata.st_size > 1024:
            raise ValueError("ownership state metadata is invalid")
        payload = os.read(descriptor, 1025)
    finally:
        os.close(descriptor)
    try:
        parsed = json.loads(payload.decode("utf-8"))
    except (UnicodeDecodeError, json.JSONDecodeError) as exc:
        raise ValueError("ownership state is invalid") from exc
    if not isinstance(parsed, Mapping) or set(parsed) != {
        "schema_version",
        "owner",
        "flaresolverr_endpoint",
    }:
        raise ValueError("ownership state shape is invalid")
    if type(parsed["schema_version"]) is not int or (
        parsed["schema_version"] != _OWNER_STATE_SCHEMA_VERSION
    ):
        raise ValueError("ownership state schema version is invalid")
    owner = _validate_session_owner(parsed["owner"])
    endpoint = _canonical_flaresolverr_endpoint(parsed["flaresolverr_endpoint"])
    return owner, endpoint


def _fsync_directory(path: Path) -> None:
    descriptor = os.open(path, os.O_RDONLY)
    try:
        os.fsync(descriptor)
    finally:
        os.close(descriptor)


def _write_owner_state(path: Path, owner: str, flaresolverr_endpoint: str) -> None:
    owner = _validate_session_owner(owner)
    endpoint = _canonical_flaresolverr_endpoint(flaresolverr_endpoint)
    payload = json.dumps(
        {
            "schema_version": _OWNER_STATE_SCHEMA_VERSION,
            "owner": owner,
            "flaresolverr_endpoint": endpoint,
        },
        sort_keys=True,
        separators=(",", ":"),
    ).encode("utf-8")
    descriptor, temporary_name = mkstemp(
        prefix=f".{path.name}.", suffix=".tmp", dir=path.parent
    )
    temporary = Path(temporary_name)
    try:
        os.fchmod(descriptor, 0o600)
        with os.fdopen(descriptor, "wb") as handle:
            handle.write(payload)
            handle.flush()
            os.fsync(handle.fileno())
        os.link(temporary, path)
        _fsync_directory(path.parent)
    finally:
        temporary.unlink(missing_ok=True)


def _remove_owner_state(
    path: Path, owner: str, flaresolverr_endpoint: str
) -> bool:
    current_owner, current_endpoint = _read_owner_state(path)
    if (
        current_owner != _validate_session_owner(owner)
        or current_endpoint
        != _canonical_flaresolverr_endpoint(flaresolverr_endpoint)
    ):
        return False
    path.unlink()
    _fsync_directory(path.parent)
    return True


def _ownership_evidence(
    *,
    lock_acquired: bool,
    preflight: Mapping[str, Any],
    final: Mapping[str, Any],
    state_file_removed: bool,
) -> dict[str, Any]:
    error_hashes = sorted(
        {
            *preflight.get("error_sha256", []),
            *final.get("error_sha256", []),
        }
    )
    return {
        "lock_acquired": lock_acquired,
        "preflight_required": bool(preflight.get("required")),
        "preflight_verified_zero": bool(preflight.get("verified_zero")),
        "final_verified_zero": bool(final.get("verified_zero")),
        "state_file_removed": state_file_removed,
        "poll_attempts": int(preflight.get("poll_attempts", 0))
        + int(final.get("poll_attempts", 0)),
        "successful_polls": int(preflight.get("successful_polls", 0))
        + int(final.get("successful_polls", 0)),
        "final_zero_scans": int(final.get("final_zero_scans", 0)),
        **{
            f"{field_name}_max": max(
                int(preflight.get(f"{field_name}_max", 0)),
                int(final.get(f"{field_name}_max", 0)),
            )
            for field_name in _LIFECYCLE_COUNT_FIELDS
        },
        "failure_generation_changed": bool(
            preflight.get("failure_generation_changed")
            or final.get("failure_generation_changed")
        ),
        "quiet_window_observed": bool(final.get("quiet_window_observed")),
        "error_count": int(preflight.get("error_count", 0))
        + int(final.get("error_count", 0)),
        "error_sha256": error_hashes,
    }


def _prepare_session_ownership(
    args: argparse.Namespace,
    *,
    monotonic: Callable[[], float],
    sleep: Callable[[float], None],
) -> _SessionOwnershipLease:
    """Lock the host, clean stale ownership, then persist a new unique owner."""

    current_endpoint = _canonical_flaresolverr_endpoint(args.flaresolverr_url)
    if current_endpoint != REQUIRED_FLARESOLVERR_ENDPOINT:
        raise ValueError("FlareSolverr endpoint does not match production binding")
    lock_path, state_path = _state_paths(args)
    no_cleanup = _empty_cleanup_result(required=False, verified=True)
    try:
        lock_descriptor = _acquire_supervisor_lock(lock_path)
    except BlockingIOError as exc:
        evidence = _ownership_evidence(
            lock_acquired=False,
            preflight=no_cleanup,
            final=_empty_cleanup_result(required=True, verified=False),
            state_file_removed=False,
        )
        raise _SessionOwnershipPreparationError(
            "another capacity supervisor is already running", evidence
        ) from exc
    except Exception as exc:
        failed = _empty_cleanup_result(required=True, verified=False)
        failed["error_count"] = 1
        failed["error_sha256"] = [_cleanup_error_hash("lock", exc)]
        evidence = _ownership_evidence(
            lock_acquired=False,
            preflight=no_cleanup,
            final=failed,
            state_file_removed=False,
        )
        raise _SessionOwnershipPreparationError(
            "capacity supervisor lock is unavailable", evidence
        ) from exc

    preflight = no_cleanup
    try:
        if state_path.exists():
            try:
                stale_owner, stale_endpoint = _read_owner_state(state_path)
            except Exception as exc:
                preflight = _empty_cleanup_result(required=True, verified=False)
                preflight["error_count"] = 1
                preflight["error_sha256"] = [
                    _cleanup_error_hash("state_read", exc)
                ]
            else:
                preflight = _sweep_owned_browser_sessions(
                    flaresolverr_url=stale_endpoint,
                    owner=stale_owner,
                    stale=True,
                    monotonic=monotonic,
                    sleep=sleep,
                )
                if preflight["verified_zero"]:
                    try:
                        removed = _remove_owner_state(
                            state_path, stale_owner, stale_endpoint
                        )
                    except Exception as exc:
                        removed = False
                        preflight["error_count"] += 1
                        preflight["error_sha256"] = sorted(
                            {
                                *preflight["error_sha256"],
                                _cleanup_error_hash("state_remove", exc),
                            }
                        )
                    if not removed:
                        preflight["verified_zero"] = False

            if not preflight["verified_zero"]:
                evidence = _ownership_evidence(
                    lock_acquired=True,
                    preflight=preflight,
                    final=_empty_cleanup_result(required=True, verified=False),
                    state_file_removed=False,
                )
                raise _SessionOwnershipPreparationError(
                    "stale browser-session cleanup could not be verified",
                    evidence,
                )

        owner = secrets.token_hex(12)
        _validate_session_owner(owner)
        _write_owner_state(state_path, owner, current_endpoint)
        fresh_probe = _probe_fresh_session_owner(
            flaresolverr_url=current_endpoint,
            owner=owner,
        )
        preflight = _combine_cleanup_results(preflight, fresh_probe)
        if not fresh_probe["verified_zero"]:
            evidence = _ownership_evidence(
                lock_acquired=True,
                preflight=preflight,
                final=_empty_cleanup_result(required=True, verified=False),
                state_file_removed=False,
            )
            raise _SessionOwnershipPreparationError(
                "fresh browser-session lifecycle probe failed", evidence
            )
    except _SessionOwnershipPreparationError:
        os.close(lock_descriptor)
        raise
    except Exception as exc:
        failed = _empty_cleanup_result(required=True, verified=False)
        failed["error_count"] = 1
        failed["error_sha256"] = [_cleanup_error_hash("state_write", exc)]
        evidence = _ownership_evidence(
            lock_acquired=True,
            preflight=preflight,
            final=failed,
            state_file_removed=False,
        )
        os.close(lock_descriptor)
        raise _SessionOwnershipPreparationError(
            "browser-session ownership could not be persisted", evidence
        ) from exc

    finalized: Optional[dict[str, Any]] = None

    def finalize() -> Mapping[str, Any]:
        nonlocal finalized
        if finalized is not None:
            return finalized
        final = _sweep_owned_browser_sessions(
            flaresolverr_url=current_endpoint,
            owner=owner,
            stale=False,
            monotonic=monotonic,
            sleep=sleep,
        )
        removed = False
        if final["verified_zero"]:
            try:
                removed = _remove_owner_state(
                    state_path, owner, current_endpoint
                )
            except Exception as exc:
                final["error_count"] += 1
                final["error_sha256"] = sorted(
                    {
                        *final["error_sha256"],
                        _cleanup_error_hash("state_remove", exc),
                    }
                )
        finalized = _ownership_evidence(
            lock_acquired=True,
            preflight=preflight,
            final=final,
            state_file_removed=removed,
        )
        return finalized

    closed = False

    def close() -> None:
        nonlocal closed
        if not closed:
            os.close(lock_descriptor)
            closed = True

    return _SessionOwnershipLease(
        owner=owner,
        preflight=preflight,
        finalize_callback=finalize,
        close_callback=close,
    )


def _prepare_noop_session_ownership(
    args: argparse.Namespace,
    *,
    monotonic: Callable[[], float],
    sleep: Callable[[float], None],
) -> _SessionOwnershipLease:
    """In-memory ownership seam for existing deterministic supervisor tests."""

    del args, monotonic, sleep
    owner = secrets.token_hex(12)
    preflight = _empty_cleanup_result(required=False, verified=True)
    evidence = _ownership_evidence(
        lock_acquired=True,
        preflight=preflight,
        final=_empty_cleanup_result(required=True, verified=True),
        state_file_removed=True,
    )
    return _SessionOwnershipLease(
        owner=owner,
        preflight=preflight,
        finalize_callback=lambda: evidence,
        close_callback=lambda: None,
    )


def _workflow_runtime_preflight() -> Optional[str]:
    """Verify that the host interpreter matches the production HTTP runtime."""

    worker_exec_error = _worker_exec_preflight()
    if worker_exec_error is not None:
        return worker_exec_error
    try:
        importlib.import_module("curl_cffi")
        importlib.import_module("curl_cffi.requests")
    except Exception:
        return (
            "host Python dependency unavailable: "
            f"curl_cffi=={REQUIRED_CURL_CFFI_VERSION} is required"
        )
    installed_version = _installed_curl_cffi_version()
    if installed_version != REQUIRED_CURL_CFFI_VERSION:
        return (
            "host Python dependency version mismatch: expected "
            f"curl_cffi=={REQUIRED_CURL_CFFI_VERSION}, found "
            f"{installed_version or 'unknown'}"
        )
    return None


def _installed_curl_cffi_version() -> str:
    """Return the normalized installed distribution version, or ``missing``."""

    try:
        return importlib.metadata.version("curl_cffi")
    except importlib.metadata.PackageNotFoundError:
        return "missing"


def _workflow_failure_label(child_error: str) -> str:
    """Surface allowlisted operational failures without copying parser payloads."""

    if "curl_cffi is required for WhoScoredTransport" in child_error:
        return "RuntimeError: curl_cffi is required for WhoScoredTransport"
    return "workflow reported failure"


def _as_nonnegative_int(value: Any, *, field_name: str) -> int:
    if isinstance(value, bool):
        raise ValueError(f"{field_name} must be a non-negative integer")
    try:
        result = int(value or 0)
    except (OverflowError, TypeError, ValueError):
        raise ValueError(
            f"{field_name} must be a non-negative integer"
        ) from None
    if result < 0:
        raise ValueError(f"{field_name} must be a non-negative integer")
    return result


def _as_nonnegative_float(value: Any, *, field_name: str) -> float:
    if type(value) not in (int, float):
        raise ValueError(f"{field_name} must be finite and non-negative")
    try:
        result = float(value)
    except OverflowError:
        raise ValueError(f"{field_name} must be finite and non-negative") from None
    if not math.isfinite(result) or result < 0:
        raise ValueError(f"{field_name} must be finite and non-negative")
    return result


def _as_child_nonnegative_int(value: Any, *, field_name: str) -> int:
    """Accept only exact JSON integers from an untrusted workflow report."""

    if type(value) is not int or value < 0:
        raise ValueError(f"{field_name} must be a non-negative integer")
    return value


def _evidence_bool(value: Any) -> Optional[bool]:
    """Project an untrusted child value without retaining its representation."""

    return value if type(value) is bool else None


def _workflow_traffic(report: Mapping[str, Any]) -> tuple[int, int, int, int]:
    """Return attempts, completed logical units, and paid traffic counters."""

    source_request_attempts = 0
    page_units = 0
    paid_bytes = 0
    paid_route_requests = 0
    phases = report.get("phases")
    if not isinstance(phases, list):
        raise ValueError("workflow phases must be a list")
    for phase in phases:
        if not isinstance(phase, Mapping):
            raise ValueError("workflow phase must be an object")
        traffic = phase.get("traffic") or {}
        if not isinstance(traffic, Mapping):
            raise ValueError("workflow phase traffic must be an object")
        attempts = _as_child_nonnegative_int(
            traffic.get("source_request_attempts", 0),
            field_name="source_request_attempts",
        )
        if "successful_page_units" not in traffic:
            raise ValueError("workflow omitted successful_page_units")
        completed = _as_child_nonnegative_int(
            traffic.get("successful_page_units"),
            field_name="successful_page_units",
        )
        if completed > attempts:
            raise ValueError("successful_page_units cannot exceed source attempts")
        source_request_attempts += attempts
        page_units += completed
        paid_bytes += _as_child_nonnegative_int(
            traffic.get("paid_proxy_bytes", 0), field_name="paid_proxy_bytes"
        )
        paid_route_requests += _as_child_nonnegative_int(
            traffic.get("paid_route_requests", 0),
            field_name="paid_route_requests",
        )
    return source_request_attempts, page_units, paid_bytes, paid_route_requests


def _workflow_shape(report: Mapping[str, Any]) -> tuple[list[str], int]:
    """Prove the canary covered every entity and a measurable source scope."""

    stage_contract = report.get("stage_statistics_contract") or {}
    if not isinstance(stage_contract, Mapping):
        raise ValueError("workflow did not prove the 68-feed stage contract")
    expected_feed_states = _as_child_nonnegative_int(
        stage_contract.get("expected_feed_states_per_stage", 0),
        field_name="expected_feed_states_per_stage",
    )
    if expected_feed_states != 68:
        raise ValueError("workflow did not prove the 68-feed stage contract")
    phases = report.get("phases") or []
    cold = next(
        (
            phase
            for phase in phases
            if isinstance(phase, Mapping) and phase.get("name") == "cold"
        ),
        None,
    )
    if not isinstance(cold, Mapping):
        raise ValueError("workflow did not emit a cold phase")
    results = cold.get("results") or []
    if not isinstance(results, list):
        raise ValueError("cold phase results must be a list")
    by_entity = {
        str(result.get("entity")): result
        for result in results
        if isinstance(result, Mapping) and result.get("entity")
    }
    required = {"schedule", "matches", "previews", "profiles"}
    missing = sorted(required.difference(by_entity))
    if missing:
        raise ValueError("workflow omitted entity results: " + ", ".join(missing))
    if not cold.get("selected_match_ids") or not cold.get("selected_profile_ids"):
        raise ValueError("workflow did not exercise match and profile targets")
    schedule_metadata = by_entity["schedule"].get("metadata") or {}
    if not isinstance(schedule_metadata, Mapping):
        raise ValueError("schedule result metadata must be an object")
    stage_count = _as_child_nonnegative_int(
        schedule_metadata.get("source_stage_count", 0),
        field_name="source_stage_count",
    )
    if stage_count == 0:
        raise ValueError("workflow scope has no source stages")
    return sorted(required), stage_count


def _summarize_outcome(outcome: WorkerOutcome) -> dict[str, Any]:
    """Reduce a child report to non-sensitive capacity evidence."""

    summary: dict[str, Any] = {
        "worker_id": outcome.worker_id,
        "iteration": outcome.iteration,
        "scope": outcome.scope,
        "returncode": outcome.returncode,
        "process_elapsed_seconds": round(outcome.elapsed_seconds, 3),
        "stderr_bytes": outcome.stderr_bytes,
        "stderr_sha256": outcome.stderr_sha256,
        "termination_reason": outcome.termination_reason,
        "status": "failed",
        "source_request_attempts": 0,
        "page_units": 0,
        "paid_bytes": 0,
        "paid_route_requests": 0,
        "traffic_evidence_valid": None,
        "workflow_elapsed_seconds": 0.0,
        "publishes": None,
        "writes_bronze": None,
        "executes_ddl": None,
        "entities": [],
        "source_stage_count": 0,
    }
    if outcome.launch_error:
        summary["error"] = _safe_message(outcome.launch_error)
        return summary
    if outcome.termination_reason:
        summary["status"] = outcome.termination_reason
        return summary
    if not isinstance(outcome.report, Mapping):
        summary["error"] = "workflow did not emit one JSON object"
        return summary

    report = outcome.report
    summary.update(
        {
            "publishes": _evidence_bool(report.get("publishes")),
            "writes_bronze": _evidence_bool(report.get("writes_bronze")),
            "executes_ddl": _evidence_bool(report.get("executes_ddl")),
            "traffic_evidence_valid": False,
        }
    )
    try:
        if report.get("benchmark_version") != EXPECTED_WORKFLOW_VERSION:
            raise ValueError("workflow benchmark version mismatch")
        attempts, page_units, paid_bytes, paid_requests = _workflow_traffic(report)
    except (TypeError, ValueError) as exc:
        summary["error"] = _safe_message(exc)
        return summary
    summary.update(
        {
            "source_request_attempts": attempts,
            "page_units": page_units,
            "paid_bytes": paid_bytes,
            "paid_route_requests": paid_requests,
            "traffic_evidence_valid": True,
        }
    )
    try:
        workflow_elapsed = _as_nonnegative_float(
            report.get("elapsed_seconds", 0.0), field_name="workflow elapsed_seconds"
        )
    except (TypeError, ValueError) as exc:
        summary["error"] = _safe_message(exc)
        return summary

    summary.update(
        {
            "workflow_elapsed_seconds": round(workflow_elapsed, 3),
        }
    )
    safety_flags = {
        "publishes": summary["publishes"],
        "writes_bronze": summary["writes_bronze"],
        "executes_ddl": summary["executes_ddl"],
    }
    unsafe = [name for name, value in safety_flags.items() if value is not False]
    if unsafe:
        summary["error"] = "workflow did not prove disabled sinks: " + ", ".join(
            unsafe
        )
        return summary
    if outcome.returncode != 0 or report.get("status") != "success":
        child_error = str(
            report.get("error") or f"workflow exited {outcome.returncode}"
        )
        summary["error"] = _workflow_failure_label(child_error)
        summary["workflow_error_sha256"] = hashlib.sha256(
            child_error.encode("utf-8", errors="replace")
        ).hexdigest()
        return summary

    if workflow_elapsed <= 0:
        summary["error"] = "workflow elapsed_seconds must be positive"
        return summary

    try:
        entities, stage_count = _workflow_shape(report)
    except (TypeError, ValueError) as exc:
        summary["error"] = _safe_message(exc)
        return summary
    summary["entities"] = entities
    summary["source_stage_count"] = stage_count

    summary["status"] = "success"
    return summary


def _accept_outcome(accumulator: CapacityAccumulator, outcome: WorkerOutcome) -> None:
    summary = _summarize_outcome(outcome)
    accumulator.run_summaries.append(summary)
    accumulator.paid_bytes += int(summary["paid_bytes"])
    accumulator.paid_route_requests += int(summary["paid_route_requests"])
    accumulator.source_request_attempts += int(summary["source_request_attempts"])

    if summary["status"] == "deadline_terminated":
        accumulator.deadline_truncations += 1
        return
    if summary["status"] == "aborted_by_gate":
        return
    if summary["traffic_evidence_valid"] is not True:
        accumulator.traffic_evidence_violations.append(
            f"worker {outcome.worker_id} iteration {outcome.iteration} did not "
            "prove paid traffic counters"
        )
    if summary["publishes"] is not False or any(
        summary[name] is not False for name in ("writes_bronze", "executes_ddl")
    ):
        accumulator.safety_violations.append(
            f"worker {outcome.worker_id} iteration {outcome.iteration} did not "
            "prove non-publishing execution"
        )
    if summary["status"] != "success":
        accumulator.worker_errors.append(
            f"worker {outcome.worker_id} iteration {outcome.iteration}: "
            f"{summary.get('error', summary['status'])}"
        )
        return

    accumulator.page_units += int(summary["page_units"])
    accumulator.worker_seconds += float(summary["workflow_elapsed_seconds"])
    accumulator.completed_runs += 1
    accumulator.completed_by_worker[outcome.worker_id] += 1
    accumulator.max_source_stage_count = max(
        accumulator.max_source_stage_count,
        int(summary["source_stage_count"]),
    )


def _projected_page_units_per_day(page_units: int, elapsed_seconds: float) -> float:
    if elapsed_seconds <= 0:
        return 0.0
    return float(page_units) * 86_400.0 / elapsed_seconds


def _normalise_container(
    raw: Mapping[str, Any],
    *,
    expected_flaresolverr_config_hash: Optional[str] = None,
    expected_flaresolverr_image_id: Optional[str] = None,
) -> dict[str, Any]:
    state = raw.get("State") or {}
    if not isinstance(state, Mapping):
        raise RuntimeError("docker inspect returned invalid State")
    name = str(raw.get("Name") or "").lstrip("/")
    container_id = str(raw.get("Id") or "")
    if not name or not container_id:
        raise RuntimeError("docker inspect omitted container identity")
    command_contract_ok = False
    image_identity_contract_ok = False
    immutable_payload_contract_ok = False
    security_contract_ok = False
    compose_identity_ok = False
    published_endpoint_contract_ok = False
    if name == "flaresolverr":
        config = raw.get("Config") or {}
        host_config = raw.get("HostConfig") or {}
        mounts = raw.get("Mounts") or []
        network_settings = raw.get("NetworkSettings") or {}
        allowed_compose_file_labels = {
            str(PRODUCTION_COMPOSE_FILES[0].resolve()),
            ",".join(str(path.resolve()) for path in PRODUCTION_COMPOSE_FILES),
        }
        if isinstance(config, Mapping):
            command = config.get("Cmd")
            entrypoint = config.get("Entrypoint")
            command_contract_ok = (
                type(command) is list
                and tuple(command) == REQUIRED_FLARESOLVERR_COMMAND
                and type(entrypoint) is list
                and tuple(entrypoint) == REQUIRED_FLARESOLVERR_ENTRYPOINT
            )
            labels = config.get("Labels") or {}
            compose_identity_ok = isinstance(labels, Mapping) and (
                expected_flaresolverr_config_hash is not None
                and labels.get("com.docker.compose.project")
                == REQUIRED_COMPOSE_PROJECT
                and labels.get("com.docker.compose.service") == "flaresolverr"
                and labels.get("com.docker.compose.config-hash")
                == expected_flaresolverr_config_hash
                and labels.get("com.docker.compose.oneoff") == "False"
                and labels.get("com.docker.compose.project.config_files")
                in allowed_compose_file_labels
                and labels.get("com.docker.compose.project.environment_file")
                == ",".join(str(path) for path in PRODUCTION_COMPOSE_ENV_FILES)
                and labels.get("com.docker.compose.project.working_dir")
                == str(REPO_ROOT.resolve())
            )
            image_identity_contract_ok = (
                isinstance(labels, Mapping)
                and config.get("Image")
                == REQUIRED_FLARESOLVERR_IMAGE_REFERENCE
                and type(expected_flaresolverr_image_id) is str
                and re.fullmatch(
                    r"sha256:[0-9a-f]{64}", expected_flaresolverr_image_id
                )
                is not None
                and raw.get("Image") == expected_flaresolverr_image_id
                and labels.get("com.docker.compose.image")
                == expected_flaresolverr_image_id
            )
        if isinstance(host_config, Mapping) and isinstance(config, Mapping):
            immutable_payload_contract_ok = (
                host_config.get("ReadonlyRootfs") is True
                and host_config.get("Tmpfs") == REQUIRED_FLARESOLVERR_TMPFS
                and isinstance(mounts, list)
                and not mounts
            )
            security_contract_ok = (
                config.get("User") == "1000:1000"
                and host_config.get("Privileged") is False
                and host_config.get("CapDrop") == ["ALL"]
                and host_config.get("CapAdd") in (None, [])
                and host_config.get("SecurityOpt")
                == [
                    "no-new-privileges:true",
                    "apparmor=docker-default",
                    "seccomp=builtin",
                ]
                and raw.get("AppArmorProfile") == "docker-default"
            )
        if isinstance(network_settings, Mapping):
            ports = network_settings.get("Ports") or {}
            bindings = (
                ports.get("8191/tcp") if isinstance(ports, Mapping) else None
            )
            published_endpoint_contract_ok = (
                isinstance(bindings, list)
                and len(bindings) == 1
                and isinstance(bindings[0], Mapping)
                and set(bindings[0]) == {"HostIp", "HostPort"}
                and bindings[0].get("HostIp") == "127.0.0.1"
                and bindings[0].get("HostPort") == "8191"
            )
    return {
        "name": name,
        "id": container_id[:12],
        "image_id": str(raw.get("Image") or ""),
        "command_contract_ok": command_contract_ok,
        "image_identity_contract_ok": image_identity_contract_ok,
        "immutable_payload_contract_ok": immutable_payload_contract_ok,
        "security_contract_ok": security_contract_ok,
        "compose_identity_ok": compose_identity_ok,
        "published_endpoint_contract_ok": published_endpoint_contract_ok,
        "status": str(state.get("Status") or "unknown"),
        "running": bool(state.get("Running", False)),
        "oom_killed": bool(state.get("OOMKilled", False)),
        "restart_count": _as_nonnegative_int(
            raw.get("RestartCount", 0), field_name="RestartCount"
        ),
        "pid": _as_nonnegative_int(state.get("Pid", 0), field_name="State.Pid"),
    }


def _parse_memory_size(value: Any) -> int:
    token = str(value or "").strip()
    match = _MEMORY_SIZE_RE.fullmatch(token)
    if match is None:
        raise RuntimeError("docker stats returned an invalid memory size")
    amount = float(match.group(1))
    return int(amount * _MEMORY_SIZE_MULTIPLIERS[match.group(2)])


def _inspect_container_resources(
    names: Sequence[str],
) -> Mapping[str, Mapping[str, int]]:
    try:
        result = subprocess.run(
            ["docker", "stats", "--no-stream", "--format", "{{json .}}", *names],
            check=False,
            capture_output=True,
            text=True,
            timeout=15,
        )
    except (OSError, subprocess.SubprocessError) as exc:
        raise RuntimeError(f"docker stats failed: {_safe_message(exc)}") from exc
    if result.returncode != 0:
        raise RuntimeError(
            "docker stats failed for required containers: "
            + ", ".join(str(name) for name in names)
        )
    resources: dict[str, dict[str, int]] = {}
    for line in result.stdout.splitlines():
        if not line.strip():
            continue
        try:
            document = json.loads(line)
        except json.JSONDecodeError as exc:
            raise RuntimeError("docker stats did not return JSON lines") from exc
        if not isinstance(document, Mapping):
            raise RuntimeError("docker stats row must be an object")
        name = str(document.get("Name") or document.get("Container") or "")
        usage_token, separator, limit_token = str(
            document.get("MemUsage") or ""
        ).partition("/")
        if not name or not separator:
            raise RuntimeError("docker stats omitted container memory evidence")
        resources[name] = {
            "memory_usage_bytes": _parse_memory_size(usage_token),
            "memory_limit_bytes": _parse_memory_size(limit_token),
            "process_count": _as_nonnegative_int(
                document.get("PIDs", 0), field_name="docker stats PIDs"
            ),
        }
    missing = sorted(set(names).difference(resources))
    if missing:
        raise RuntimeError("docker stats omitted containers: " + ", ".join(missing))
    return {name: resources[name] for name in names}


def _resolved_flaresolverr_compose_hash() -> str:
    """Hash the currently resolved production service without retaining values."""

    for environment_file in PRODUCTION_COMPOSE_ENV_FILES:
        try:
            metadata = environment_file.stat()
        except OSError as exc:
            raise RuntimeError(
                "production compose environment is unavailable"
            ) from exc
        if (
            not stat.S_ISREG(metadata.st_mode)
            or stat.S_IMODE(metadata.st_mode) != 0o600
        ):
            raise RuntimeError("production compose environment metadata is invalid")
    environment_arguments = [
        argument
        for environment_file in PRODUCTION_COMPOSE_ENV_FILES
        for argument in ("--env-file", str(environment_file))
    ]
    try:
        result = subprocess.run(
            [
                str(PRODUCTION_COMPOSE_WRAPPER),
                *environment_arguments,
                "-p",
                REQUIRED_COMPOSE_PROJECT,
                "config",
                "--hash",
                "flaresolverr",
            ],
            cwd=REPO_ROOT,
            check=False,
            capture_output=True,
            text=True,
            timeout=15,
        )
    except (OSError, subprocess.SubprocessError) as exc:
        raise RuntimeError("cannot resolve production FlareSolverr config") from exc
    fields = result.stdout.strip().split()
    if (
        result.returncode != 0
        or len(fields) != 2
        or fields[0] != "flaresolverr"
        or re.fullmatch(r"[0-9a-f]{64}", fields[1]) is None
    ):
        raise RuntimeError("production FlareSolverr config hash is unavailable")
    return fields[1]


def _resolved_flaresolverr_image_id() -> str:
    """Resolve the exact local ID behind the reviewed derived-image reference."""

    try:
        result = subprocess.run(
            [
                "docker",
                "image",
                "inspect",
                "--format",
                "{{.Id}}",
                REQUIRED_FLARESOLVERR_IMAGE_REFERENCE,
            ],
            check=False,
            capture_output=True,
            text=True,
            timeout=15,
        )
    except (OSError, subprocess.SubprocessError) as exc:
        raise RuntimeError("cannot resolve production FlareSolverr image") from exc
    image_id = result.stdout.strip()
    if (
        result.returncode != 0
        or re.fullmatch(r"sha256:[0-9a-f]{64}", image_id) is None
    ):
        raise RuntimeError("production FlareSolverr image ID is unavailable")
    return image_id


def _inspect_containers(names: Sequence[str]) -> Mapping[str, Mapping[str, Any]]:
    if not names:
        raise RuntimeError("at least one container is required for restart/OOM evidence")
    try:
        result = subprocess.run(
            ["docker", "inspect", *names],
            check=False,
            capture_output=True,
            text=True,
            timeout=15,
        )
    except (OSError, subprocess.SubprocessError) as exc:
        raise RuntimeError(f"docker inspect failed: {_safe_message(exc)}") from exc
    if result.returncode != 0:
        raise RuntimeError(
            "docker inspect failed for required containers: "
            + ", ".join(str(name) for name in names)
        )
    try:
        payload = json.loads(result.stdout)
    except json.JSONDecodeError as exc:
        raise RuntimeError("docker inspect did not return JSON") from exc
    if not isinstance(payload, list):
        raise RuntimeError("docker inspect JSON must be a list")
    expected_flaresolverr_config_hash = (
        _resolved_flaresolverr_compose_hash()
        if "flaresolverr" in names
        else None
    )
    expected_flaresolverr_image_id = (
        _resolved_flaresolverr_image_id() if "flaresolverr" in names else None
    )
    inspected = {
        item["name"]: item
        for item in (
            _normalise_container(
                raw,
                expected_flaresolverr_config_hash=(
                    expected_flaresolverr_config_hash
                ),
                expected_flaresolverr_image_id=expected_flaresolverr_image_id,
            )
            for raw in payload
        )
    }
    missing = sorted(set(names).difference(inspected))
    if missing:
        raise RuntimeError("docker inspect omitted containers: " + ", ".join(missing))
    resources = _inspect_container_resources(names)
    return {
        name: {**inspected[name], **resources[name]}
        for name in names
    }


def _read_process_status(pid: int) -> Optional[tuple[int, int]]:
    """Return ``(ppid, rss_bytes)`` from procfs, tolerating process exit races."""

    try:
        content = Path(f"/proc/{pid}/status").read_text(encoding="utf-8")
    except (FileNotFoundError, ProcessLookupError, PermissionError, OSError):
        return None
    ppid: Optional[int] = None
    rss_kib: Optional[int] = None
    for line in content.splitlines():
        if line.startswith("PPid:"):
            ppid = int(line.split()[1])
        elif line.startswith("VmRSS:"):
            rss_kib = int(line.split()[1])
    if ppid is None:
        return None
    return ppid, (rss_kib or 0) * 1024


def _sample_process_rss(root_pids: Sequence[int]) -> Mapping[str, Any]:
    """Sum unique proc trees for the harness and monitored container roots."""

    process_table: dict[int, tuple[int, int]] = {}
    try:
        proc_entries = list(Path("/proc").iterdir())
    except OSError as exc:
        raise RuntimeError("cannot enumerate procfs for RSS evidence") from exc
    for entry in proc_entries:
        if not entry.name.isdigit():
            continue
        pid = int(entry.name)
        status = _read_process_status(pid)
        if status is not None:
            process_table[pid] = status

    children: dict[int, list[int]] = {}
    for pid, (ppid, _rss) in process_table.items():
        children.setdefault(ppid, []).append(pid)
    roots = sorted({int(pid) for pid in root_pids if int(pid) > 0})
    missing_roots = sorted(set(roots).difference(process_table))
    if missing_roots:
        raise RuntimeError("required RSS roots disappeared during sampling")
    included: set[int] = set()
    pending = list(roots)
    while pending:
        pid = pending.pop()
        if pid in included:
            continue
        included.add(pid)
        pending.extend(children.get(pid, ()))

    present = included.intersection(process_table)
    if os.getpid() not in present:
        raise RuntimeError("harness process is missing from RSS sample")
    return {
        "root_pids": roots,
        "process_count": len(present),
        "rss_bytes": sum(process_table[pid][1] for pid in present),
    }


def _container_gate_violations(
    baseline: Mapping[str, Mapping[str, Any]],
    current: Mapping[str, Mapping[str, Any]],
) -> list[str]:
    violations: list[str] = []
    for name, initial in baseline.items():
        observed = current.get(name)
        if observed is None:
            violations.append(f"{name}: missing from container sample")
            continue
        if observed.get("id") != initial.get("id"):
            violations.append(f"{name}: container was recreated")
        if observed.get("restart_count") != initial.get("restart_count"):
            violations.append(
                f"{name}: restart count changed from "
                f"{initial.get('restart_count')} to {observed.get('restart_count')}"
            )
        if observed.get("oom_killed") is True:
            violations.append(f"{name}: OOMKilled is true")
        if observed.get("running") is not True or observed.get("status") != "running":
            violations.append(f"{name}: container is not running")
    return sorted(set(violations))


def _parse_child_report(stdout: str) -> Optional[Mapping[str, Any]]:
    payload = stdout.strip()
    if not payload:
        return None
    try:
        parsed = json.loads(payload)
    except json.JSONDecodeError:
        return None
    return parsed if isinstance(parsed, Mapping) else None


def _finish_process(
    running: _RunningProcess,
    *,
    monotonic: Callable[[], float],
    termination_reason: Optional[str] = None,
) -> WorkerOutcome:
    process = running.process
    returncode = process.poll()
    if returncode is None:
        raise _WorkerTerminationUnverified(
            "tracked capacity worker is still alive"
        )
    running.stdout_handle.flush()
    running.stderr_handle.flush()
    running.stdout_handle.seek(0)
    running.stderr_handle.seek(0)
    stdout = running.stdout_handle.read()
    stderr = running.stderr_handle.read()
    stderr_encoded = stderr.encode("utf-8", errors="replace")
    return WorkerOutcome(
        worker_id=running.command.worker_id,
        iteration=running.command.iteration,
        scope=running.command.scope,
        returncode=returncode,
        report=_parse_child_report(stdout),
        elapsed_seconds=max(0.0, monotonic() - running.started_at),
        stderr_bytes=len(stderr_encoded),
        stderr_sha256=hashlib.sha256(stderr_encoded).hexdigest(),
        termination_reason=termination_reason,
    )


def _signal_process(process: subprocess.Popen[str], sig: int) -> None:
    try:
        os.killpg(process.pid, sig)
        return
    except (ProcessLookupError, PermissionError, OSError):
        if process.poll() is not None:
            return
        try:
            process.send_signal(sig)
        except (ProcessLookupError, OSError):
            pass


def _process_group_has_live_members(process_group_id: int) -> bool:
    """Fail closed unless procfs proves this worker group has no live member."""

    try:
        entries = list(Path("/proc").iterdir())
    except OSError:
        return True
    uncertain = False
    for entry in entries:
        if not entry.name.isdigit():
            continue
        try:
            content = (entry / "stat").read_text(encoding="utf-8")
        except (FileNotFoundError, ProcessLookupError):
            continue
        except (PermissionError, OSError):
            uncertain = True
            continue
        closing_parenthesis = content.rfind(")")
        fields = content[closing_parenthesis + 1 :].split()
        if closing_parenthesis < 0 or len(fields) < 3:
            uncertain = True
            continue
        try:
            member_group_id = int(fields[2])
        except ValueError:
            uncertain = True
            continue
        if member_group_id == process_group_id and fields[0] != "Z":
            return True
    return uncertain


def _tracked_worker_group_alive(running: _RunningProcess) -> bool:
    leader_alive = running.process.poll() is None
    return leader_alive or _process_group_has_live_members(running.process.pid)


def _stop_processes(
    running: Sequence[_RunningProcess],
    *,
    monotonic: Callable[[], float],
    sleep: Callable[[float], None],
) -> None:
    active = [item for item in running if _tracked_worker_group_alive(item)]
    for item in active:
        _signal_process(item.process, signal.SIGTERM)
    grace_deadline = monotonic() + _TERMINATE_GRACE_SECONDS
    while active and monotonic() < grace_deadline:
        active = [item for item in active if _tracked_worker_group_alive(item)]
        if active:
            sleep(min(_POLL_SECONDS, max(0.0, grace_deadline - monotonic())))
    for item in active:
        _signal_process(item.process, signal.SIGKILL)
    kill_deadline = monotonic() + _KILL_CONFIRM_SECONDS
    while active and monotonic() < kill_deadline:
        active = [item for item in active if _tracked_worker_group_alive(item)]
        if active:
            sleep(min(_POLL_SECONDS, max(0.0, kill_deadline - monotonic())))
    active = [item for item in active if _tracked_worker_group_alive(item)]
    if active:
        raise _WorkerTerminationUnverified(
            "tracked capacity worker survived bounded SIGKILL confirmation"
        )


def _worker_exec_argv(argv: Sequence[str]) -> tuple[str, ...]:
    if not argv or any(type(value) is not str or not value for value in argv):
        raise ValueError("invalid capacity worker argv")
    return (
        sys.executable,
        str(WORKER_EXEC_SCRIPT),
        "--expected-parent-pid",
        str(os.getpid()),
        "--",
        str(WORKER_NAMESPACE_EXECUTABLE),
        "--pid",
        "--fork",
        "--kill-child=SIGKILL",
        "--",
        *argv,
    )


def _capacity_control_pipe(
    *, owner: str, flaresolverr_endpoint: str
) -> int:
    """Return a read FD containing one bounded, versioned private control."""

    owner = _validate_session_owner(owner)
    endpoint = _canonical_flaresolverr_endpoint(flaresolverr_endpoint)
    if endpoint != REQUIRED_FLARESOLVERR_ENDPOINT:
        raise ValueError("FlareSolverr endpoint does not match production binding")
    payload = json.dumps(
        {
            "schema_version": _CAPACITY_CONTROL_SCHEMA_VERSION,
            "owner": owner,
            "flaresolverr_endpoint": endpoint,
        },
        sort_keys=True,
        separators=(",", ":"),
    ).encode("utf-8")
    if not payload or len(payload) >= _CAPACITY_CONTROL_PAYLOAD_LIMIT:
        raise ValueError("capacity control payload is too large")
    read_fd, write_fd = os.pipe()
    try:
        view = memoryview(payload)
        while view:
            written = os.write(write_fd, view)
            if written <= 0:  # pragma: no cover - defensive pipe contract
                raise OSError("capacity control pipe write failed")
            view = view[written:]
    except BaseException:
        os.close(read_fd)
        raise
    finally:
        os.close(write_fd)
    return read_fd


def _run_subprocess_round(
    commands: Sequence[WorkerCommand],
    *,
    deadline: float,
    on_sample: Callable[[bool], None],
    on_outcome: Callable[[WorkerOutcome], None],
    should_stop: Callable[[], bool],
    before_launch: Callable[[], None],
    monotonic: Callable[[], float],
    sleep: Callable[[float], None],
) -> None:
    """Keep exactly four isolated workflow slots busy and supervise fail-fast."""

    if len(commands) != WORKER_COUNT:
        raise ValueError(f"capacity round requires exactly {WORKER_COUNT} workers")
    worker_exec_error = _worker_exec_preflight()
    if worker_exec_error is not None:
        raise RuntimeError(worker_exec_error)
    running: list[_RunningProcess] = []
    with TemporaryDirectory(prefix="whoscored-capacity-round-") as temp_root:
        def launch(command: WorkerCommand) -> bool:
            if should_stop():
                return False
            before_launch()
            if should_stop():
                return False
            stem = f"worker-{command.worker_id}-iteration-{command.iteration}"
            stdout_handle = open(
                Path(temp_root) / f"{stem}.stdout", "w+", encoding="utf-8"
            )
            stderr_handle = open(
                Path(temp_root) / f"{stem}.stderr", "w+", encoding="utf-8"
            )
            control_fd: Optional[int] = None
            try:
                worker_argv = command.argv
                if (
                    command.browser_session_owner is not None
                    or command.flaresolverr_endpoint is not None
                ):
                    if (
                        command.browser_session_owner is None
                        or command.flaresolverr_endpoint is None
                    ):
                        raise ValueError("incomplete capacity worker control")
                    control_fd = _capacity_control_pipe(
                        owner=command.browser_session_owner,
                        flaresolverr_endpoint=command.flaresolverr_endpoint,
                    )
                    worker_argv = (
                        *worker_argv,
                        "--capacity-control-fd",
                        str(control_fd),
                    )
                process = subprocess.Popen(
                    _worker_exec_argv(worker_argv),
                    cwd=REPO_ROOT,
                    env={
                        **os.environ,
                        "PYTHONUNBUFFERED": "1",
                        "WHOSCORED_SOURCE_CIRCUIT_PATH": str(
                            CAPACITY_SOURCE_CIRCUIT_PATH
                        ),
                        # Capacity workers wait behind one persistent shared
                        # half-open probe. Production tasks fail fast instead.
                        "WHOSCORED_SOURCE_CIRCUIT_WAIT": "1",
                    },
                    stdout=stdout_handle,
                    stderr=stderr_handle,
                    text=True,
                    start_new_session=True,
                    pass_fds=(() if control_fd is None else (control_fd,)),
                )
            except (OSError, ValueError, subprocess.SubprocessError) as exc:
                stdout_handle.close()
                stderr_handle.close()
                on_outcome(
                    WorkerOutcome(
                        worker_id=command.worker_id,
                        iteration=command.iteration,
                        scope=command.scope,
                        returncode=None,
                        report=None,
                        elapsed_seconds=0.0,
                        stderr_bytes=0,
                        stderr_sha256=hashlib.sha256(b"").hexdigest(),
                        launch_error=_safe_message(exc),
                    )
                )
                return False
            finally:
                if control_fd is not None:
                    try:
                        os.close(control_fd)
                    except OSError:
                        pass
            running.append(
                _RunningProcess(
                    command=command,
                    process=process,
                    stdout_handle=stdout_handle,
                    stderr_handle=stderr_handle,
                    started_at=monotonic(),
                )
            )
            return True

        try:
            for command in commands:
                if not launch(command):
                    break
                if should_stop():
                    break

            while running:
                on_sample(False)
                completed: list[WorkerCommand] = []
                for item in list(running):
                    if item.process.poll() is None:
                        continue
                    termination_reason = None
                    if _process_group_has_live_members(item.process.pid):
                        _stop_processes(
                            [item], monotonic=monotonic, sleep=sleep
                        )
                        termination_reason = "orphan_process_group"
                    on_outcome(
                        _finish_process(
                            item,
                            monotonic=monotonic,
                            termination_reason=termination_reason,
                        )
                    )
                    completed.append(item.command)
                    item.stdout_handle.close()
                    item.stderr_handle.close()
                    running.remove(item)

                stop_requested = should_stop()
                deadline_reached = monotonic() >= deadline
                if stop_requested or deadline_reached:
                    reason = (
                        "deadline_terminated"
                        if deadline_reached and not stop_requested
                        else "aborted_by_gate"
                    )
                    _stop_processes(running, monotonic=monotonic, sleep=sleep)
                    for item in list(running):
                        on_outcome(
                            _finish_process(
                                item,
                                monotonic=monotonic,
                                termination_reason=reason,
                            )
                        )
                        item.stdout_handle.close()
                        item.stderr_handle.close()
                        running.remove(item)
                    break

                for command in completed:
                    if should_stop():
                        break
                    replacement = WorkerCommand(
                        worker_id=command.worker_id,
                        iteration=command.iteration + 1,
                        scope=command.scope,
                        argv=command.argv,
                        browser_session_owner=command.browser_session_owner,
                        flaresolverr_endpoint=command.flaresolverr_endpoint,
                    )
                    if not launch(replacement):
                        break
                if should_stop():
                    continue
                if len(running) != WORKER_COUNT:
                    raise RuntimeError(
                        f"capacity supervisor lost worker slots: {len(running)}/"
                        f"{WORKER_COUNT}"
                    )
                sleep(min(_POLL_SECONDS, max(0.0, deadline - monotonic())))
        finally:
            if running:
                try:
                    _stop_processes(running, monotonic=monotonic, sleep=sleep)
                finally:
                    for item in running:
                        item.stdout_handle.close()
                        item.stderr_handle.close()


def _default_dependencies() -> CapacityDependencies:
    return CapacityDependencies(
        monotonic=time.monotonic,
        sleep=time.sleep,
        inspect_containers=_inspect_containers,
        sample_rss=_sample_process_rss,
        runtime_identity=_runtime_identity,
        run_round=_run_subprocess_round,
        prepare_session_ownership=_prepare_session_ownership,
    )


def _scope_values(args: argparse.Namespace) -> tuple[str, ...]:
    raw = getattr(args, "scopes", None)
    values = raw if raw else DEFAULT_SCOPES
    return tuple(str(value).strip() for value in values)


def _container_values(args: argparse.Namespace) -> tuple[str, ...]:
    raw = getattr(args, "containers", None)
    values = (*DEFAULT_CONTAINERS, *(raw or ()))
    return tuple(dict.fromkeys(str(value).strip() for value in values))


def _runtime_files(args: argparse.Namespace) -> tuple[Path, ...]:
    catalog = Path(str(args.catalog)).resolve()
    files = {
        *(REPO_ROOT / relative for relative in _RUNTIME_STATIC_PATHS),
        *sorted((REPO_ROOT / "scrapers" / "whoscored").glob("*.py")),
        catalog,
        WORKER_NAMESPACE_EXECUTABLE,
    }
    missing = sorted(str(path) for path in files if not path.is_file())
    if missing:
        raise RuntimeError("runtime identity files are missing: " + ", ".join(missing))
    return tuple(sorted((path.resolve() for path in files), key=str))


def _runtime_identity(args: argparse.Namespace) -> Mapping[str, Any]:
    files = _runtime_files(args)
    hashes: dict[str, str] = {}
    repository_paths: list[str] = []
    for path in files:
        try:
            label = path.relative_to(REPO_ROOT).as_posix()
            repository_paths.append(label)
        except ValueError:
            label = f"external:{path.name}"
        hashes[label] = hashlib.sha256(path.read_bytes()).hexdigest()
    try:
        revision_result = subprocess.run(
            ["git", "rev-parse", "HEAD"],
            cwd=REPO_ROOT,
            check=True,
            capture_output=True,
            text=True,
            timeout=10,
        )
        status_result = subprocess.run(
            [
                "git",
                "status",
                "--porcelain",
                "--untracked-files=all",
                "--",
                *repository_paths,
            ],
            cwd=REPO_ROOT,
            check=True,
            capture_output=True,
            text=True,
            timeout=10,
        )
    except (OSError, subprocess.SubprocessError) as exc:
        raise RuntimeError("cannot bind canary evidence to git revision") from exc
    python_executable = str(sys.executable)
    python_prefix = str(sys.prefix)
    dependency_versions = {"curl_cffi": _installed_curl_cffi_version()}
    identity_material = {
        "file_sha256": hashes,
        "python_executable": python_executable,
        "python_prefix": python_prefix,
        "python_version": sys.version.split()[0],
        "dependency_versions": dependency_versions,
    }
    aggregate = hashlib.sha256(
        json.dumps(
            identity_material, sort_keys=True, separators=(",", ":")
        ).encode("utf-8")
    ).hexdigest()
    return {
        "git_revision": revision_result.stdout.strip(),
        "git_clean": not bool(status_result.stdout.strip()),
        "manifest_sha256": aggregate,
        "file_sha256": hashes,
        "python_executable": python_executable,
        "python_prefix": python_prefix,
        "python_version": sys.version.split()[0],
        "dependency_versions": dependency_versions,
    }


def _runtime_identity_pin_error(identity: Mapping[str, Any]) -> Optional[str]:
    dependency_versions = identity.get("dependency_versions")
    if not isinstance(dependency_versions, Mapping):
        return "runtime identity omitted dependency versions"
    if dependency_versions.get("curl_cffi") != REQUIRED_CURL_CFFI_VERSION:
        return (
            "runtime identity does not match production "
            f"curl_cffi=={REQUIRED_CURL_CFFI_VERSION}"
        )
    file_sha256 = identity.get("file_sha256")
    if not isinstance(file_sha256, Mapping) or (
        file_sha256.get(f"external:{WORKER_NAMESPACE_EXECUTABLE.name}")
        != REQUIRED_UNSHARE_SHA256
    ):
        return "runtime identity does not match pinned unshare helper"
    return None


def _validate_args(args: argparse.Namespace) -> Optional[str]:
    try:
        duration = float(args.duration_seconds)
        sample_interval = float(args.sample_interval_seconds)
        match_limit = int(args.match_limit)
        profile_limit = int(args.profile_limit)
    except (AttributeError, OverflowError, TypeError, ValueError):
        return "duration, sampling interval and limits must be numeric"
    if not math.isfinite(duration) or duration <= 0:
        return "duration-seconds must be positive"
    if not math.isfinite(sample_interval) or not 0.1 <= sample_interval <= 300:
        return "sample-interval-seconds must be in 0.1..300"
    if not 1 <= match_limit <= MAX_MATCH_LIMIT:
        return f"match-limit must be in 1..{MAX_MATCH_LIMIT}"
    if not 1 <= profile_limit <= MAX_PROFILE_LIMIT:
        return f"profile-limit must be in 1..{MAX_PROFILE_LIMIT}"
    try:
        endpoint = _canonical_flaresolverr_endpoint(args.flaresolverr_url)
    except (AttributeError, TypeError, ValueError) as exc:
        return str(exc) or "invalid FlareSolverr endpoint"
    if endpoint != REQUIRED_FLARESOLVERR_ENDPOINT:
        return "FlareSolverr endpoint does not match production binding"
    for scope in _scope_values(args):
        if scope.count("=") != 1 or not all(
            part.strip() for part in scope.split("=", 1)
        ):
            return "every scope must have the form '<competition>=<season-id>'"
    containers = _container_values(args)
    if not containers or any(not name for name in containers):
        return "at least one non-empty container name is required"
    output = getattr(args, "output", None)
    if output is not None:
        target = Path(output)
        if target.exists():
            return f"output already exists: {target}"
        if not target.parent.is_dir():
            return f"output parent does not exist: {target.parent}"
    lock_path, owner_path = _state_paths(args)
    if lock_path == owner_path:
        return "supervisor lock and session owner files must be different"
    if not lock_path.parent.is_dir() or not owner_path.parent.is_dir():
        return "supervisor state parent directories must exist"
    return None


def _build_commands(
    args: argparse.Namespace,
    iteration: int,
    *,
    browser_session_owner: str,
) -> list[WorkerCommand]:
    owner = _validate_session_owner(browser_session_owner)
    scopes = _scope_values(args)
    commands: list[WorkerCommand] = []
    for worker_id in range(WORKER_COUNT):
        scope = scopes[(iteration * WORKER_COUNT + worker_id) % len(scopes)]
        argv = (
            sys.executable,
            str(WORKFLOW_SCRIPT),
            "--scope",
            scope,
            "--match-limit",
            str(int(args.match_limit)),
            "--profile-limit",
            str(int(args.profile_limit)),
            "--catalog",
            str(args.catalog),
        )
        commands.append(
            WorkerCommand(
                worker_id=worker_id,
                iteration=iteration,
                scope=scope,
                argv=argv,
                browser_session_owner=owner,
                flaresolverr_endpoint=REQUIRED_FLARESOLVERR_ENDPOINT,
            )
        )
    return commands


def _gate_documents(
    accumulator: CapacityAccumulator,
    *,
    elapsed_seconds: float,
    requested_duration_seconds: float,
) -> list[dict[str, Any]]:
    projected = _projected_page_units_per_day(
        accumulator.page_units, elapsed_seconds
    )
    all_workers_completed = all(
        accumulator.completed_by_worker.get(worker_id, 0) > 0
        for worker_id in range(WORKER_COUNT)
    )
    duration_complete = (
        not accumulator.interrupted
        and elapsed_seconds + 0.05 >= requested_duration_seconds
    )
    return [
        {
            "name": "throughput",
            "passed": projected >= MIN_PAGE_UNITS_PER_DAY,
            "observed_page_units_per_day": round(projected, 3),
            "minimum_page_units_per_day": MIN_PAGE_UNITS_PER_DAY,
        },
        {
            "name": "paid_traffic",
            "passed": (
                accumulator.paid_bytes == 0
                and accumulator.paid_route_requests == 0
                and not accumulator.traffic_evidence_violations
            ),
            "observed_paid_bytes": accumulator.paid_bytes,
            "observed_paid_route_requests": accumulator.paid_route_requests,
            "required_paid_bytes": 0,
            "required_paid_route_requests": 0,
            "evidence_violations": list(
                accumulator.traffic_evidence_violations
            ),
        },
        {
            "name": "memory",
            "passed": accumulator.max_aggregate_memory_bytes <= MAX_RSS_BYTES,
            "observed_max_harness_rss_bytes": accumulator.max_harness_rss_bytes,
            "observed_max_container_memory_bytes": (
                accumulator.max_container_memory_bytes
            ),
            "observed_max_aggregate_memory_bytes": (
                accumulator.max_aggregate_memory_bytes
            ),
            "maximum_aggregate_memory_bytes": MAX_RSS_BYTES,
        },
        {
            "name": "container_restart_oom",
            "passed": not accumulator.container_violations,
            "violations": list(accumulator.container_violations),
        },
        {
            "name": "non_publishing",
            "passed": not accumulator.safety_violations,
            "violations": list(accumulator.safety_violations),
        },
        {
            "name": "runtime_identity",
            "passed": (
                bool(accumulator.runtime_identity)
                and not accumulator.runtime_identity_violations
            ),
            "git_revision": accumulator.runtime_identity.get("git_revision"),
            "manifest_sha256": accumulator.runtime_identity.get("manifest_sha256"),
            "git_clean": accumulator.runtime_identity.get("git_clean"),
            "violations": list(accumulator.runtime_identity_violations),
        },
        {
            "name": "representative_workload",
            "passed": (
                accumulator.completed_runs > 0
                and accumulator.max_source_stage_count >= 2
            ),
            "required_entities": ["schedule", "matches", "previews", "profiles"],
            "observed_max_source_stage_count": accumulator.max_source_stage_count,
            "minimum_source_stage_count": 2,
        },
        {
            "name": "worker_health",
            "passed": not accumulator.worker_errors and all_workers_completed,
            "completed_by_worker": dict(accumulator.completed_by_worker),
            "errors": list(accumulator.worker_errors),
        },
        {
            "name": "monitoring_evidence",
            "passed": (
                not accumulator.monitoring_errors
                and bool(accumulator.samples)
                and bool(accumulator.baseline_containers)
            ),
            "sample_count": len(accumulator.samples),
            "errors": list(accumulator.monitoring_errors),
        },
        {
            "name": "sustained_duration",
            "passed": duration_complete,
            "observed_seconds": round(elapsed_seconds, 3),
            "required_seconds": requested_duration_seconds,
        },
    ]


def _session_cleanup_gate(evidence: Mapping[str, Any]) -> dict[str, Any]:
    passed = (
        evidence.get("lock_acquired") is True
        and evidence.get("preflight_verified_zero") is True
        and evidence.get("final_verified_zero") is True
        and evidence.get("state_file_removed") is True
        and evidence.get("quiet_window_observed") is True
    )
    return {
        "name": "browser_session_cleanup",
        "passed": passed,
        **dict(evidence),
    }


def _install_termination_handlers(
    enabled: bool, on_signal: Callable[[int], None]
) -> dict[int, Any]:
    if not enabled:
        return {}
    previous: dict[int, Any] = {}
    termination_started = False

    def terminate(signum: int, _frame: Any) -> None:
        nonlocal termination_started
        if termination_started:
            return
        termination_started = True
        on_signal(signum)

    for signum in (signal.SIGTERM, signal.SIGHUP, signal.SIGINT):
        previous[signum] = signal.getsignal(signum)
        signal.signal(signum, terminate)
    return previous


def _restore_termination_handlers(previous: Mapping[int, Any]) -> None:
    for signum, handler in previous.items():
        signal.signal(signum, handler)


def _install_deferred_termination_handlers(
    enabled: bool, on_signal: Callable[[int], None]
) -> None:
    """Make cleanup non-interruptible while still recording its first signal."""

    if not enabled:
        return
    observed = False

    def defer(signum: int, _frame: Any) -> None:
        nonlocal observed
        if observed:
            return
        observed = True
        on_signal(signum)

    for signum in (signal.SIGTERM, signal.SIGHUP, signal.SIGINT):
        signal.signal(signum, defer)


def run(
    args: argparse.Namespace,
    *,
    dependencies: Optional[CapacityDependencies] = None,
) -> tuple[int, dict[str, Any]]:
    """Execute the canary and return a process code plus JSON-safe evidence."""

    validation_error = _validate_args(args)
    if validation_error is None and dependencies is None:
        validation_error = _workflow_runtime_preflight()
    if validation_error:
        return 2, {
            "canary_version": CANARY_VERSION,
            "status": "configuration_error",
            "error": validation_error,
            "publishes": False,
            "writes_bronze": False,
            "executes_ddl": False,
            "worker_count": WORKER_COUNT,
        }

    deps = dependencies or _default_dependencies()
    prepare_ownership = (
        deps.prepare_session_ownership or _prepare_noop_session_ownership
    )
    try:
        ownership = prepare_ownership(
            args,
            monotonic=deps.monotonic,
            sleep=deps.sleep,
        )
    except _SessionOwnershipPreparationError as exc:
        return 2, {
            "canary_version": CANARY_VERSION,
            "status": "configuration_error",
            "error": exc.reason,
            "publishes": False,
            "writes_bronze": False,
            "executes_ddl": False,
            "worker_count": WORKER_COUNT,
            "workers_launched": 0,
            "session_cleanup": dict(exc.evidence),
        }

    accumulator = CapacityAccumulator()
    duration = float(args.duration_seconds)
    sample_interval = float(args.sample_interval_seconds)
    containers = _container_values(args)
    started_wall = _utc_now()
    started = deps.monotonic()
    deadline = started + duration
    last_sample_at: Optional[float] = None
    stop_reasons: set[str] = set()

    def verify_runtime_identity() -> None:
        if not accumulator.runtime_identity:
            return
        try:
            current = dict(deps.runtime_identity(args))
        except Exception as exc:
            message = "runtime identity check failed: " + _safe_message(exc)
            if message not in accumulator.runtime_identity_violations:
                accumulator.runtime_identity_violations.append(message)
            stop_reasons.add("runtime_identity")
            return
        for field_name in (
            "git_revision",
            "manifest_sha256",
            "python_executable",
            "python_prefix",
            "dependency_versions",
        ):
            if current.get(field_name) != accumulator.runtime_identity.get(field_name):
                message = f"runtime identity changed: {field_name}"
                if message not in accumulator.runtime_identity_violations:
                    accumulator.runtime_identity_violations.append(message)
        if accumulator.runtime_identity_violations:
            stop_reasons.add("runtime_identity")

    def verify_before_launch() -> None:
        verify_runtime_identity()
        if accumulator.runtime_identity_violations:
            raise RuntimeError("runtime identity changed before worker launch")

    def capture_sample(force: bool = False) -> None:
        nonlocal last_sample_at
        now = deps.monotonic()
        if (
            not force
            and last_sample_at is not None
            and now - last_sample_at < sample_interval
        ):
            return
        try:
            verify_runtime_identity()
            current = {
                name: dict(value)
                for name, value in deps.inspect_containers(containers).items()
            }
            if set(current) != set(containers):
                raise RuntimeError("container sample did not cover every required name")
            if not accumulator.baseline_containers:
                accumulator.baseline_containers = {
                    name: dict(value) for name, value in current.items()
                }
            violations = _container_gate_violations(
                accumulator.baseline_containers, current
            )
            flaresolverr = current.get("flaresolverr")
            flaresolverr_contracts = (
                (
                    "command_contract_ok",
                    "flaresolverr: running command does not match production",
                ),
                (
                    "image_identity_contract_ok",
                    "flaresolverr: derived image ID does not match the reviewed compose image",
                ),
                (
                    "immutable_payload_contract_ok",
                    "flaresolverr: immutable payload/root filesystem contract does not match",
                ),
                (
                    "security_contract_ok",
                    "flaresolverr: runtime security contract does not match production",
                ),
                (
                    "compose_identity_ok",
                    "flaresolverr: running compose identity does not match production",
                ),
                (
                    "published_endpoint_contract_ok",
                    "flaresolverr: published endpoint does not match 127.0.0.1:8191",
                ),
            )
            for field_name, message in flaresolverr_contracts:
                if (
                    not isinstance(flaresolverr, Mapping)
                    or flaresolverr.get(field_name) is not True
                ):
                    violations.append(message)
            for violation in violations:
                if violation not in accumulator.container_violations:
                    accumulator.container_violations.append(violation)
            if violations:
                stop_reasons.add("container_restart_oom")

            rss = dict(deps.sample_rss((os.getpid(),)))
            rss_bytes = _as_nonnegative_int(
                rss.get("rss_bytes", 0), field_name="rss_bytes"
            )
            container_memory_bytes = sum(
                _as_nonnegative_int(
                    value.get("memory_usage_bytes", 0),
                    field_name=f"{name} memory_usage_bytes",
                )
                for name, value in current.items()
            )
            aggregate_memory_bytes = rss_bytes + container_memory_bytes
            accumulator.max_harness_rss_bytes = max(
                accumulator.max_harness_rss_bytes, rss_bytes
            )
            accumulator.max_container_memory_bytes = max(
                accumulator.max_container_memory_bytes, container_memory_bytes
            )
            accumulator.max_aggregate_memory_bytes = max(
                accumulator.max_aggregate_memory_bytes, aggregate_memory_bytes
            )
            if aggregate_memory_bytes > MAX_RSS_BYTES:
                stop_reasons.add("memory")

            elapsed = max(0.0, now - started)
            accumulator.samples.append(
                {
                    "elapsed_seconds": round(elapsed, 3),
                    "completed_runs": accumulator.completed_runs,
                    "source_request_attempts": accumulator.source_request_attempts,
                    "page_units": accumulator.page_units,
                    "projected_page_units_per_day": round(
                        _projected_page_units_per_day(
                            accumulator.page_units, elapsed
                        ),
                        3,
                    ),
                    "paid_bytes": accumulator.paid_bytes,
                    "paid_route_requests": accumulator.paid_route_requests,
                    "harness_rss_bytes": rss_bytes,
                    "container_memory_bytes": container_memory_bytes,
                    "aggregate_memory_bytes": aggregate_memory_bytes,
                    "rss_process_count": _as_nonnegative_int(
                        rss.get("process_count", 0), field_name="process_count"
                    ),
                    "containers": [current[name] for name in containers],
                }
            )
            last_sample_at = now
        except Exception as exc:
            message = _safe_message(exc)
            if message not in accumulator.monitoring_errors:
                accumulator.monitoring_errors.append(message)
            stop_reasons.add("monitoring_evidence")

    def accept_outcome(outcome: WorkerOutcome) -> None:
        _accept_outcome(accumulator, outcome)
        if accumulator.paid_bytes or accumulator.paid_route_requests:
            stop_reasons.add("paid_traffic")
        if accumulator.traffic_evidence_violations:
            stop_reasons.add("paid_traffic")
        if accumulator.worker_errors:
            stop_reasons.add("worker_health")
        if accumulator.safety_violations:
            stop_reasons.add("non_publishing")

    iteration = 0
    initial_identity_accepted = False
    work_ended: Optional[float] = None
    total_ended: Optional[float] = None
    cleanup_evidence: Mapping[str, Any]
    processes_verified_dead = True

    def record_termination(signum: int) -> None:
        accumulator.interrupted = True
        stop_reasons.add(f"signal_{signal.Signals(signum).name}")

    previous_handlers = _install_termination_handlers(
        dependencies is None, record_termination
    )

    try:
        try:
            accumulator.runtime_identity = dict(deps.runtime_identity(args))
            pin_error = _runtime_identity_pin_error(accumulator.runtime_identity)
            if pin_error:
                accumulator.runtime_identity_violations.append(pin_error)
                stop_reasons.add("runtime_identity")
            else:
                initial_identity_accepted = True
        except Exception as exc:
            accumulator.runtime_identity_violations.append(
                "initial runtime identity failed: " + _safe_message(exc)
            )
            stop_reasons.add("runtime_identity")
        if initial_identity_accepted:
            capture_sample(force=True)
        while deps.monotonic() < deadline and not stop_reasons:
            commands = _build_commands(
                args,
                iteration,
                browser_session_owner=ownership.owner,
            )
            deps.run_round(
                commands,
                deadline=deadline,
                on_sample=capture_sample,
                on_outcome=accept_outcome,
                should_stop=lambda: bool(stop_reasons),
                before_launch=verify_before_launch,
                monotonic=deps.monotonic,
                sleep=deps.sleep,
            )
            iteration += 1
    except _SupervisorTermination as exc:
        record_termination(exc.signum)
    except KeyboardInterrupt:
        accumulator.interrupted = True
        stop_reasons.add("interrupted")
    except _WorkerTerminationUnverified as exc:
        processes_verified_dead = False
        accumulator.worker_errors.append(
            "capacity worker termination was not verified: " + _safe_message(exc)
        )
        stop_reasons.add("worker_health")
    except Exception as exc:
        accumulator.worker_errors.append(
            "capacity supervisor failed: " + _safe_message(exc)
        )
        stop_reasons.add("worker_health")
    finally:
        while True:
            try:
                _install_deferred_termination_handlers(
                    dependencies is None, record_termination
                )
                break
            except _SupervisorTermination as exc:
                record_termination(exc.signum)
        try:
            if initial_identity_accepted:
                try:
                    capture_sample(force=True)
                except _SupervisorTermination as exc:
                    record_termination(exc.signum)
        finally:
            work_ended = deps.monotonic()
            try:
                if not processes_verified_dead:
                    failed = _empty_cleanup_result(required=True, verified=False)
                    failed["error_count"] = 1
                    failed["error_sha256"] = [
                        _cleanup_error_hash(
                            "worker_termination", _WorkerTerminationUnverified()
                        )
                    ]
                    cleanup_evidence = _ownership_evidence(
                        lock_acquired=True,
                        preflight=ownership.preflight,
                        final=failed,
                        state_file_removed=False,
                    )
                else:
                    cleanup_evidence = ownership.finalize()
            except _SupervisorTermination as exc:
                record_termination(exc.signum)
                try:
                    cleanup_evidence = ownership.finalize()
                except Exception as retry_exc:
                    failed = _empty_cleanup_result(required=True, verified=False)
                    failed["error_count"] = 1
                    failed["error_sha256"] = [
                        _cleanup_error_hash("finalize", retry_exc)
                    ]
                    cleanup_evidence = _ownership_evidence(
                        lock_acquired=True,
                        preflight=ownership.preflight,
                        final=failed,
                        state_file_removed=False,
                    )
            except Exception as exc:
                failed = _empty_cleanup_result(required=True, verified=False)
                failed["error_count"] = 1
                failed["error_sha256"] = [
                    _cleanup_error_hash("finalize", exc)
                ]
                cleanup_evidence = _ownership_evidence(
                    lock_acquired=True,
                    preflight=ownership.preflight,
                    final=failed,
                    state_file_removed=False,
                )
            finally:
                try:
                    if initial_identity_accepted:
                        try:
                            capture_sample(force=True)
                        except _SupervisorTermination as exc:
                            record_termination(exc.signum)
                    total_ended = deps.monotonic()
                finally:
                    try:
                        ownership.close()
                    finally:
                        # Cleanup is complete and no workers remain.  Restore
                        # the host's signal policy before gates/report work so
                        # a late signal cannot mutate an already-built status.
                        _restore_termination_handlers(previous_handlers)

    cleanup_gate = _session_cleanup_gate(cleanup_evidence)
    if not cleanup_gate["passed"]:
        stop_reasons.add("browser_session_cleanup")

    elapsed = max(0.0, (work_ended or deps.monotonic()) - started)
    total_elapsed = max(0.0, (total_ended or deps.monotonic()) - started)
    cleanup_elapsed = max(0.0, total_elapsed - elapsed)
    gates = _gate_documents(
        accumulator,
        elapsed_seconds=elapsed,
        requested_duration_seconds=duration,
    )
    gates.append(cleanup_gate)
    status = "success" if all(gate["passed"] for gate in gates) else "failed"
    report: dict[str, Any] = {
        "canary_version": CANARY_VERSION,
        "status": status,
        "started_at": started_wall,
        "ended_at": _utc_now(),
        "duration_seconds_requested": duration,
        "elapsed_seconds": round(elapsed, 3),
        "cleanup_elapsed_seconds": round(cleanup_elapsed, 3),
        "total_elapsed_seconds": round(total_elapsed, 3),
        "worker_count": WORKER_COUNT,
        "scopes": list(_scope_values(args)),
        "match_limit": int(args.match_limit),
        "profile_limit": int(args.profile_limit),
        "publishes": False,
        "writes_bronze": False,
        "executes_ddl": False,
        "raw_store_policy": "per-process temporary local storage",
        "repository_policy": "per-process in-memory repository",
        "page_unit_definition": (
            "one unique successfully completed source target per workflow phase"
        ),
        "source_request_attempts": accumulator.source_request_attempts,
        "page_units": accumulator.page_units,
        "projected_page_units_per_day": round(
            _projected_page_units_per_day(accumulator.page_units, elapsed), 3
        ),
        "paid_bytes": accumulator.paid_bytes,
        "paid_route_requests": accumulator.paid_route_requests,
        "completed_runs": accumulator.completed_runs,
        "completed_by_worker": dict(accumulator.completed_by_worker),
        "completed_worker_seconds": round(accumulator.worker_seconds, 3),
        "max_source_stage_count": accumulator.max_source_stage_count,
        "deadline_truncations": accumulator.deadline_truncations,
        "max_harness_rss_bytes": accumulator.max_harness_rss_bytes,
        "max_container_memory_bytes": accumulator.max_container_memory_bytes,
        "max_aggregate_memory_bytes": accumulator.max_aggregate_memory_bytes,
        "runtime_identity": accumulator.runtime_identity,
        "session_cleanup": dict(cleanup_evidence),
        "baseline_containers": [
            accumulator.baseline_containers[name]
            for name in containers
            if name in accumulator.baseline_containers
        ],
        "stop_reasons": sorted(stop_reasons),
        "gates": gates,
        "samples": accumulator.samples,
        "runs": accumulator.run_summaries,
    }
    # Prove that the supervisor itself emits one JSON-safe evidence document.
    report = _json_safe_document(report)
    return (0 if status == "success" else 1), report


def _write_report(path: Path, report: Mapping[str, Any]) -> None:
    payload = json.dumps(report, ensure_ascii=False, sort_keys=True) + "\n"
    descriptor, temporary_name = mkstemp(
        prefix=f".{path.name}.", suffix=".tmp", dir=path.parent
    )
    temporary = Path(temporary_name)
    try:
        os.fchmod(descriptor, 0o600)
        with os.fdopen(descriptor, "w", encoding="utf-8") as handle:
            handle.write(payload)
            handle.flush()
            os.fsync(handle.fileno())
        os.link(temporary, path)
        directory = os.open(path.parent, os.O_RDONLY)
        try:
            os.fsync(directory)
        finally:
            os.close(directory)
    finally:
        temporary.unlink(missing_ok=True)


def _parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument(
        "--duration-seconds",
        type=float,
        default=DEFAULT_DURATION_SECONDS,
        help="wall-clock canary duration (default: 21600, six hours)",
    )
    parser.add_argument(
        "--sample-interval-seconds",
        type=float,
        default=DEFAULT_SAMPLE_INTERVAL_SECONDS,
    )
    parser.add_argument(
        "--scope",
        dest="scopes",
        action="append",
        help="repeat to rotate representative scopes across the four workers",
    )
    parser.add_argument("--match-limit", type=int, default=DEFAULT_MATCH_LIMIT)
    parser.add_argument("--profile-limit", type=int, default=DEFAULT_PROFILE_LIMIT)
    parser.add_argument(
        "--catalog",
        default=str(REPO_ROOT / "configs" / "medallion" / "competitions.yaml"),
    )
    parser.add_argument(
        "--flaresolverr-url",
        default=os.environ.get("FLARESOLVERR_URL", "http://127.0.0.1:8191"),
    )
    parser.add_argument(
        "--container",
        dest="containers",
        action="append",
        help=(
            "repeat to add a monitored container; airflow-scheduler, flaresolverr "
            "and proxy_filter are always required"
        ),
    )
    parser.add_argument(
        "--output",
        type=Path,
        help="optional new 0600 local JSON evidence file (never an object-store URI)",
    )
    return parser


def main() -> int:
    args = _parser().parse_args()
    code, report = run(args)
    if args.output is not None:
        try:
            _write_report(args.output, report)
        except Exception as exc:
            report["status"] = "failed"
            report["output_error"] = _safe_message(exc)
            code = 1
    print(json.dumps(report, ensure_ascii=False, sort_keys=True, default=str))
    return code


if __name__ == "__main__":
    raise SystemExit(main())
