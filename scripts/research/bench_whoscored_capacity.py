#!/usr/bin/env python3
"""Run a sustained, four-worker, non-publishing WhoScored capacity canary.

Each worker repeatedly executes ``bench_whoscored_workflow.py`` in an
independent process.  Production uses the exact content-addressed cache corpus,
with worker network and paid routes forbidden.  The former direct-first path is
retained only as an explicit diagnostic mode.  Both workflows use a temporary
local raw store and an in-memory repository, so this supervisor cannot publish
Bronze rows, manifests or DDL.  Production runs exactly four workers for six
hours and fails closed unless all of these gates hold:

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
import io
import json
import math
import os
from pathlib import Path, PurePosixPath
import re
import secrets
import select
import shutil
import signal
import stat
import subprocess
import sys
from tempfile import mkstemp, TemporaryDirectory
import time
import types
from typing import Any, Callable, Mapping, Optional, Sequence
from urllib.parse import urlsplit
import zipfile

import requests
from requests.adapters import HTTPAdapter


REPO_ROOT = Path(__file__).resolve().parents[2]
WORKFLOW_SCRIPT = REPO_ROOT / "scripts" / "research" / "bench_whoscored_workflow.py"
FLARESOLVERR_EXTENSION_SCRIPT = REPO_ROOT / "scripts" / "flaresolverr_extended.py"
WORKER_EXEC_SCRIPT = (
    REPO_ROOT / "scripts" / "research" / "whoscored_capacity_worker_exec.py"
)

CANARY_VERSION = "whoscored-capacity-canary-v4"
CAPACITY_REPORT_SCHEMA_VERSION = 1
CACHE_CAPACITY_MODE = "cache-capacity-v1"
DIRECT_DIAGNOSTIC_MODE = "direct-diagnostic-v1"
EXPECTED_WORKFLOW_VERSION = "whoscored-workflow-benchmark-v2"
EXPECTED_CACHE_SEED_SHA256 = (
    "eb3739448a42a00f3a5812c20a0885ea691306634b2922cda1dd2d7c8490fb4a"
)
REQUIRED_CURL_CFFI_VERSION = "0.15.0"
REQUIRED_FLARESOLVERR_VERSION = "3.4.6"
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
PRODUCTION_COMPOSE_FILES = (
    REPO_ROOT / "compose.yaml",
    REPO_ROOT / "compose.seaweedfs-supervised.yaml",
)
PRODUCTION_GATEWAY_COMPOSE_FILE = (
    REPO_ROOT / "deploy/whoscored/gateway.compose.yaml"
)
PRODUCTION_COMPOSE_ENV_FILES = (
    Path("/root/data-platform-football/.env"),
    Path("/root/.secrets/whoscored-runtime-v2.env"),
    Path("/root/.secrets/whoscored-proxy-v2.env"),
)
PRODUCTION_ADMISSION_SCRIPT = REPO_ROOT / "scripts/whoscored_production_admission.py"
PRODUCTION_BUILD_ATTESTATION = (
    REPO_ROOT
    / "docker/images/airflow/whoscored-build-provenance-attestation.json"
)
PRODUCTION_BUILD_MANIFEST = (
    REPO_ROOT / "docker/images/airflow/whoscored-build-provenance-manifest.json"
)
PRODUCTION_ADMISSION_PYTHON = Path("/usr/bin/python3")
_PINNED_IMAGE_RE = re.compile(
    r"[a-z0-9][a-z0-9._-]*(?::[0-9]+)?/"
    r"[a-z0-9]+(?:[._-][a-z0-9]+)*(?:/[a-z0-9]+(?:[._-][a-z0-9]+)*)*"
    r"@sha256:[0-9a-f]{64}"
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
MAX_RETAINED_ERROR_RUN_SUMMARIES = 8
MAX_RETAINED_RUN_SUMMARIES = WORKER_COUNT * 2 + MAX_RETAINED_ERROR_RUN_SUMMARIES
_CAPACITY_BOOTSTRAP_LIVENESS_EXIT_CODE = 70
DEFAULT_DURATION_SECONDS = 6 * 60 * 60
DEFAULT_SAMPLE_INTERVAL_SECONDS = 30.0
MAX_RETAINED_SAMPLES = (
    math.ceil(DEFAULT_DURATION_SECONDS / DEFAULT_SAMPLE_INTERVAL_SECONDS) + 4
)
DEFAULT_SCOPES = ("INT-World Cup=2026", "ENG-Premier League=2526")
DEFAULT_MATCH_LIMIT = 3
DEFAULT_PROFILE_LIMIT = 3
MAX_MATCH_LIMIT = 10
MAX_PROFILE_LIMIT = 20
ADMITTED_RUNNING_SERVICES = ("airflow-scheduler", "flaresolverr")
PROTECTED_PRODUCTION_SERVICES = (
    "airflow-scheduler",
    "flaresolverr",
    "flaresolverr_whoscored_paid",
    "whoscored_paid_gateway",
    "whoscored_proxy_filter",
)
DEFAULT_CONTAINERS = ADMITTED_RUNNING_SERVICES
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
    "docker/images/airflow/whoscored-build-provenance-attestation.json",
    "docker/images/airflow/whoscored-build-provenance-manifest.json",
    "docker/images/airflow/whoscored_capacity_worker_bootstrap.py",
    "scripts/research/bench_whoscored_capacity.py",
    "scripts/research/bench_whoscored_workflow.py",
    "scripts/research/whoscored_capacity_container_runtime.py",
    "scripts/research/whoscored_capacity_worker_exec.py",
    "scripts/flaresolverr_extended.py",
    "scripts/audit_seaweedfs_control_network.py",
    "scripts/audit_seaweedfs_runtime_container.py",
    "scripts/compose.sh",
    "scripts/seaweedfs_legacy_entrypoint.sh",
    "scripts/seaweedfs_lifecycle_lock.sh",
    "scripts/validate_seaweedfs_s3_identity_config.py",
    "scripts/validate_whoscored_build_provenance.py",
    "scripts/whoscored_production_admission.py",
    "scripts/proxy_filter/filter_proxy.py",
    "docker/images/airflow/requirements-scraping.txt",
    "scrapers/__init__.py",
    "scrapers/base/__init__.py",
    "scrapers/base/flaresolverr_client.py",
    "scrapers/base/iceberg_writer.py",
    "scrapers/base/sql_validator.py",
    "scrapers/base/trino_manager.py",
    "scrapers/utils/__init__.py",
    "scrapers/utils/rate_limiter.py",
    "scrapers/whoscored/runtime_contract.lock",
)
_WORKER_BUNDLE_PATHS = (
    "scrapers/__init__.py",
    "scrapers/base/__init__.py",
    "scrapers/base/flaresolverr_client.py",
    "scrapers/base/iceberg_writer.py",
    "scrapers/base/sql_validator.py",
    "scrapers/base/trino_manager.py",
    "scrapers/utils/__init__.py",
    "scrapers/utils/rate_limiter.py",
    "scrapers/whoscored/__init__.py",
    "scrapers/whoscored/catalog.py",
    "scrapers/whoscored/detailed_feeds.py",
    "scrapers/whoscored/domain.py",
    "scrapers/whoscored/parsers.py",
    "scrapers/whoscored/profile_policy.py",
    "scrapers/whoscored/proxy_campaign.py",
    "scrapers/whoscored/raw_store.py",
    "scrapers/whoscored/repository.py",
    "scrapers/whoscored/runtime_contract.lock",
    "scrapers/whoscored/runtime_contract.py",
    "scrapers/whoscored/runtime_limits.py",
    "scrapers/whoscored/service.py",
    "scrapers/whoscored/source_circuit.py",
    "scrapers/whoscored/stage_feeds.py",
    "scrapers/whoscored/transport.py",
)
_WORKER_RUNTIME_CONTRACT_RELATIVE = "scrapers/whoscored/runtime_contract.lock"
_WORKER_WORKFLOW_RELATIVE = "scripts/research/bench_whoscored_workflow.py"
_WORKER_HELPER_RELATIVE = "scripts/research/whoscored_capacity_worker_exec.py"
_WORKER_CONTAINER_RUNTIME_RELATIVE = (
    "scripts/research/whoscored_capacity_container_runtime.py"
)
_CONTAINER_WORKFLOW_PATH = "/opt/airflow/scripts/research/bench_whoscored_workflow.py"
_CONTAINER_CATALOG_PATH = "/opt/airflow/configs/medallion/competitions.yaml"
_HOST_RUNTIME_OWNER_PREFIX = "whoscored-capacity-runtime-"
_POLL_SECONDS = 0.25
_TERMINATE_GRACE_SECONDS = 10.0
_KILL_CONFIRM_SECONDS = 5.0
_WORKER_READY_TIMEOUT_SECONDS = 10.0
_WORKER_READY_PAYLOAD = b"READY\n"
_SESSION_OWNER_RE = re.compile(r"[a-z0-9]{16,32}")
_SESSION_PREFIX_TEMPLATE = "ws-cap-{owner}-"
_OWNER_STATE_SCHEMA_VERSION = 2
_DEFAULT_SUPERVISOR_LOCK_PATH = Path(
    "/root/.whoscored-capacity-supervisor.lock"
)
_DEFAULT_SESSION_OWNER_PATH = Path(
    "/root/.whoscored-capacity-session-owner.json"
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

_LOCAL_DOCKER_ENVIRONMENT = {
    "HOME": "/nonexistent",
    "PATH": "/usr/bin:/bin",
    "LANG": "C.UTF-8",
    "LC_ALL": "C.UTF-8",
    "DOCKER_HOST": "unix:///run/docker.sock",
}
_LOCAL_DOCKER_CLI = Path("/usr/bin/docker")
_LOCAL_DOCKER_SOCKET = Path("/run/docker.sock")
_LOCAL_GIT_CLI = Path("/usr/bin/git")
_LOCAL_GIT_ENVIRONMENT = {
    "HOME": "/nonexistent",
    "LANG": "C.UTF-8",
    "LC_ALL": "C.UTF-8",
    "PATH": "/usr/bin:/bin",
}
_PROTECTED_INPUT_IDENTITY_FIELDS = (
    "st_dev",
    "st_ino",
    "st_mode",
    "st_uid",
    "st_gid",
    "st_nlink",
    "st_size",
    "st_mtime_ns",
    "st_ctime_ns",
)
_SAFE_IMAGE_LABEL_PREFIXES = (
    "org.apache.airflow.",
    "org.opencontainers.image.",
)
_COMPOSE_VERSION_RE = re.compile(
    r"^([0-9]+)\.([0-9]+)\.([0-9]+)(?:[-+][0-9A-Za-z.-]+)?$"
)


@dataclass(frozen=True)
class WorkerCommand:
    """One isolated workflow invocation in a fixed four-worker round."""

    worker_id: int
    iteration: int
    scope: str
    argv: tuple[str, ...]
    browser_session_owner: Optional[str] = None
    flaresolverr_endpoint: Optional[str] = None
    session_owner: Optional[str] = None


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
    network_requests: int = 0
    completed_runs: int = 0
    completed_by_worker: dict[int, int] = field(
        default_factory=lambda: {worker_id: 0 for worker_id in range(WORKER_COUNT)}
    )
    worker_seconds: float = 0.0
    deadline_truncations: int = 0
    run_summaries: list[dict[str, Any]] = field(default_factory=list)
    run_summaries_total: int = 0
    first_run_summary_by_worker: dict[int, dict[str, Any]] = field(
        default_factory=dict
    )
    last_run_summary_by_worker: dict[int, dict[str, Any]] = field(
        default_factory=dict
    )
    error_run_summaries: list[dict[str, Any]] = field(default_factory=list)
    worker_errors: list[str] = field(default_factory=list)
    safety_violations: list[str] = field(default_factory=list)
    traffic_evidence_violations: list[str] = field(default_factory=list)
    seed_evidence_violations: list[str] = field(default_factory=list)
    cleanup_evidence_violations: list[str] = field(default_factory=list)
    seed_sha256: Optional[str] = None
    monitoring_errors: list[str] = field(default_factory=list)
    container_violations: list[str] = field(default_factory=list)
    samples: list[dict[str, Any]] = field(default_factory=list)
    baseline_containers: dict[str, dict[str, Any]] = field(default_factory=dict)
    max_harness_rss_bytes: int = 0
    max_container_memory_bytes: int = 0
    max_worker_container_memory_bytes: int = 0
    max_worker_container_pids: int = 0
    max_aggregate_memory_bytes: int = 0
    max_source_stage_count: int = 0
    runtime_identity: dict[str, Any] = field(default_factory=dict)
    runtime_identity_violations: list[str] = field(default_factory=list)
    interrupted: bool = False


@dataclass(frozen=True)
class ProtectedInputSnapshot:
    """One fd-pinned production input, including replacement evidence."""

    label: str
    path: Path
    identity: tuple[int, ...]
    sha256: str
    private: bool

    def evidence(self) -> dict[str, Any]:
        return {
            "identity": list(self.identity),
            "sha256": self.sha256,
        }


@dataclass(frozen=True)
class ProductionDeployment:
    """One externally attested ready-v1 Compose/image identity."""

    deployment_attestation_path: Path
    deployment_attestation_sha256: str
    deployment_attestation_identity: tuple[int, ...]
    common_digest_override_path: Path
    common_digest_override_sha256: str
    common_digest_override_identity: tuple[int, ...]
    gateway_digest_override_path: Optional[Path]
    gateway_digest_override_sha256: Optional[str]
    gateway_digest_override_identity: Optional[tuple[int, ...]]
    provider_policy_path: Optional[Path]
    owner_secret_file_path: Optional[Path]
    deployment_admission_receipt_path: Optional[Path]
    release_revision: str
    payload_revision: str
    provenance_manifest_sha256: str
    source_tree_sha256: str
    protected_bindings: Mapping[str, str]
    protected_payload_image_ids: Mapping[str, str]
    protected_config_hashes: Mapping[str, str]
    running_admission: Mapping[str, Any]
    protected_inputs: tuple[ProtectedInputSnapshot, ...]
    admission_mode: str = DIRECT_DIAGNOSTIC_MODE

    @property
    def flaresolverr_payload_image_id(self) -> str:
        return self.protected_payload_image_ids["flaresolverr"]

    @property
    def flaresolverr_image_reference(self) -> str:
        return self.protected_bindings["flaresolverr"]

    @property
    def compose_files(self) -> tuple[Path, ...]:
        return (*PRODUCTION_COMPOSE_FILES, self.common_digest_override_path)

    @property
    def gateway_compose_files(self) -> tuple[Path, ...]:
        if self.gateway_digest_override_path is None:
            raise RuntimeError("cache admission has no paid-gateway Compose files")
        return (
            PRODUCTION_GATEWAY_COMPOSE_FILE,
            self.gateway_digest_override_path,
        )

    def evidence(self) -> dict[str, Any]:
        return {
            "admission_mode": self.admission_mode,
            "deployment_attestation_sha256": self.deployment_attestation_sha256,
            "common_digest_override_sha256": self.common_digest_override_sha256,
            "gateway_digest_override_sha256": self.gateway_digest_override_sha256,
            "release_revision": self.release_revision,
            "payload_revision": self.payload_revision,
            "provenance_manifest_sha256": self.provenance_manifest_sha256,
            "source_tree_sha256": self.source_tree_sha256,
            "protected_bindings": dict(self.protected_bindings),
            "protected_payload_image_ids": dict(
                self.protected_payload_image_ids
            ),
            "protected_config_hashes": dict(self.protected_config_hashes),
            "running_admission": json.loads(
                json.dumps(self.running_admission, sort_keys=True)
            ),
            "protected_inputs": {
                snapshot.label: snapshot.evidence()
                for snapshot in self.protected_inputs
            },
        }


@dataclass
class AdmittedWorkerRuntime:
    """Sealed worker bytes that remain immutable for the canary lifetime."""

    bundle_fd: int
    helper_fd: int
    catalog_fd: int
    python_fd: int
    unshare_fd: int
    site_packages: Path
    file_sha256: Mapping[str, str]
    bundle_sha256: str
    runtime_root: Optional[Path] = None
    source_circuit_root: Optional[Path] = None
    runtime_tree_sha256: Optional[str] = None
    runtime_tree_owner: Any = None
    execution_mode: str = "sealed-host-v1"
    container_runtime_module: Any = None
    container_runtime_module_name: Optional[str] = None
    worker_container_memory_bytes: int = 0
    worker_container_pids: int = 0
    worker_container_evidence: tuple[Mapping[str, Any], ...] = ()
    pending_runtime_tree_files: Optional[Mapping[str, bytes]] = None
    session_owner: Optional[str] = None
    runtime_cleanup_complete: bool = True

    def close(self) -> bool:
        module = self.container_runtime_module
        module_name = self.container_runtime_module_name
        self.container_runtime_module = None
        self.container_runtime_module_name = None
        if (
            module is not None
            and module_name is not None
            and sys.modules.get(module_name) is module
        ):
            del sys.modules[module_name]
        self.worker_container_memory_bytes = 0
        self.worker_container_pids = 0
        self.worker_container_evidence = ()
        self.pending_runtime_tree_files = None
        for field_name in (
            "bundle_fd",
            "helper_fd",
            "catalog_fd",
            "python_fd",
            "unshare_fd",
        ):
            descriptor = getattr(self, field_name)
            if descriptor >= 0:
                try:
                    os.close(descriptor)
                except OSError:
                    pass
                setattr(self, field_name, -1)
        owner = self.runtime_tree_owner
        self.runtime_tree_owner = None
        self.runtime_root = None
        self.source_circuit_root = None
        if owner is not None:
            try:
                owner.cleanup()
            except OSError:
                self.runtime_cleanup_complete = False
        return self.runtime_cleanup_complete


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
    containerized_workers: bool = False


@dataclass
class _SessionOwnershipLease:
    """One supervisor's private browser-session namespace and host lock."""

    owner: str
    preflight: Mapping[str, Any]
    finalize_callback: Callable[[], Mapping[str, Any]]
    close_callback: Callable[[], None]
    abort_callback: Optional[Callable[[], Mapping[str, Any]]] = None

    def finalize(self) -> Mapping[str, Any]:
        return self.finalize_callback()

    def close(self) -> None:
        self.close_callback()

    def abort_before_workers(self) -> Mapping[str, Any]:
        if self.abort_callback is not None:
            return self.abort_callback()
        return self.finalize_callback()


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
    ready_fd: Optional[int] = None


def _utc_now() -> str:
    return datetime.now(timezone.utc).isoformat()


def _safe_message(value: Any, *, limit: int = 500) -> str:
    """Bound errors and remove URLs, which can contain request context."""

    message = _URL_RE.sub("<url-redacted>", str(value or "")).replace("\x00", "")
    return message[:limit]


def _json_safe_document(value: Mapping[str, Any]) -> dict[str, Any]:
    """Return one detached JSON-safe document for the final report."""

    return json.loads(json.dumps(value, sort_keys=True, default=str))


def _canonical_json_bytes(value: Mapping[str, Any]) -> bytes:
    return json.dumps(
        value,
        ensure_ascii=False,
        sort_keys=True,
        separators=(",", ":"),
    ).encode("utf-8")


def _attach_report_sha256(value: Mapping[str, Any]) -> dict[str, Any]:
    """Hash canonical report bytes excluding the self-describing digest field."""

    report = _json_safe_document(value)
    report.pop("report_sha256", None)
    report["report_sha256"] = hashlib.sha256(
        _canonical_json_bytes(report)
    ).hexdigest()
    return report


def verify_report_sha256(value: Mapping[str, Any]) -> bool:
    """Validate one untrusted capacity receipt's content address."""

    if not isinstance(value, Mapping):
        return False
    claimed = value.get("report_sha256")
    if not isinstance(claimed, str) or re.fullmatch(r"[0-9a-f]{64}", claimed) is None:
        return False
    document = dict(value)
    document.pop("report_sha256", None)
    try:
        actual = hashlib.sha256(_canonical_json_bytes(document)).hexdigest()
    except (TypeError, ValueError):
        return False
    return secrets.compare_digest(claimed, actual)


def _absolute_evidence_path(value: Any, *, label: str) -> Path:
    if value is None:
        raise ValueError(f"{label} is required")
    path = Path(value)
    if (
        not path.is_absolute()
        or not path.name
        or path != Path(os.path.abspath(path))
        or "," in str(path)
    ):
        raise ValueError(f"{label} must be one canonical absolute path")
    return path


_CACHE_ADMISSION_VALIDATION_BRIDGE = r"""
import hashlib
import json
from pathlib import Path
import runpy
import sys

admission = runpy.run_path(
    sys.argv[1], run_name="_whoscored_cache_capacity_production_admission"
)
root = Path(sys.argv[2])
deployment_attestation_path = Path(sys.argv[5])
common_override = Path(sys.argv[6])
env_files = tuple(Path(value) for value in sys.argv[7:])
if len(env_files) != 3 or len(set(env_files)) != 3:
    raise RuntimeError("cache capacity admission requires three environment files")
admission["_assert_canonical_release"](root)
evidence = admission["validate_bindings_with_evidence"](
    root=root,
    attestation_path=Path(sys.argv[3]),
    manifest_path=Path(sys.argv[4]),
    deployment_attestation_path=deployment_attestation_path,
)
common_override_raw, common_override_identity = admission["verify_override_snapshot"](
    common_override, evidence.bindings, admission["COMMON_PROTECTED_SERVICES"]
)
protected_inputs = admission["_assert_protected_compose_inputs"](
    (
        root / "compose.yaml",
        root / "compose.seaweedfs-supervised.yaml",
        common_override,
        *env_files,
    )
)
projections, config_hashes, config_files, _rendered = admission[
    "render_attested_common_project"
](
    evidence.bindings,
    root=root,
    common_override_path=common_override,
    env_files=env_files,
    runner=admission["_run_docker"],
    protected_inputs={
        path: evidence.raw for path, evidence in protected_inputs.items()
    },
)
if set(protected_inputs) != set((*config_files, *env_files)):
    raise RuntimeError("cache capacity protected Compose inputs differ")
running_admission = admission["verify_created_containers"](
    evidence.bindings,
    project=admission["COMMON_PROJECT"],
    selected_services=admission["COMMON_PROTECTED_SERVICES"],
    projections=projections,
    config_hashes=config_hashes,
    config_files=config_files,
    env_files=env_files,
    runner=admission["_run_docker"],
    expected_state="running",
)
if any(
    record.get("image_id")
    != evidence.validated_payload_image_ids.get(record.get("service"))
    for record in running_admission.get("images", ())
):
    raise RuntimeError("cache capacity live image differs from validated payload")

document = {
    "build_attestation_identity": list(evidence.build_attestation_identity),
    "build_attestation_sha256": hashlib.sha256(evidence.build_attestation_raw).hexdigest(),
    "build_manifest_identity": list(evidence.build_manifest_identity),
    "build_manifest_sha256": hashlib.sha256(evidence.build_manifest_raw).hexdigest(),
    "deployment_attestation_identity": list(evidence.deployment_attestation_identity),
    "deployment_attestation_sha256": hashlib.sha256(
        evidence.deployment_attestation_raw
    ).hexdigest(),
    "common_digest_override_identity": list(common_override_identity),
    "common_digest_override_sha256": hashlib.sha256(common_override_raw).hexdigest(),
    "protected_bindings": dict(evidence.bindings),
    "protected_config_hashes": config_hashes,
    "protected_payload_image_ids": {
        service: evidence.validated_payload_image_ids[service]
        for service in evidence.bindings
    },
    "running_admission": running_admission,
    "payload_revision": evidence.validated_payload_revision,
    "provenance_manifest_sha256": evidence.validated_manifest_sha256,
    "release_revision": evidence.validated_release_revision,
    "source_tree_sha256": evidence.validated_source_tree_sha256,
}
sys.stdout.write(json.dumps(document, sort_keys=True, separators=(",", ":")))
"""


_ADMISSION_VALIDATION_BRIDGE = r"""
import hashlib
import json
from pathlib import Path
import runpy
import sys

admission = runpy.run_path(
    sys.argv[1], run_name="_whoscored_capacity_production_admission"
)
root = Path(sys.argv[2])
common_override = Path(sys.argv[6])
gateway_override = Path(sys.argv[7])
provider_policy_path = Path(sys.argv[8])
owner_secret_path = Path(sys.argv[9])
deployment_admission_receipt_path = Path(sys.argv[10])
env_files = tuple(Path(value) for value in sys.argv[11:])
if len(env_files) != 3 or len(set(env_files)) != 3:
    raise RuntimeError("capacity admission requires exactly three environment files")
admission["_assert_canonical_release"](root)
evidence = admission["validate_bindings_with_evidence"](
    root=root,
    attestation_path=Path(sys.argv[3]),
    manifest_path=Path(sys.argv[4]),
    deployment_attestation_path=Path(sys.argv[5]),
)
common_override_raw, common_override_identity = admission["verify_override_snapshot"](
    common_override, evidence.bindings, admission["COMMON_PROTECTED_SERVICES"]
)
gateway_override_raw, gateway_override_identity = admission["verify_override_snapshot"](
    gateway_override, evidence.bindings, admission["GATEWAY_PROTECTED_SERVICES"]
)
provider_policy = admission["validate_provider_policy"](
    provider_policy_path, owner_secret_path=owner_secret_path
)
deployment_admission_receipt = admission[
    "validate_deployment_admission_receipt"
](
    deployment_admission_receipt_path,
    deployment_attestation_path=Path(sys.argv[5]),
    provider_policy=provider_policy,
)
provider_authority = {
    **provider_policy,
    "provider_policy_sha256": provider_policy["document_sha256"],
}
protected_inputs = admission["_assert_protected_compose_inputs"](
    (
        root / "compose.yaml",
        root / "compose.seaweedfs-supervised.yaml",
        root / "deploy/whoscored/gateway.compose.yaml",
        common_override,
        gateway_override,
        *env_files,
    )
)
projections, config_hashes, config_files, _rendered = admission[
    "render_attested_projects"
](
    evidence.bindings,
    root=root,
    common_override_path=common_override,
    gateway_override_path=gateway_override,
    env_files=env_files,
    provider_authority=provider_authority,
    runner=admission["_run_docker"],
    protected_inputs=protected_inputs,
)
running_admission = admission["verify_created_containers"](
    evidence.bindings,
    project=admission["_SERVICE_PROJECT"],
    selected_services=("airflow-scheduler", "flaresolverr"),
    projections=projections,
    config_hashes=config_hashes,
    config_files=config_files,
    env_files=env_files,
    runner=admission["_run_docker"],
    expected_state="running",
)
document = {
    "build_attestation_identity": list(evidence.build_attestation_identity),
    "build_attestation_sha256": hashlib.sha256(
        evidence.build_attestation_raw
    ).hexdigest(),
    "build_manifest_identity": list(evidence.build_manifest_identity),
    "build_manifest_sha256": hashlib.sha256(evidence.build_manifest_raw).hexdigest(),
    "deployment_attestation_identity": list(
        evidence.deployment_attestation_identity
    ),
    "deployment_attestation_sha256": hashlib.sha256(
        evidence.deployment_attestation_raw
    ).hexdigest(),
    "common_digest_override_identity": list(common_override_identity),
    "common_digest_override_sha256": hashlib.sha256(common_override_raw).hexdigest(),
    "gateway_digest_override_identity": list(gateway_override_identity),
    "gateway_digest_override_sha256": hashlib.sha256(gateway_override_raw).hexdigest(),
    "deployment_admission_receipt": deployment_admission_receipt,
    "provider_policy": provider_policy,
    "protected_bindings": dict(evidence.bindings),
    "protected_config_hashes": dict(config_hashes),
    "protected_payload_image_ids": {
        service: evidence.validated_payload_image_ids[service]
        for service in evidence.bindings
    },
    "running_admission": running_admission,
    "payload_revision": evidence.validated_payload_revision,
    "provenance_manifest_sha256": evidence.validated_manifest_sha256,
    "release_revision": evidence.validated_release_revision,
    "source_tree_sha256": evidence.validated_source_tree_sha256,
}
sys.stdout.write(json.dumps(document, sort_keys=True, separators=(",", ":")))
"""


def _validate_cache_production_deployment(
    args: argparse.Namespace,
) -> ProductionDeployment:
    """Admit ready common runtime without any paid-provider authority."""

    deployment_attestation = _absolute_evidence_path(
        getattr(args, "deployment_attestation", None),
        label="deployment attestation",
    )
    common_digest_override = _absolute_evidence_path(
        getattr(args, "common_digest_override", None),
        label="common digest override",
    )
    input_specs = (
        ("build-attestation", PRODUCTION_BUILD_ATTESTATION, False),
        ("build-manifest", PRODUCTION_BUILD_MANIFEST, False),
        ("deployment-attestation", deployment_attestation, True),
        ("compose:compose.yaml", PRODUCTION_COMPOSE_FILES[0], False),
        (
            "compose:compose.seaweedfs-supervised.yaml",
            PRODUCTION_COMPOSE_FILES[1],
            False,
        ),
        ("common-digest-override", common_digest_override, True),
        *(
            (f"compose-env:{index}", path, True)
            for index, path in enumerate(PRODUCTION_COMPOSE_ENV_FILES)
        ),
    )
    if (
        len({label for label, _path, _private in input_specs}) != len(input_specs)
        or len({path for _label, path, _private in input_specs}) != len(input_specs)
    ):
        raise RuntimeError("cache deployment inputs are duplicated")
    before_snapshots = tuple(
        _protected_input_snapshot(path, label=label, private=private)
        for label, path, private in input_specs
    )
    command = [
        str(PRODUCTION_ADMISSION_PYTHON),
        "-I",
        "-S",
        "-c",
        _CACHE_ADMISSION_VALIDATION_BRIDGE,
        str(PRODUCTION_ADMISSION_SCRIPT),
        str(REPO_ROOT),
        str(PRODUCTION_BUILD_ATTESTATION),
        str(PRODUCTION_BUILD_MANIFEST),
        str(deployment_attestation),
        str(common_digest_override),
        *(str(path) for path in PRODUCTION_COMPOSE_ENV_FILES),
    ]
    try:
        result = subprocess.run(
            command,
            cwd=REPO_ROOT,
            env={
                "HOME": "/nonexistent",
                "PATH": "/usr/bin:/bin",
                "LANG": "C.UTF-8",
                "LC_ALL": "C.UTF-8",
            },
            check=False,
            capture_output=True,
            text=True,
            timeout=180,
        )
    except (OSError, subprocess.SubprocessError) as exc:
        raise RuntimeError("cannot validate cache ready-v1 deployment") from exc
    if result.returncode != 0:
        detail = _safe_message(result.stderr.strip() or "validation failed")
        raise RuntimeError(f"cache ready-v1 deployment is invalid: {detail}")
    if len(result.stdout.encode("utf-8")) > 8192:
        raise RuntimeError("cache deployment validation output is too large")
    try:
        document = json.loads(result.stdout)
    except json.JSONDecodeError as exc:
        raise RuntimeError("cache deployment validation returned invalid JSON") from exc
    expected_fields = {
        "build_attestation_identity",
        "build_attestation_sha256",
        "build_manifest_identity",
        "build_manifest_sha256",
        "deployment_attestation_identity",
        "deployment_attestation_sha256",
        "common_digest_override_identity",
        "common_digest_override_sha256",
        "protected_bindings",
        "protected_config_hashes",
        "protected_payload_image_ids",
        "running_admission",
        "payload_revision",
        "provenance_manifest_sha256",
        "release_revision",
        "source_tree_sha256",
    }
    if not isinstance(document, dict) or set(document) != expected_fields:
        raise RuntimeError("cache deployment validation has an invalid shape")
    after_snapshots = tuple(
        _protected_input_snapshot(path, label=label, private=private)
        for label, path, private in input_specs
    )
    if after_snapshots != before_snapshots:
        raise RuntimeError("cache deployment inputs changed during validation")
    snapshots = {snapshot.label: snapshot for snapshot in before_snapshots}

    def bridge_identity(field: str) -> tuple[int, ...]:
        value = document.get(field)
        if (
            type(value) is not list
            or len(value) != len(_PROTECTED_INPUT_IDENTITY_FIELDS)
            or any(type(item) is not int or item < 0 for item in value)
        ):
            raise RuntimeError("cache deployment returned an invalid input identity")
        return tuple(value)

    for label, identity_field, digest_field in (
        ("build-attestation", "build_attestation_identity", "build_attestation_sha256"),
        ("build-manifest", "build_manifest_identity", "build_manifest_sha256"),
        (
            "deployment-attestation",
            "deployment_attestation_identity",
            "deployment_attestation_sha256",
        ),
        (
            "common-digest-override",
            "common_digest_override_identity",
            "common_digest_override_sha256",
        ),
    ):
        if (
            bridge_identity(identity_field) != snapshots[label].identity
            or document.get(digest_field) != snapshots[label].sha256
        ):
            raise RuntimeError("cache validated bytes differ from their snapshot")

    protected_bindings = document.get("protected_bindings")
    protected_payloads = document.get("protected_payload_image_ids")
    config_hashes = document.get("protected_config_hashes")
    running = document.get("running_admission")
    protected_set = set(PROTECTED_PRODUCTION_SERVICES)
    common_set = set(ADMITTED_RUNNING_SERVICES)
    digests = (
        document.get("deployment_attestation_sha256"),
        document.get("common_digest_override_sha256"),
        document.get("provenance_manifest_sha256"),
        document.get("source_tree_sha256"),
    )
    revisions = (
        document.get("release_revision"),
        document.get("payload_revision"),
    )
    if (
        any(
            not isinstance(value, str)
            or re.fullmatch(r"[0-9a-f]{64}", value) is None
            for value in digests
        )
        or any(
            not isinstance(value, str)
            or re.fullmatch(r"[0-9a-f]{40}", value) is None
            for value in revisions
        )
        or document.get("provenance_manifest_sha256")
        != snapshots["build-manifest"].sha256
        or not isinstance(protected_bindings, dict)
        or set(protected_bindings) != protected_set
        or any(
            not isinstance(value, str) or _PINNED_IMAGE_RE.fullmatch(value) is None
            for value in protected_bindings.values()
        )
        or not isinstance(protected_payloads, dict)
        or set(protected_payloads) != protected_set
        or any(
            not isinstance(value, str)
            or re.fullmatch(r"sha256:[0-9a-f]{64}", value) is None
            for value in protected_payloads.values()
        )
        or not isinstance(config_hashes, dict)
        or set(config_hashes) != common_set
        or any(
            not isinstance(value, str) or re.fullmatch(r"[0-9a-f]{64}", value) is None
            for value in config_hashes.values()
        )
        or not isinstance(running, dict)
        or set(running)
        != {
            "apparmor_profile",
            "docker_security_options",
            "images",
            "networks",
            "projects",
            "schema_version",
            "status",
            "volumes",
        }
        or running.get("schema_version") != 1
        or running.get("status") != "admitted-running-v1"
        or running.get("projects")
        != {"data-platform": list(ADMITTED_RUNNING_SERVICES)}
    ):
        raise RuntimeError("cache deployment validation returned invalid bindings")
    running_images = running.get("images")
    if not isinstance(running_images, list) or len(running_images) != len(common_set):
        raise RuntimeError("cache running admission has an invalid image set")
    for service, record in zip(ADMITTED_RUNNING_SERVICES, running_images):
        if (
            not isinstance(record, dict)
            or set(record) != {"container_id", "final_image", "image_id", "service"}
            or record.get("service") != service
            or record.get("final_image") != protected_bindings[service]
            or not isinstance(record.get("container_id"), str)
            or re.fullmatch(r"[0-9a-f]{64}", record["container_id"]) is None
            or not isinstance(record.get("image_id"), str)
            or re.fullmatch(r"sha256:[0-9a-f]{64}", record["image_id"]) is None
            or record.get("image_id") != protected_payloads[service]
        ):
            raise RuntimeError("cache running admission has invalid image identity")
    return ProductionDeployment(
        deployment_attestation_path=deployment_attestation,
        deployment_attestation_sha256=str(digests[0]),
        deployment_attestation_identity=snapshots["deployment-attestation"].identity,
        common_digest_override_path=common_digest_override,
        common_digest_override_sha256=str(digests[1]),
        common_digest_override_identity=snapshots["common-digest-override"].identity,
        gateway_digest_override_path=None,
        gateway_digest_override_sha256=None,
        gateway_digest_override_identity=None,
        provider_policy_path=None,
        owner_secret_file_path=None,
        deployment_admission_receipt_path=None,
        release_revision=str(revisions[0]),
        payload_revision=str(revisions[1]),
        provenance_manifest_sha256=str(digests[2]),
        source_tree_sha256=str(digests[3]),
        protected_bindings=dict(protected_bindings),
        protected_payload_image_ids=dict(protected_payloads),
        protected_config_hashes=dict(config_hashes),
        running_admission=_json_safe_document(running),
        protected_inputs=before_snapshots,
        admission_mode=CACHE_CAPACITY_MODE,
    )


def _validate_production_deployment(args: argparse.Namespace) -> ProductionDeployment:
    """Validate ready-v1 provenance and both exact Compose projects."""

    if _capacity_mode(args) == CACHE_CAPACITY_MODE:
        return _validate_cache_production_deployment(args)

    deployment_attestation = _absolute_evidence_path(
        getattr(args, "deployment_attestation", None),
        label="deployment attestation",
    )
    common_digest_override = _absolute_evidence_path(
        getattr(args, "common_digest_override", None),
        label="common digest override",
    )
    gateway_digest_override = _absolute_evidence_path(
        getattr(args, "gateway_digest_override", None),
        label="gateway digest override",
    )
    provider_policy = _absolute_evidence_path(
        getattr(args, "provider_policy", None),
        label="provider policy",
    )
    owner_secret_file = _absolute_evidence_path(
        getattr(args, "owner_secret_file", None),
        label="provider-policy owner key",
    )
    deployment_admission_receipt = _absolute_evidence_path(
        getattr(args, "deployment_admission_receipt", None),
        label="deployment admission receipt",
    )
    input_specs = (
        ("build-attestation", PRODUCTION_BUILD_ATTESTATION, False),
        ("build-manifest", PRODUCTION_BUILD_MANIFEST, False),
        ("deployment-attestation", deployment_attestation, True),
        ("compose:compose.yaml", PRODUCTION_COMPOSE_FILES[0], False),
        (
            "compose:compose.seaweedfs-supervised.yaml",
            PRODUCTION_COMPOSE_FILES[1],
            False,
        ),
        (
            "compose:gateway.compose.yaml",
            PRODUCTION_GATEWAY_COMPOSE_FILE,
            False,
        ),
        ("common-digest-override", common_digest_override, True),
        ("gateway-digest-override", gateway_digest_override, True),
        ("provider-policy", provider_policy, True),
        ("provider-policy-owner-key", owner_secret_file, True),
        (
            "deployment-admission-receipt",
            deployment_admission_receipt,
            True,
        ),
        *(
            (f"compose-env:{index}", path, True)
            for index, path in enumerate(PRODUCTION_COMPOSE_ENV_FILES)
        ),
    )
    if (
        len({label for label, _path, _private in input_specs}) != len(input_specs)
        or len({path for _label, path, _private in input_specs}) != len(input_specs)
    ):
        raise RuntimeError("production deployment inputs are duplicated")
    before_snapshots = tuple(
        _protected_input_snapshot(path, label=label, private=private)
        for label, path, private in input_specs
    )
    command = [
        str(PRODUCTION_ADMISSION_PYTHON),
        "-I",
        "-S",
        "-c",
        _ADMISSION_VALIDATION_BRIDGE,
        str(PRODUCTION_ADMISSION_SCRIPT),
        str(REPO_ROOT),
        str(PRODUCTION_BUILD_ATTESTATION),
        str(PRODUCTION_BUILD_MANIFEST),
        str(deployment_attestation),
        str(common_digest_override),
        str(gateway_digest_override),
        str(provider_policy),
        str(owner_secret_file),
        str(deployment_admission_receipt),
        *(str(path) for path in PRODUCTION_COMPOSE_ENV_FILES),
    ]
    environment = {
        "HOME": "/nonexistent",
        "PATH": "/usr/bin:/bin",
        "LANG": "C.UTF-8",
        "LC_ALL": "C.UTF-8",
    }
    try:
        result = subprocess.run(
            command,
            cwd=REPO_ROOT,
            env=environment,
            check=False,
            capture_output=True,
            text=True,
            timeout=180,
        )
    except (OSError, subprocess.SubprocessError) as exc:
        raise RuntimeError("cannot validate ready-v1 production deployment") from exc
    if result.returncode != 0:
        detail = _safe_message(result.stderr.strip() or "validation failed")
        raise RuntimeError(f"ready-v1 production deployment is invalid: {detail}")
    if len(result.stdout.encode("utf-8")) > 8192:
        raise RuntimeError("ready-v1 deployment validation output is too large")
    try:
        document = json.loads(result.stdout)
    except json.JSONDecodeError as exc:
        raise RuntimeError(
            "ready-v1 deployment validation returned invalid JSON"
        ) from exc
    expected_fields = {
        "build_attestation_identity",
        "build_attestation_sha256",
        "build_manifest_identity",
        "build_manifest_sha256",
        "deployment_attestation_identity",
        "deployment_attestation_sha256",
        "common_digest_override_identity",
        "common_digest_override_sha256",
        "gateway_digest_override_identity",
        "gateway_digest_override_sha256",
        "deployment_admission_receipt",
        "provider_policy",
        "protected_bindings",
        "protected_config_hashes",
        "protected_payload_image_ids",
        "running_admission",
        "payload_revision",
        "provenance_manifest_sha256",
        "release_revision",
        "source_tree_sha256",
    }
    if not isinstance(document, dict) or set(document) != expected_fields:
        raise RuntimeError("ready-v1 deployment validation has an invalid shape")
    after_snapshots = tuple(
        _protected_input_snapshot(path, label=label, private=private)
        for label, path, private in input_specs
    )
    if after_snapshots != before_snapshots:
        raise RuntimeError("production deployment inputs changed during validation")
    snapshots_by_label = {
        snapshot.label: snapshot for snapshot in before_snapshots
    }

    def bridge_identity(name: str) -> tuple[int, ...]:
        value = document.get(name)
        if (
            type(value) is not list
            or len(value) != len(_PROTECTED_INPUT_IDENTITY_FIELDS)
            or any(type(item) is not int or item < 0 for item in value)
        ):
            raise RuntimeError(
                "ready-v1 deployment validation returned invalid bindings"
            )
        return tuple(value)

    bridge_snapshots = (
        (
            "build-attestation",
            "build_attestation_identity",
            "build_attestation_sha256",
        ),
        (
            "build-manifest",
            "build_manifest_identity",
            "build_manifest_sha256",
        ),
        (
            "deployment-attestation",
            "deployment_attestation_identity",
            "deployment_attestation_sha256",
        ),
        (
            "common-digest-override",
            "common_digest_override_identity",
            "common_digest_override_sha256",
        ),
        (
            "gateway-digest-override",
            "gateway_digest_override_identity",
            "gateway_digest_override_sha256",
        ),
    )
    for label, identity_field, digest_field in bridge_snapshots:
        snapshot = snapshots_by_label[label]
        if (
            bridge_identity(identity_field) != snapshot.identity
            or document.get(digest_field) != snapshot.sha256
        ):
            raise RuntimeError(
                "ready-v1 validated bytes differ from the admitted snapshot"
            )
    deployment_sha256 = document.get("deployment_attestation_sha256")
    common_override_sha256 = document.get("common_digest_override_sha256")
    gateway_override_sha256 = document.get("gateway_digest_override_sha256")
    protected_bindings = document.get("protected_bindings")
    protected_payloads = document.get("protected_payload_image_ids")
    protected_config_hashes = document.get("protected_config_hashes")
    running_admission = document.get("running_admission")
    provider_policy_projection = document.get("provider_policy")
    deployment_admission_projection = document.get(
        "deployment_admission_receipt"
    )
    release_revision = document.get("release_revision")
    payload_revision = document.get("payload_revision")
    manifest_sha256 = document.get("provenance_manifest_sha256")
    source_tree_sha256 = document.get("source_tree_sha256")
    if (
        not isinstance(deployment_sha256, str)
        or re.fullmatch(r"[0-9a-f]{64}", deployment_sha256) is None
        or not isinstance(common_override_sha256, str)
        or re.fullmatch(r"[0-9a-f]{64}", common_override_sha256) is None
        or not isinstance(gateway_override_sha256, str)
        or re.fullmatch(r"[0-9a-f]{64}", gateway_override_sha256) is None
        or not isinstance(release_revision, str)
        or re.fullmatch(r"[0-9a-f]{40}", release_revision) is None
        or not isinstance(payload_revision, str)
        or re.fullmatch(r"[0-9a-f]{40}", payload_revision) is None
        or not isinstance(manifest_sha256, str)
        or re.fullmatch(r"[0-9a-f]{64}", manifest_sha256) is None
        or not isinstance(source_tree_sha256, str)
        or re.fullmatch(r"[0-9a-f]{64}", source_tree_sha256) is None
        or manifest_sha256 != snapshots_by_label["build-manifest"].sha256
        or not isinstance(provider_policy_projection, dict)
        or provider_policy_projection.get("policy_path") != str(provider_policy)
        or not isinstance(
            provider_policy_projection.get("document_sha256"), str
        )
        or re.fullmatch(
            r"[0-9a-f]{64}",
            str(provider_policy_projection.get("document_sha256")),
        )
        is None
        or deployment_admission_projection
        != {
            "path": str(deployment_admission_receipt),
            "sha256": snapshots_by_label[
                "deployment-admission-receipt"
            ].sha256,
        }
    ):
        raise RuntimeError("ready-v1 deployment validation returned invalid bindings")
    protected_set = set(PROTECTED_PRODUCTION_SERVICES)
    if (
        not isinstance(protected_bindings, dict)
        or set(protected_bindings) != protected_set
        or any(
            not isinstance(value, str)
            or _PINNED_IMAGE_RE.fullmatch(value) is None
            for value in protected_bindings.values()
        )
        or not isinstance(protected_payloads, dict)
        or set(protected_payloads) != protected_set
        or any(
            not isinstance(value, str)
            or re.fullmatch(r"sha256:[0-9a-f]{64}", value) is None
            for value in protected_payloads.values()
        )
        or not isinstance(protected_config_hashes, dict)
        or set(protected_config_hashes) != protected_set
        or any(
            not isinstance(value, str)
            or re.fullmatch(r"[0-9a-f]{64}", value) is None
            for value in protected_config_hashes.values()
        )
        or not isinstance(running_admission, dict)
        or set(running_admission)
        != {
            "apparmor_profile",
            "docker_security_options",
            "images",
            "networks",
            "projects",
            "schema_version",
            "status",
            "volumes",
        }
        or running_admission.get("schema_version") != 2
        or running_admission.get("status") != "admitted-running-v1"
        or running_admission.get("projects")
        != {
            "data-platform": ["airflow-scheduler", "flaresolverr"],
            "whoscored-gw": [
                "flaresolverr_whoscored_paid",
                "whoscored_paid_gateway",
                "whoscored_proxy_filter",
            ],
        }
        or running_admission.get("apparmor_profile")
        != "docker-default (enforce)"
        or not isinstance(running_admission.get("docker_security_options"), list)
        or not isinstance(running_admission.get("networks"), list)
        or not isinstance(running_admission.get("volumes"), list)
    ):
        raise RuntimeError("ready-v1 deployment validation returned invalid bindings")
    running_images = running_admission.get("images")
    if (
        not isinstance(running_images, list)
        or len(running_images) != len(ADMITTED_RUNNING_SERVICES)
    ):
        raise RuntimeError("ready-v1 running admission has an invalid image set")
    for service, record in zip(ADMITTED_RUNNING_SERVICES, running_images):
        if (
            not isinstance(record, dict)
            or set(record)
            != {"container_id", "final_image", "image_id", "service"}
            or record.get("service") != service
            or record.get("final_image") != protected_bindings[service]
            or not isinstance(record.get("container_id"), str)
            or re.fullmatch(r"[0-9a-f]{64}", record["container_id"]) is None
            or not isinstance(record.get("image_id"), str)
            or re.fullmatch(r"sha256:[0-9a-f]{64}", record["image_id"])
            is None
        ):
            raise RuntimeError("ready-v1 running admission has invalid image identity")
    return ProductionDeployment(
        deployment_attestation_path=deployment_attestation,
        deployment_attestation_sha256=deployment_sha256,
        deployment_attestation_identity=snapshots_by_label[
            "deployment-attestation"
        ].identity,
        common_digest_override_path=common_digest_override,
        common_digest_override_sha256=common_override_sha256,
        common_digest_override_identity=snapshots_by_label[
            "common-digest-override"
        ].identity,
        gateway_digest_override_path=gateway_digest_override,
        gateway_digest_override_sha256=gateway_override_sha256,
        gateway_digest_override_identity=snapshots_by_label[
            "gateway-digest-override"
        ].identity,
        provider_policy_path=provider_policy,
        owner_secret_file_path=owner_secret_file,
        deployment_admission_receipt_path=deployment_admission_receipt,
        release_revision=release_revision,
        payload_revision=payload_revision,
        provenance_manifest_sha256=manifest_sha256,
        source_tree_sha256=source_tree_sha256,
        protected_bindings=dict(protected_bindings),
        protected_payload_image_ids=dict(protected_payloads),
        protected_config_hashes=dict(protected_config_hashes),
        running_admission=_json_safe_document(running_admission),
        protected_inputs=before_snapshots,
    )


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
        python_executable = Path(sys.executable).resolve(strict=True)
        _read_admitted_worker_bytes(
            python_executable, label="worker Python interpreter"
        )
        unshare_sha256 = hashlib.sha256(
            _read_admitted_worker_bytes(
                WORKER_NAMESPACE_EXECUTABLE,
                label="worker PID-namespace helper",
            )
        ).hexdigest()
    except OSError:
        return "capacity worker PID-namespace helper is unavailable"
    except RuntimeError:
        return "capacity worker executable metadata is invalid"
    if unshare_sha256 != REQUIRED_UNSHARE_SHA256:
        return "capacity worker PID-namespace helper hash mismatch"
    try:
        version_result = subprocess.run(
            [str(WORKER_NAMESPACE_EXECUTABLE), "--version"],
            check=False,
            capture_output=True,
            text=True,
            timeout=3,
            env={
                "HOME": "/nonexistent",
                "LANG": "C.UTF-8",
                "LC_ALL": "C.UTF-8",
                "PATH": "/usr/bin:/bin",
            },
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
            env={
                "HOME": "/nonexistent",
                "LANG": "C.UTF-8",
                "LC_ALL": "C.UTF-8",
                "PATH": "/usr/bin:/bin",
            },
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


def _pinned_worker_exec_preflight(runtime: AdmittedWorkerRuntime) -> Optional[str]:
    """Exercise the exact sealed Python and unshare bytes used by workers."""

    descriptors = (runtime.python_fd, runtime.unshare_fd)
    required_seals = (
        fcntl.F_SEAL_WRITE
        | fcntl.F_SEAL_GROW
        | fcntl.F_SEAL_SHRINK
        | fcntl.F_SEAL_SEAL
    )
    try:
        for descriptor in descriptors:
            metadata = os.fstat(descriptor)
            if (
                not stat.S_ISREG(metadata.st_mode)
                or metadata.st_mode & 0o111 == 0
                or fcntl.fcntl(descriptor, fcntl.F_GET_SEALS) != required_seals
            ):
                return "sealed capacity worker executable metadata is invalid"
        python_path = f"/proc/self/fd/{runtime.python_fd}"
        unshare_path = f"/proc/self/fd/{runtime.unshare_fd}"
        environment = {
            "HOME": "/nonexistent",
            "LANG": "C.UTF-8",
            "LC_ALL": "C.UTF-8",
            "PATH": "/usr/bin:/bin",
        }
        version_result = subprocess.run(
            [unshare_path, "--version"],
            check=False,
            capture_output=True,
            text=True,
            timeout=3,
            env=environment,
            pass_fds=descriptors,
        )
        capability_result = subprocess.run(
            [
                unshare_path,
                "--pid",
                "--fork",
                "--kill-child=SIGKILL",
                "--",
                python_path,
                "-I",
                "-S",
                "-c",
                "pass",
            ],
            check=False,
            capture_output=True,
            text=True,
            timeout=3,
            env=environment,
            pass_fds=descriptors,
        )
    except (OSError, subprocess.SubprocessError):
        return "sealed capacity worker executable is unavailable"
    if (
        version_result.returncode != 0
        or version_result.stdout.strip() != REQUIRED_UNSHARE_VERSION
    ):
        return "sealed capacity worker PID-namespace helper version mismatch"
    if capability_result.returncode != 0:
        return "sealed capacity worker PID namespace is unavailable"
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
    combined["stale_worker_cleanup_required"] = any(
        result.get("stale_worker_cleanup_required") is True
        for result in results
    )
    combined["stale_worker_cleanup_verified"] = all(
        result.get("stale_worker_cleanup_verified") is True
        for result in results
        if result.get("stale_worker_cleanup_required") is True
    )
    combined["stale_worker_containers_removed"] = sum(
        int(result.get("stale_worker_containers_removed", 0))
        for result in results
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


def _state_entry_exists(path: Path) -> bool:
    """Return whether the exact path entry exists, including a dangling link."""

    try:
        path.lstat()
    except FileNotFoundError:
        return False
    return True


def _acquire_supervisor_lock(path: Path) -> int:
    flags = os.O_RDWR | os.O_CREAT | getattr(os, "O_CLOEXEC", 0)
    if hasattr(os, "O_NOFOLLOW"):
        flags |= os.O_NOFOLLOW
    descriptor = os.open(path, flags, 0o600)
    try:
        os.fchmod(descriptor, 0o600)
        metadata = os.fstat(descriptor)
        if (
            not stat.S_ISREG(metadata.st_mode)
            or metadata.st_uid != 0
            or metadata.st_gid != 0
            or metadata.st_nlink != 1
            or stat.S_IMODE(metadata.st_mode) != 0o600
        ):
            raise RuntimeError("capacity supervisor lock metadata is invalid")
        fcntl.flock(descriptor, fcntl.LOCK_EX | fcntl.LOCK_NB)
    except BaseException:
        os.close(descriptor)
        raise
    return descriptor


def _validated_worker_image_id(value: object) -> Optional[str]:
    if value is None:
        return None
    if not isinstance(value, str) or re.fullmatch(
        r"sha256:[0-9a-f]{64}", value
    ) is None:
        raise ValueError("worker image identity is invalid")
    return value


def _read_owner_state(path: Path) -> tuple[str, str, Optional[str]]:
    flags = os.O_RDONLY
    if hasattr(os, "O_NOFOLLOW"):
        flags |= os.O_NOFOLLOW
    descriptor = os.open(path, flags)
    try:
        metadata = os.fstat(descriptor)
        if (
            not stat.S_ISREG(metadata.st_mode)
            or metadata.st_uid != 0
            or metadata.st_gid != 0
            or metadata.st_nlink != 1
            or stat.S_IMODE(metadata.st_mode) != 0o600
            or metadata.st_size > 1024
        ):
            raise ValueError("ownership state metadata is invalid")
        payload = os.read(descriptor, 1025)
    finally:
        os.close(descriptor)
    try:
        parsed = json.loads(payload.decode("utf-8"))
    except (UnicodeDecodeError, json.JSONDecodeError) as exc:
        raise ValueError("ownership state is invalid") from exc
    if not isinstance(parsed, Mapping):
        raise ValueError("ownership state shape is invalid")
    schema_version = parsed.get("schema_version")
    if type(schema_version) is not int or schema_version not in {1, 2}:
        raise ValueError("ownership state schema version is invalid")
    expected_fields = {
        "schema_version",
        "owner",
        "flaresolverr_endpoint",
        *({"worker_image_id"} if schema_version == 2 else set()),
    }
    if set(parsed) != expected_fields:
        raise ValueError("ownership state shape is invalid")
    owner = _validate_session_owner(parsed["owner"])
    endpoint = _canonical_flaresolverr_endpoint(parsed["flaresolverr_endpoint"])
    worker_image_id = (
        _validated_worker_image_id(parsed["worker_image_id"])
        if schema_version == 2
        else None
    )
    return owner, endpoint, worker_image_id


def _fsync_directory(path: Path) -> None:
    descriptor = os.open(path, os.O_RDONLY)
    try:
        os.fsync(descriptor)
    finally:
        os.close(descriptor)


def _write_owner_state(
    path: Path,
    owner: str,
    flaresolverr_endpoint: str,
    worker_image_id: Optional[str] = None,
) -> None:
    owner = _validate_session_owner(owner)
    endpoint = _canonical_flaresolverr_endpoint(flaresolverr_endpoint)
    worker_image_id = _validated_worker_image_id(worker_image_id)
    payload = json.dumps(
        {
            "schema_version": _OWNER_STATE_SCHEMA_VERSION,
            "owner": owner,
            "flaresolverr_endpoint": endpoint,
            "worker_image_id": worker_image_id,
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
        temporary.unlink()
        metadata = path.lstat()
        if (
            not stat.S_ISREG(metadata.st_mode)
            or metadata.st_uid != 0
            or metadata.st_gid != 0
            or metadata.st_nlink != 1
            or stat.S_IMODE(metadata.st_mode) != 0o600
            or metadata.st_size != len(payload)
        ):
            raise RuntimeError("ownership state metadata is invalid")
        _fsync_directory(path.parent)
    finally:
        temporary.unlink(missing_ok=True)


def _remove_owner_state(
    path: Path,
    owner: str,
    flaresolverr_endpoint: str,
    worker_image_id: Optional[str],
) -> bool:
    current_owner, current_endpoint, current_worker_image_id = _read_owner_state(
        path
    )
    if (
        current_owner != _validate_session_owner(owner)
        or current_endpoint
        != _canonical_flaresolverr_endpoint(flaresolverr_endpoint)
        or current_worker_image_id
        != _validated_worker_image_id(worker_image_id)
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
        "stale_worker_cleanup_required": bool(
            preflight.get("stale_worker_cleanup_required")
        ),
        "stale_worker_cleanup_verified": bool(
            preflight.get(
                "stale_worker_cleanup_verified",
                not preflight.get("stale_worker_cleanup_required"),
            )
        ),
        "stale_worker_containers_removed": int(
            preflight.get("stale_worker_containers_removed", 0)
        ),
        "worker_artifact_cleanup_required": bool(
            final.get("worker_artifact_cleanup_required")
        ),
        "worker_artifact_cleanup_verified": bool(
            final.get(
                "worker_artifact_cleanup_verified",
                not final.get("worker_artifact_cleanup_required"),
            )
        ),
    }


def _prepare_session_ownership(
    args: argparse.Namespace,
    *,
    monotonic: Callable[[], float],
    sleep: Callable[[float], None],
    worker_image_id: Optional[str] = None,
    cleanup_stale_workers: Optional[
        Callable[[str, Optional[str]], Sequence[str]]
    ] = None,
    finalize_worker_artifacts: Optional[Callable[[str], bool]] = None,
    browser_network_control: bool = True,
) -> _SessionOwnershipLease:
    """Lock the host, clean stale ownership, then persist a new unique owner."""

    current_endpoint = _canonical_flaresolverr_endpoint(args.flaresolverr_url)
    if current_endpoint != REQUIRED_FLARESOLVERR_ENDPOINT:
        raise ValueError("FlareSolverr endpoint does not match production binding")
    worker_image_id = _validated_worker_image_id(worker_image_id)
    lock_path, direct_state_path = _state_paths(args)
    cache_ownership = not browser_network_control
    state_path = direct_state_path
    if cache_ownership:
        # Cache workers have a distinct crash-recovery record.  Never consume
        # or overwrite a diagnostic owner record: it may still name browser
        # sessions which only a later explicit diagnostic cleanup may sweep.
        state_path = state_path.with_name(f"{state_path.name}.cache")
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
        # The shared flock prevents a supervised direct run from creating this
        # record after the check.  Cache mode must not read, repair, or sweep a
        # retained direct owner because only the explicit diagnostic path has
        # browser lifecycle authority.  Even an invalid or dangling entry is a
        # fail-closed recovery boundary.
        if cache_ownership and _state_entry_exists(direct_state_path):
            preflight = _empty_cleanup_result(required=True, verified=False)
            evidence = _ownership_evidence(
                lock_acquired=True,
                preflight=preflight,
                final=_empty_cleanup_result(required=False, verified=True),
                state_file_removed=False,
            )
            raise _SessionOwnershipPreparationError(
                "direct diagnostic ownership state blocks cache capacity startup",
                evidence,
            )

        if state_path.exists():
            try:
                stale_owner, stale_endpoint, stale_worker_image_id = (
                    _read_owner_state(state_path)
                )
            except Exception as exc:
                preflight = _empty_cleanup_result(required=True, verified=False)
                preflight["error_count"] = 1
                preflight["error_sha256"] = [
                    _cleanup_error_hash("state_read", exc)
                ]
            else:
                preflight = _empty_cleanup_result(required=True, verified=True)
                preflight["stale_worker_cleanup_required"] = (
                    cleanup_stale_workers is not None
                )
                preflight["stale_worker_cleanup_verified"] = (
                    cleanup_stale_workers is None
                )
                if cleanup_stale_workers is not None:
                    try:
                        removed_workers = tuple(
                            cleanup_stale_workers(
                                stale_owner, stale_worker_image_id
                            )
                        )
                        if any(
                            not isinstance(container_id, str)
                            or re.fullmatch(r"[0-9a-f]{64}", container_id)
                            is None
                            for container_id in removed_workers
                        ):
                            raise RuntimeError(
                                "stale worker cleanup returned invalid evidence"
                            )
                        preflight["stale_worker_containers_removed"] = len(
                            removed_workers
                        )
                        preflight["stale_worker_cleanup_verified"] = True
                    except Exception as exc:
                        preflight["verified_zero"] = False
                        preflight["error_count"] += 1
                        preflight["error_sha256"] = [
                            _cleanup_error_hash("stale_workers", exc)
                        ]
                if preflight["verified_zero"] and browser_network_control:
                    browser_cleanup = _sweep_owned_browser_sessions(
                        flaresolverr_url=stale_endpoint,
                        owner=stale_owner,
                        stale=True,
                        monotonic=monotonic,
                        sleep=sleep,
                    )
                    preflight = _combine_cleanup_results(
                        preflight, browser_cleanup
                    )
                if preflight["verified_zero"]:
                    try:
                        removed = _remove_owner_state(
                            state_path,
                            stale_owner,
                            stale_endpoint,
                            stale_worker_image_id,
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
                    "stale session/worker cleanup could not be verified",
                    evidence,
                )

        owner = secrets.token_hex(12)
        _validate_session_owner(owner)
        _write_owner_state(
            state_path, owner, current_endpoint, worker_image_id
        )
        if browser_network_control:
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
        else:
            preflight = _combine_cleanup_results(
                preflight,
                _empty_cleanup_result(required=False, verified=True),
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

    def abort_before_workers() -> Mapping[str, Any]:
        nonlocal finalized
        if finalized is not None:
            return finalized
        final = _empty_cleanup_result(required=True, verified=True)
        final["worker_artifact_cleanup_required"] = (
            finalize_worker_artifacts is not None
        )
        final["worker_artifact_cleanup_verified"] = (
            finalize_worker_artifacts is None
        )
        if finalize_worker_artifacts is not None:
            try:
                artifact_cleanup_verified = (
                    finalize_worker_artifacts(owner) is True
                )
            except Exception as exc:
                artifact_cleanup_verified = False
                final["error_count"] = 1
                final["error_sha256"] = [
                    _cleanup_error_hash("worker_artifacts", exc)
                ]
            final["worker_artifact_cleanup_verified"] = (
                artifact_cleanup_verified
            )
            if not artifact_cleanup_verified:
                final["verified_zero"] = False
        removed = False
        if final["verified_zero"]:
            try:
                removed = _remove_owner_state(
                    state_path, owner, current_endpoint, worker_image_id
                )
            except Exception as exc:
                final["verified_zero"] = False
                final["error_count"] += 1
                final["error_sha256"] = sorted(
                    {
                        *final["error_sha256"],
                        _cleanup_error_hash("state_remove", exc),
                    }
                )
        if not removed:
            final["verified_zero"] = False
        finalized = _ownership_evidence(
            lock_acquired=True,
            preflight=preflight,
            final=final,
            state_file_removed=removed,
        )
        return finalized

    def finalize() -> Mapping[str, Any]:
        nonlocal finalized
        if finalized is not None:
            return finalized
        final = (
            _sweep_owned_browser_sessions(
                flaresolverr_url=current_endpoint,
                owner=owner,
                stale=False,
                monotonic=monotonic,
                sleep=sleep,
            )
            if browser_network_control
            else _empty_cleanup_result(required=False, verified=True)
        )
        removed = False
        final["worker_artifact_cleanup_required"] = (
            finalize_worker_artifacts is not None
        )
        final["worker_artifact_cleanup_verified"] = (
            finalize_worker_artifacts is None
        )
        if final["verified_zero"] and finalize_worker_artifacts is not None:
            try:
                artifact_cleanup_verified = (
                    finalize_worker_artifacts(owner) is True
                )
            except Exception as exc:
                artifact_cleanup_verified = False
                final["error_count"] += 1
                final["error_sha256"] = sorted(
                    {
                        *final["error_sha256"],
                        _cleanup_error_hash("worker_artifacts", exc),
                    }
                )
            final["worker_artifact_cleanup_verified"] = (
                artifact_cleanup_verified
            )
            if not artifact_cleanup_verified:
                final["verified_zero"] = False
        if final["verified_zero"]:
            try:
                removed = _remove_owner_state(
                    state_path, owner, current_endpoint, worker_image_id
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
        abort_callback=abort_before_workers,
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


def _workflow_traffic(
    report: Mapping[str, Any],
) -> tuple[int, int, int, int, int]:
    """Return source/network attempts, work units, and paid counters."""

    source_request_attempts = 0
    page_units = 0
    paid_bytes = 0
    paid_route_requests = 0
    phase_network_requests = 0
    mode = report.get("mode")
    cache_mode = mode == CACHE_CAPACITY_MODE
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
        cache_attempts = _as_child_nonnegative_int(
            traffic.get("cache_work_units_attempted", 0),
            field_name="cache_work_units_attempted",
        )
        if "successful_page_units" not in traffic:
            raise ValueError("workflow omitted successful_page_units")
        completed = _as_child_nonnegative_int(
            traffic.get("successful_page_units"),
            field_name="successful_page_units",
        )
        denominator = cache_attempts if cache_mode else attempts
        if completed > denominator:
            raise ValueError("successful_page_units cannot exceed attempted work")
        if cache_mode and attempts != 0:
            raise ValueError("cache workflow emitted source request attempts")
        source_request_attempts += attempts
        page_units += completed
        paid_bytes += _as_child_nonnegative_int(
            traffic.get("paid_proxy_bytes", 0), field_name="paid_proxy_bytes"
        )
        paid_route_requests += _as_child_nonnegative_int(
            traffic.get("paid_route_requests", 0),
            field_name="paid_route_requests",
        )
        if cache_mode:
            phase_network_requests += _as_child_nonnegative_int(
                traffic.get("network_requests"), field_name="network_requests"
            )
    if cache_mode:
        if report.get("schema_version") != CAPACITY_REPORT_SCHEMA_VERSION:
            raise ValueError("cache workflow schema version mismatch")
        network_requests = _as_child_nonnegative_int(
            report.get("network_requests"), field_name="network_requests"
        )
        reported_paid = _as_child_nonnegative_int(
            report.get("paid_proxy_bytes"), field_name="paid_proxy_bytes"
        )
        reported_paid_requests = _as_child_nonnegative_int(
            report.get("paid_route_requests"), field_name="paid_route_requests"
        )
        if network_requests != phase_network_requests:
            raise ValueError("cache workflow network counters disagree")
        if reported_paid != paid_bytes or reported_paid_requests != paid_route_requests:
            raise ValueError("cache workflow paid counters disagree")
    else:
        network_requests = source_request_attempts
    return (
        source_request_attempts,
        page_units,
        paid_bytes,
        paid_route_requests,
        network_requests,
    )


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
    cache_mode = report.get("mode") == CACHE_CAPACITY_MODE
    expected_phase = "cache_replay" if cache_mode else "cold"
    measured = next(
        (
            phase
            for phase in phases
            if isinstance(phase, Mapping) and phase.get("name") == expected_phase
        ),
        None,
    )
    if not isinstance(measured, Mapping):
        raise ValueError(f"workflow did not emit a {expected_phase} phase")
    results = measured.get("results") or []
    if not isinstance(results, list):
        raise ValueError("cold phase results must be a list")
    by_entity = {
        str(result.get("entity")): result
        for result in results
        if isinstance(result, Mapping) and result.get("entity")
    }
    required = (
        {"matches", "previews", "profiles", "multistage"}
        if cache_mode
        else {"schedule", "matches", "previews", "profiles"}
    )
    missing = sorted(required.difference(by_entity))
    if missing:
        raise ValueError("workflow omitted entity results: " + ", ".join(missing))
    if not cache_mode and (
        not measured.get("selected_match_ids")
        or not measured.get("selected_profile_ids")
    ):
        raise ValueError("workflow did not exercise match and profile targets")
    stage_entity = "multistage" if cache_mode else "schedule"
    schedule_metadata = by_entity[stage_entity].get("metadata") or {}
    if not isinstance(schedule_metadata, Mapping):
        raise ValueError("stage result metadata must be an object")
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
        "network_requests": 0,
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
        "mode": None,
        "seed_sha256": None,
        "seed_evidence_valid": None,
        "cleanup_evidence_valid": None,
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
    mode = report.get("mode") or DIRECT_DIAGNOSTIC_MODE
    summary.update(
        {
            "mode": mode if mode in {CACHE_CAPACITY_MODE, DIRECT_DIAGNOSTIC_MODE} else None,
            "publishes": _evidence_bool(report.get("publishes")),
            "writes_bronze": _evidence_bool(report.get("writes_bronze")),
            "executes_ddl": _evidence_bool(report.get("executes_ddl")),
            "traffic_evidence_valid": False,
        }
    )
    try:
        if report.get("benchmark_version") != EXPECTED_WORKFLOW_VERSION:
            raise ValueError("workflow benchmark version mismatch")
        (
            attempts,
            page_units,
            paid_bytes,
            paid_requests,
            network_requests,
        ) = _workflow_traffic(report)
    except (TypeError, ValueError) as exc:
        summary["error"] = _safe_message(exc)
        return summary
    summary.update(
        {
            "source_request_attempts": attempts,
            "network_requests": network_requests,
            "page_units": page_units,
            "paid_bytes": paid_bytes,
            "paid_route_requests": paid_requests,
            "traffic_evidence_valid": True,
        }
    )
    if mode == CACHE_CAPACITY_MODE:
        seed_sha256 = report.get("seed_sha256")
        cleanup = report.get("cleanup")
        seed_valid = (
            isinstance(seed_sha256, str)
            and seed_sha256 == EXPECTED_CACHE_SEED_SHA256
        )
        cleanup_valid = (
            isinstance(cleanup, Mapping)
            and cleanup.get("status") == "success"
            and cleanup.get("temporary_workspace_removed") is True
        )
        summary.update(
            {
                "seed_sha256": (
                    seed_sha256
                    if isinstance(seed_sha256, str)
                    and re.fullmatch(r"[0-9a-f]{64}", seed_sha256)
                    else None
                ),
                "seed_evidence_valid": seed_valid,
                "cleanup_evidence_valid": cleanup_valid,
            }
        )
        if not seed_valid:
            summary["error"] = "cache workflow seed identity mismatch"
            return summary
        if network_requests != 0:
            summary["error"] = "cache workflow attempted network access"
            return summary
        if paid_bytes != 0 or paid_requests != 0:
            summary["error"] = "cache workflow used a paid route"
            return summary
        if not cleanup_valid:
            summary["error"] = "cache workflow did not prove cleanup"
            return summary
    elif mode == DIRECT_DIAGNOSTIC_MODE:
        summary["seed_evidence_valid"] = True
        summary["cleanup_evidence_valid"] = True
    else:
        summary["error"] = "workflow mode mismatch"
        return summary
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


def _retain_run_summary(
    accumulator: CapacityAccumulator, summary: Mapping[str, Any]
) -> None:
    """Retain bounded deterministic first/last/error evidence only."""

    snapshot = dict(summary)
    worker_id = int(snapshot["worker_id"])
    iteration = int(snapshot["iteration"])
    accumulator.run_summaries_total += 1
    first = accumulator.first_run_summary_by_worker.get(worker_id)
    if first is None or iteration < int(first["iteration"]):
        accumulator.first_run_summary_by_worker[worker_id] = snapshot
    last = accumulator.last_run_summary_by_worker.get(worker_id)
    if last is None or iteration > int(last["iteration"]):
        accumulator.last_run_summary_by_worker[worker_id] = snapshot
    if snapshot.get("status") not in {
        "success",
        "deadline_terminated",
        "aborted_by_gate",
        "aborted_by_peer_failure",
    }:
        errors = [*accumulator.error_run_summaries, snapshot]
        errors.sort(key=lambda item: (int(item["iteration"]), int(item["worker_id"])))
        accumulator.error_run_summaries = errors[
            :MAX_RETAINED_ERROR_RUN_SUMMARIES
        ]
    retained: dict[tuple[int, int], dict[str, Any]] = {}
    for item in (
        *accumulator.first_run_summary_by_worker.values(),
        *accumulator.last_run_summary_by_worker.values(),
        *accumulator.error_run_summaries,
    ):
        retained[(int(item["iteration"]), int(item["worker_id"]))] = item
    accumulator.run_summaries = [retained[key] for key in sorted(retained)]
    if len(accumulator.run_summaries) > MAX_RETAINED_RUN_SUMMARIES:
        raise RuntimeError("bounded run-summary retention invariant failed")


def _accept_outcome(
    accumulator: CapacityAccumulator,
    outcome: WorkerOutcome,
    *,
    expected_mode: Optional[str] = None,
) -> None:
    summary = _summarize_outcome(outcome)
    _retain_run_summary(accumulator, summary)
    accumulator.paid_bytes += int(summary["paid_bytes"])
    accumulator.paid_route_requests += int(summary["paid_route_requests"])
    accumulator.source_request_attempts += int(summary["source_request_attempts"])
    accumulator.network_requests += int(summary["network_requests"])

    if summary["status"] == "deadline_terminated":
        accumulator.deadline_truncations += 1
        return
    if summary["status"] in {"aborted_by_gate", "aborted_by_peer_failure"}:
        return
    if expected_mode is not None and summary.get("mode") != expected_mode:
        accumulator.traffic_evidence_violations.append(
            f"worker {outcome.worker_id} iteration {outcome.iteration} did not "
            "prove the requested capacity mode"
        )
    if expected_mode == CACHE_CAPACITY_MODE:
        if summary.get("seed_evidence_valid") is not True:
            accumulator.seed_evidence_violations.append(
                f"worker {outcome.worker_id} iteration {outcome.iteration} did not "
                "prove the reviewed cache seed"
            )
        elif accumulator.seed_sha256 is None:
            accumulator.seed_sha256 = str(summary["seed_sha256"])
        elif accumulator.seed_sha256 != summary.get("seed_sha256"):
            accumulator.seed_evidence_violations.append(
                f"worker {outcome.worker_id} iteration {outcome.iteration} changed "
                "cache seed identity"
            )
        if summary.get("cleanup_evidence_valid") is not True:
            accumulator.cleanup_evidence_violations.append(
                f"worker {outcome.worker_id} iteration {outcome.iteration} did not "
                "prove cache workspace cleanup"
            )
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


def _is_reportable_container_worker_failure(
    lifecycle_outcome: Any,
    worker_result: Any,
    returncode: object,
) -> bool:
    """Accept child evidence only for one fully captured worker exit."""

    return (
        getattr(lifecycle_outcome, "status", None) == "failed"
        and getattr(lifecycle_outcome, "reason", None)
        == "a worker exited unsuccessfully"
        and getattr(lifecycle_outcome, "released", None) is True
        and type(returncode) is int
        and returncode not in (0, _CAPACITY_BOOTSTRAP_LIVENESS_EXIT_CODE)
        and worker_result is not None
        and getattr(worker_result, "output_complete", None) is True
        and type(getattr(worker_result, "attach_returncode", None)) is int
        and worker_result.attach_returncode == returncode
        and isinstance(getattr(worker_result, "stdout_json", None), Mapping)
        and type(getattr(worker_result, "stderr_bytes", None)) is int
        and worker_result.stderr_bytes >= 0
    )


def _projected_page_units_per_day(page_units: int, elapsed_seconds: float) -> float:
    if elapsed_seconds <= 0:
        return 0.0
    return float(page_units) * 86_400.0 / elapsed_seconds


def _flaresolverr_compose_labels_match(
    labels: object,
    *,
    image_labels: Mapping[str, str],
    config_hash: str,
    image_id: str,
    deployment: ProductionDeployment,
) -> bool:
    if not isinstance(labels, Mapping) or any(
        not isinstance(name, str) or not isinstance(value, str)
        for name, value in labels.items()
    ):
        return False
    if any(not name.startswith(_SAFE_IMAGE_LABEL_PREFIXES) for name in image_labels):
        return False
    compose_version = labels.get("com.docker.compose.version")
    version_match = (
        _COMPOSE_VERSION_RE.fullmatch(compose_version)
        if isinstance(compose_version, str)
        else None
    )
    if version_match is None or tuple(
        int(version_match.group(index)) for index in (1, 2, 3)
    ) < (2, 24, 4):
        return False
    compose_labels = {
        "com.docker.compose.config-hash": config_hash,
        "com.docker.compose.container-number": "1",
        "com.docker.compose.depends_on": "",
        "com.docker.compose.image": image_id,
        "com.docker.compose.oneoff": "False",
        "com.docker.compose.project": REQUIRED_COMPOSE_PROJECT,
        "com.docker.compose.project.config_files": ",".join(
            str(path) for path in deployment.compose_files
        ),
        "com.docker.compose.project.environment_file": ",".join(
            str(path) for path in PRODUCTION_COMPOSE_ENV_FILES
        ),
        "com.docker.compose.project.working_dir": str(REPO_ROOT.resolve()),
        "com.docker.compose.service": "flaresolverr",
        "com.docker.compose.version": compose_version,
    }
    replacement = labels.get("com.docker.compose.replace")
    if replacement is not None:
        if replacement != "flaresolverr":
            return False
        compose_labels["com.docker.compose.replace"] = "flaresolverr"
    return dict(labels) == {**dict(image_labels), **compose_labels}


def _normalise_container(
    raw: Mapping[str, Any],
    *,
    production_deployment: Optional[ProductionDeployment] = None,
    expected_flaresolverr_config_hash: Optional[str] = None,
    expected_flaresolverr_image_id: Optional[str] = None,
    expected_flaresolverr_image_labels: Optional[Mapping[str, str]] = None,
) -> dict[str, Any]:
    state = raw.get("State") or {}
    if not isinstance(state, Mapping):
        raise RuntimeError("docker inspect returned invalid State")
    name = str(raw.get("Name") or "").lstrip("/")
    container_id = str(raw.get("Id") or "")
    if not name or re.fullmatch(r"[0-9a-f]{64}", container_id) is None:
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
            compose_identity_ok = bool(
                production_deployment is not None
                and expected_flaresolverr_config_hash is not None
                and expected_flaresolverr_image_id is not None
                and expected_flaresolverr_image_labels is not None
                and _flaresolverr_compose_labels_match(
                    labels,
                    image_labels=expected_flaresolverr_image_labels,
                    config_hash=expected_flaresolverr_config_hash,
                    image_id=expected_flaresolverr_image_id,
                    deployment=production_deployment,
                )
            )
            image_identity_contract_ok = (
                isinstance(labels, Mapping)
                and production_deployment is not None
                and config.get("Image")
                == production_deployment.flaresolverr_image_reference
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
        "id": container_id,
        "image_id": str(raw.get("Image") or ""),
        "command_contract_ok": command_contract_ok,
        "image_identity_contract_ok": image_identity_contract_ok,
        "immutable_payload_contract_ok": immutable_payload_contract_ok,
        "security_contract_ok": security_contract_ok,
        "compose_identity_ok": compose_identity_ok,
        "published_endpoint_contract_ok": published_endpoint_contract_ok,
        "status": str(state.get("Status") or "unknown"),
        "running": bool(state.get("Running", False)),
        "healthy": (
            isinstance(state.get("Health"), Mapping)
            and state["Health"].get("Status") == "healthy"
        ),
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


def _run_local_docker(
    arguments: Sequence[str], *, timeout: float
) -> subprocess.CompletedProcess[str]:
    """Run the one trusted Docker CLI against the one local daemon."""

    try:
        cli = _LOCAL_DOCKER_CLI.lstat()
        socket = _LOCAL_DOCKER_SOCKET.lstat()
    except OSError as exc:
        raise RuntimeError("local Docker endpoint is unavailable") from exc
    if (
        not stat.S_ISREG(cli.st_mode)
        or cli.st_uid != 0
        or cli.st_mode & 0o022
        or not cli.st_mode & 0o111
        or not stat.S_ISSOCK(socket.st_mode)
        or socket.st_uid != 0
        or socket.st_mode & 0o002
    ):
        raise RuntimeError("local Docker endpoint metadata is invalid")
    try:
        result = subprocess.run(
            [str(_LOCAL_DOCKER_CLI), *arguments],
            cwd=REPO_ROOT,
            env=dict(_LOCAL_DOCKER_ENVIRONMENT),
            stdin=subprocess.DEVNULL,
            check=False,
            capture_output=True,
            text=True,
            timeout=timeout,
        )
    except (OSError, subprocess.SubprocessError) as exc:
        raise RuntimeError("local Docker command failed") from exc
    if (
        len(str(getattr(result, "stdout", "")).encode("utf-8", errors="replace"))
        > 16 * 1024 * 1024
        or len(
            str(getattr(result, "stderr", "")).encode(
                "utf-8", errors="replace"
            )
        )
        > 1024 * 1024
    ):
        raise RuntimeError("local Docker command output is too large")
    return result


def _inspect_container_resources(
    names: Sequence[str],
) -> Mapping[str, Mapping[str, int]]:
    result = _run_local_docker(
        ("stats", "--no-stream", "--format", "{{json .}}", *names),
        timeout=15,
    )
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


def _protected_input_snapshot(
    path: Path, *, label: str, private: bool
) -> ProtectedInputSnapshot:
    """Read one root-owned input through an fd-pinned no-symlink path walk."""

    if (
        not path.is_absolute()
        or not path.name
        or path != Path(os.path.abspath(path))
    ):
        raise RuntimeError("production input path is not canonical")
    directory_flags = (
        os.O_RDONLY
        | os.O_DIRECTORY
        | getattr(os, "O_CLOEXEC", 0)
        | getattr(os, "O_NOFOLLOW", 0)
    )
    file_flags = (
        os.O_RDONLY
        | getattr(os, "O_CLOEXEC", 0)
        | getattr(os, "O_NOFOLLOW", 0)
        | getattr(os, "O_NONBLOCK", 0)
    )
    directory_fd = -1
    file_fd = -1
    try:
        directory_fd = os.open("/", directory_flags)
        for component in path.parts[1:-1]:
            child_fd = os.open(component, directory_flags, dir_fd=directory_fd)
            os.close(directory_fd)
            directory_fd = child_fd
            metadata = os.fstat(directory_fd)
            writable = metadata.st_mode & 0o022
            sticky_root_directory = (
                metadata.st_uid == 0
                and metadata.st_mode & stat.S_ISVTX
                and metadata.st_mode & 0o002
            )
            if metadata.st_uid != 0 or (writable and not sticky_root_directory):
                raise RuntimeError("production input parent metadata is invalid")
        file_fd = os.open(path.name, file_flags, dir_fd=directory_fd)
        before = os.fstat(file_fd)
        if (
            not stat.S_ISREG(before.st_mode)
            or before.st_uid != 0
            or before.st_nlink != 1
            or (private and stat.S_IMODE(before.st_mode) != 0o600)
            or (not private and before.st_mode & 0o022)
        ):
            raise RuntimeError("production Compose input metadata is invalid")
        chunks: list[bytes] = []
        size = 0
        while chunk := os.read(file_fd, 1024 * 1024):
            size += len(chunk)
            if size > 16 * 1024 * 1024:
                raise RuntimeError("production Compose input is too large")
            chunks.append(chunk)
        after = os.fstat(file_fd)
        entry = os.stat(path.name, dir_fd=directory_fd, follow_symlinks=False)
    except OSError as exc:
        raise RuntimeError("production Compose input is unavailable") from exc
    finally:
        if file_fd >= 0:
            os.close(file_fd)
        if directory_fd >= 0:
            os.close(directory_fd)
    before_identity = tuple(
        getattr(before, field) for field in _PROTECTED_INPUT_IDENTITY_FIELDS
    )
    if any(
        before_identity
        != tuple(
            getattr(candidate, field)
            for field in _PROTECTED_INPUT_IDENTITY_FIELDS
        )
        for candidate in (after, entry)
    ) or size != before.st_size:
        raise RuntimeError("production Compose input changed while reading")
    return ProtectedInputSnapshot(
        label=label,
        path=path,
        identity=before_identity,
        sha256=hashlib.sha256(b"".join(chunks)).hexdigest(),
        private=private,
    )


def _protected_compose_input_snapshot(
    path: Path, *, private: bool
) -> tuple[tuple[int, ...], str]:
    snapshot = _protected_input_snapshot(
        path, label=f"compose:{path.name}", private=private
    )
    return snapshot.identity, snapshot.sha256


def _resolved_flaresolverr_compose_hash(
    deployment: ProductionDeployment,
) -> str:
    """Hash the exact admitted three-file production Compose service."""

    input_paths = (*deployment.compose_files, *PRODUCTION_COMPOSE_ENV_FILES)
    if len(input_paths) != len(set(input_paths)):
        raise RuntimeError("production Compose inputs are duplicated")
    admitted_by_path = {
        snapshot.path: snapshot for snapshot in deployment.protected_inputs
    }
    snapshots = {
        path: _protected_input_snapshot(
            path,
            label=(
                admitted_by_path[path].label
                if path in admitted_by_path
                else f"compose:{path.name}"
            ),
            private=(
                admitted_by_path[path].private
                if path in admitted_by_path
                else path == deployment.common_digest_override_path
                or path in PRODUCTION_COMPOSE_ENV_FILES
            ),
        )
        for path in input_paths
    }
    if any(
        admitted_by_path.get(path) != snapshot
        for path, snapshot in snapshots.items()
    ):
        raise RuntimeError("production Compose input differs from admission")
    arguments = [
        "compose",
        "--project-name",
        REQUIRED_COMPOSE_PROJECT,
        "--project-directory",
        str(REPO_ROOT),
    ]
    for environment_file in PRODUCTION_COMPOSE_ENV_FILES:
        arguments.extend(("--env-file", str(environment_file)))
    for config_file in deployment.compose_files:
        arguments.extend(("--file", str(config_file)))
    arguments.extend(("config", "--hash", "flaresolverr"))
    result = _run_local_docker(arguments, timeout=30)
    for path, expected in snapshots.items():
        if _protected_input_snapshot(
            path,
            label=expected.label,
            private=expected.private,
        ) != expected:
            raise RuntimeError("production Compose input changed during render")
    fields = result.stdout.strip().split()
    if (
        result.returncode != 0
        or len(fields) != 2
        or fields[0] != "flaresolverr"
        or re.fullmatch(r"[0-9a-f]{64}", fields[1]) is None
    ):
        raise RuntimeError("production FlareSolverr config hash is unavailable")
    return fields[1]


def _resolved_flaresolverr_image(
    image_reference: str,
) -> tuple[str, Mapping[str, str]]:
    """Resolve the local ID, RepoDigest and labels of an attested image."""

    if _PINNED_IMAGE_RE.fullmatch(image_reference) is None:
        raise RuntimeError("production FlareSolverr image reference is mutable")
    result = _run_local_docker(("image", "inspect", image_reference), timeout=15)
    try:
        payload = json.loads(result.stdout)
    except json.JSONDecodeError as exc:
        raise RuntimeError("production FlareSolverr image ID is unavailable") from exc
    if result.returncode != 0 or not isinstance(payload, list) or len(payload) != 1:
        raise RuntimeError("production FlareSolverr image ID is unavailable")
    document = payload[0]
    if not isinstance(document, Mapping):
        raise RuntimeError("production FlareSolverr image ID is unavailable")
    image_id = document.get("Id")
    repo_digests = document.get("RepoDigests")
    config = document.get("Config")
    labels = config.get("Labels") if isinstance(config, Mapping) else None
    if (
        not isinstance(image_id, str)
        or re.fullmatch(r"sha256:[0-9a-f]{64}", image_id) is None
        or not isinstance(repo_digests, list)
        or image_reference not in repo_digests
        or labels is not None
        and (
            not isinstance(labels, Mapping)
            or any(
                not isinstance(name, str) or not isinstance(value, str)
                for name, value in labels.items()
            )
        )
    ):
        raise RuntimeError("production FlareSolverr image ID is unavailable")
    return image_id, dict(labels or {})


def _resolved_flaresolverr_image_id(image_reference: str) -> str:
    """Compatibility wrapper returning the verified local final image ID."""

    return _resolved_flaresolverr_image(image_reference)[0]


def _docker_inspect_payload(names: Sequence[str]) -> list[Mapping[str, Any]]:
    result = _run_local_docker(("inspect", *names), timeout=15)
    if result.returncode != 0:
        raise RuntimeError(
            "docker inspect failed for required containers: "
            + ", ".join(str(name) for name in names)
        )
    try:
        payload = json.loads(result.stdout)
    except json.JSONDecodeError as exc:
        raise RuntimeError("docker inspect did not return JSON") from exc
    if not isinstance(payload, list) or any(
        not isinstance(item, Mapping) for item in payload
    ):
        raise RuntimeError("docker inspect JSON must be a list")
    return payload


def _inspect_containers(
    names: Sequence[str], deployment: ProductionDeployment
) -> Mapping[str, Mapping[str, Any]]:
    if not names:
        raise RuntimeError("at least one container is required for restart/OOM evidence")
    if not set(ADMITTED_RUNNING_SERVICES).issubset(names):
        raise RuntimeError("container sample omits an admitted production service")
    before_payload = _docker_inspect_payload(names)
    expected_flaresolverr_config_hash = (
        _resolved_flaresolverr_compose_hash(deployment)
        if "flaresolverr" in names
        else None
    )
    expected_flaresolverr_image_id: Optional[str] = None
    expected_flaresolverr_image_labels: Optional[Mapping[str, str]] = None
    if "flaresolverr" in names:
        (
            expected_flaresolverr_image_id,
            expected_flaresolverr_image_labels,
        ) = _resolved_flaresolverr_image(
            deployment.flaresolverr_image_reference
        )
    resources = _inspect_container_resources(names)
    after_payload = _docker_inspect_payload(names)

    def normalize(payload: Sequence[Mapping[str, Any]]) -> dict[str, dict[str, Any]]:
        rows = [
            _normalise_container(
                raw,
                production_deployment=deployment,
                expected_flaresolverr_config_hash=expected_flaresolverr_config_hash,
                expected_flaresolverr_image_id=expected_flaresolverr_image_id,
                expected_flaresolverr_image_labels=(
                    expected_flaresolverr_image_labels
                ),
            )
            for raw in payload
        ]
        if len({row["name"] for row in rows}) != len(rows):
            raise RuntimeError("docker inspect duplicated container identities")
        return {row["name"]: row for row in rows}

    before = normalize(before_payload)
    inspected = normalize(after_payload)
    if set(before) != set(names) or set(inspected) != set(names):
        raise RuntimeError("docker inspect did not return the exact container set")
    if before != inspected:
        raise RuntimeError("container identity changed during admission")
    admitted_images = deployment.running_admission.get("images")
    if not isinstance(admitted_images, list):
        raise RuntimeError("production running admission image evidence is invalid")
    admitted_by_service = {
        str(record.get("service")): record
        for record in admitted_images
        if isinstance(record, Mapping)
    }
    for service in ADMITTED_RUNNING_SERVICES:
        record = admitted_by_service.get(service)
        row = inspected[service]
        row["production_admission_contract_ok"] = bool(
            isinstance(record, Mapping)
            and row.get("id") == record.get("container_id")
            and row.get("image_id") == record.get("image_id")
            and record.get("final_image")
            == deployment.protected_bindings.get(service)
            and row.get("running") is True
            and row.get("healthy") is True
            and row.get("oom_killed") is False
        )
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


def _container_admission_violations(
    current: Mapping[str, Mapping[str, Any]],
    required_names: Sequence[str],
) -> list[str]:
    violations: list[str] = []
    if set(current) != set(required_names):
        violations.append("container sample did not cover the exact required set")
    for service in ADMITTED_RUNNING_SERVICES:
        record = current.get(service)
        if (
            not isinstance(record, Mapping)
            or record.get("production_admission_contract_ok") is not True
        ):
            violations.append(
                f"{service}: running identity differs from production admission"
            )
    flaresolverr = current.get("flaresolverr")
    contracts = (
        (
            "command_contract_ok",
            "flaresolverr: running command does not match production",
        ),
        (
            "image_identity_contract_ok",
            "flaresolverr: final image identity does not match production",
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
            "flaresolverr: running Compose identity does not match production",
        ),
        (
            "published_endpoint_contract_ok",
            "flaresolverr: published endpoint does not match 127.0.0.1:8191",
        ),
    )
    for field_name, message in contracts:
        if (
            not isinstance(flaresolverr, Mapping)
            or flaresolverr.get(field_name) is not True
        ):
            violations.append(message)
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


def _worker_exec_argv(
    argv: Sequence[str],
    *,
    python_path: str = sys.executable,
    namespace_path: str = str(WORKER_NAMESPACE_EXECUTABLE),
    helper_path: str = str(WORKER_EXEC_SCRIPT),
    close_fds: Sequence[int] = (),
    ready_fd: int,
    release_fd: int,
) -> tuple[str, ...]:
    if not argv or any(type(value) is not str or not value for value in argv):
        raise ValueError("invalid capacity worker argv")
    return (
        python_path,
        "-I",
        "-S",
        "-B",
        "-u",
        helper_path,
        "--expected-parent-pid",
        str(os.getpid()),
        "--ready-fd",
        str(ready_fd),
        "--release-fd",
        str(release_fd),
        *(
            argument
            for descriptor in close_fds
            for argument in ("--close-fd", str(descriptor))
        ),
        "--",
        namespace_path,
        "--pid",
        "--fork",
        "--kill-child=SIGKILL",
        "--",
        *argv,
    )


def _worker_environment() -> dict[str, str]:
    """Return the complete allowlisted environment inherited by a worker."""

    return {
        "HOME": "/nonexistent",
        "LANG": "C.UTF-8",
        "LC_ALL": "C.UTF-8",
        "PATH": "/usr/bin:/bin",
        "PYTHONDONTWRITEBYTECODE": "1",
        "PYTHONUNBUFFERED": "1",
        "WHOSCORED_SOURCE_CIRCUIT_PATH": str(CAPACITY_SOURCE_CIRCUIT_PATH),
        # Capacity workers wait behind one persistent shared half-open probe.
        # Production tasks fail fast instead.
        "WHOSCORED_SOURCE_CIRCUIT_WAIT": "1",
    }


def _atomic_release_cohort(release_fd: int, worker_count: int) -> None:
    """Release one cohort with the kernel's single PIPE_BUF-atomic write."""

    if type(worker_count) is not int or not 1 <= worker_count <= WORKER_COUNT:
        raise ValueError("capacity release cohort size is invalid")
    payload = b"G" * worker_count
    try:
        pipe_buf = os.fpathconf(release_fd, "PC_PIPE_BUF")
    except (OSError, ValueError) as exc:
        raise RuntimeError("capacity release pipe atomicity is unavailable") from exc
    if not os.get_blocking(release_fd) or len(payload) > pipe_buf:
        raise RuntimeError("capacity release pipe is not atomically writable")
    if os.write(release_fd, payload) != len(payload):
        # A blocking <=PIPE_BUF pipe write is all-or-none.  Treat a platform
        # violating that contract as fatal and kill the fully tracked cohort.
        raise RuntimeError("capacity release pipe accepted a partial cohort")


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
    worker_runtime: Optional[AdmittedWorkerRuntime] = None,
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
    production_commands = any(
        tuple(command.argv[:2]) == (sys.executable, str(WORKFLOW_SCRIPT))
        for command in commands
    )
    if production_commands and worker_runtime is None:
        raise RuntimeError("production workers require a sealed admitted runtime")
    worker_exec_error = (
        _pinned_worker_exec_preflight(worker_runtime)
        if worker_runtime is not None
        else _worker_exec_preflight()
    )
    if worker_exec_error is not None:
        raise RuntimeError(worker_exec_error)
    running: list[_RunningProcess] = []
    blocked: list[_RunningProcess] = []
    with TemporaryDirectory(prefix="whoscored-capacity-round-") as temp_root:
        stop_latched = False

        def stop_is_requested() -> bool:
            """Make a fail-fast stop request sticky across outcome callbacks."""

            nonlocal stop_latched
            if not stop_latched and should_stop():
                stop_latched = True
            return stop_latched

        def emit_outcome(outcome: WorkerOutcome) -> None:
            on_outcome(outcome)
            stop_is_requested()

        def close_fd(fd: Optional[int]) -> None:
            if fd is None:
                return
            try:
                os.close(fd)
            except OSError:
                pass

        def spawn_blocked(
            command: WorkerCommand, *, release_read_fd: int
        ) -> Optional[_RunningProcess]:
            if stop_is_requested():
                return None
            stem = f"worker-{command.worker_id}-iteration-{command.iteration}"
            stdout_handle = open(
                Path(temp_root) / f"{stem}.stdout", "w+", encoding="utf-8"
            )
            stderr_handle = open(
                Path(temp_root) / f"{stem}.stderr", "w+", encoding="utf-8"
            )
            control_fd: Optional[int] = None
            ready_read_fd: Optional[int] = None
            ready_write_fd: Optional[int] = None
            try:
                worker_argv = (
                    _sealed_worker_argv(command.argv, worker_runtime)
                    if worker_runtime is not None
                    else command.argv
                )
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
                ready_read_fd, ready_write_fd = os.pipe()
                runtime_fds = (
                    (
                        worker_runtime.bundle_fd,
                        worker_runtime.helper_fd,
                        worker_runtime.catalog_fd,
                        worker_runtime.python_fd,
                        worker_runtime.unshare_fd,
                    )
                    if worker_runtime is not None
                    else ()
                )
                child_fds = tuple(
                    fd
                    for fd in (
                        control_fd,
                        ready_write_fd,
                        release_read_fd,
                        *runtime_fds,
                    )
                    if fd is not None
                )
                environment = _worker_environment()
                helper_path = str(WORKER_EXEC_SCRIPT)
                if worker_runtime is not None:
                    environment.update(
                        {
                            "WHOSCORED_CAPACITY_BUNDLE_PATH": (
                                f"/proc/self/fd/{worker_runtime.bundle_fd}"
                            ),
                            "WHOSCORED_CAPACITY_SITE_PACKAGES": str(
                                worker_runtime.site_packages
                            ),
                        }
                    )
                    helper_path = f"/proc/self/fd/{worker_runtime.helper_fd}"
                process = subprocess.Popen(
                    _worker_exec_argv(
                        worker_argv,
                        python_path=(
                            f"/proc/self/fd/{worker_runtime.python_fd}"
                            if worker_runtime is not None
                            else sys.executable
                        ),
                        namespace_path=(
                            f"/proc/self/fd/{worker_runtime.unshare_fd}"
                            if worker_runtime is not None
                            else str(WORKER_NAMESPACE_EXECUTABLE)
                        ),
                        helper_path=helper_path,
                        close_fds=(
                            (worker_runtime.helper_fd,)
                            if worker_runtime is not None
                            else ()
                        ),
                        ready_fd=ready_write_fd,
                        release_fd=release_read_fd,
                    ),
                    cwd=REPO_ROOT,
                    env=environment,
                    stdout=stdout_handle,
                    stderr=stderr_handle,
                    text=True,
                    start_new_session=True,
                    pass_fds=child_fds,
                )
            except (OSError, ValueError, subprocess.SubprocessError) as exc:
                for fd in (
                    ready_read_fd,
                    ready_write_fd,
                    control_fd,
                ):
                    close_fd(fd)
                stdout_handle.close()
                stderr_handle.close()
                emit_outcome(
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
                return None
            close_fd(control_fd)
            close_fd(ready_write_fd)
            return _RunningProcess(
                command=command,
                process=process,
                stdout_handle=stdout_handle,
                stderr_handle=stderr_handle,
                started_at=monotonic(),
                ready_fd=ready_read_fd,
            )

        def await_ready(item: _RunningProcess) -> None:
            if item.ready_fd is None:
                raise RuntimeError("capacity worker readiness pipe is unavailable")
            payload = bytearray()
            ready_deadline = time.monotonic() + _WORKER_READY_TIMEOUT_SECONDS
            while True:
                if stop_is_requested():
                    raise RuntimeError("capacity worker release was cancelled")
                if monotonic() >= deadline:
                    raise RuntimeError("capacity worker release missed the deadline")
                remaining = ready_deadline - time.monotonic()
                if remaining <= 0:
                    raise RuntimeError("capacity worker readiness timed out")
                readable, _, _ = select.select(
                    (item.ready_fd,), (), (), min(_POLL_SECONDS, remaining)
                )
                if not readable:
                    if item.process.poll() is not None:
                        raise RuntimeError(
                            "capacity worker exited before readiness acknowledgement"
                        )
                    continue
                chunk = os.read(
                    item.ready_fd,
                    len(_WORKER_READY_PAYLOAD) + 1 - len(payload),
                )
                if not chunk:
                    close_fd(item.ready_fd)
                    item.ready_fd = None
                    if bytes(payload) != _WORKER_READY_PAYLOAD:
                        raise RuntimeError(
                            "capacity worker readiness acknowledgement is invalid"
                        )
                    return
                payload.extend(chunk)
                if (
                    len(payload) > len(_WORKER_READY_PAYLOAD)
                    or not _WORKER_READY_PAYLOAD.startswith(payload)
                ):
                    raise RuntimeError(
                        "capacity worker readiness acknowledgement is invalid"
                    )

        def abort_blocked(
            cohort: Sequence[_RunningProcess], *, reason: str
        ) -> None:
            for item in cohort:
                close_fd(item.ready_fd)
                item.ready_fd = None
            _stop_processes(cohort, monotonic=monotonic, sleep=sleep)
            for item in cohort:
                emit_outcome(
                    _finish_process(
                        item,
                        monotonic=monotonic,
                        termination_reason=reason,
                    )
                )
                item.stdout_handle.close()
                item.stderr_handle.close()

        def close_release_gate(gate: dict[str, Optional[int]]) -> None:
            descriptor = gate.get("write_fd")
            gate["write_fd"] = None
            close_fd(descriptor)

        def activate(
            cohort: Sequence[_RunningProcess],
            gate: dict[str, Optional[int]],
        ) -> None:
            try:
                for item in cohort:
                    await_ready(item)
                before_launch()
                if stop_is_requested():
                    raise RuntimeError("capacity worker release was cancelled")
                released_at = monotonic()
                if released_at >= deadline:
                    raise RuntimeError("capacity worker release missed the deadline")
                if any(item.process.poll() is not None for item in cohort):
                    raise RuntimeError("capacity worker exited before cohort release")
                release_fd = gate.get("write_fd")
                if release_fd is None:
                    raise RuntimeError("capacity worker release pipe is unavailable")
                _atomic_release_cohort(release_fd, len(cohort))
                close_release_gate(gate)
                for item in cohort:
                    item.started_at = released_at
                running.extend(cohort)
                for item in cohort:
                    if item in blocked:
                        blocked.remove(item)
            except BaseException:
                # Closing one common writer gives every still-blocked helper
                # EOF.  If the atomic write already committed, all cohort
                # members remain tracked here and are killed as one unit.
                close_release_gate(gate)
                abort_blocked(cohort, reason="launch_barrier_failed")
                for item in cohort:
                    if item in blocked:
                        blocked.remove(item)
                raise

        def spawn_cohort(
            planned: Sequence[WorkerCommand],
        ) -> tuple[list[_RunningProcess], Optional[dict[str, Optional[int]]]]:
            release_read_fd, release_write_fd = os.pipe()
            gate: dict[str, Optional[int]] = {"write_fd": release_write_fd}
            cohort: list[_RunningProcess] = []
            try:
                for command in planned:
                    item = spawn_blocked(
                        command, release_read_fd=release_read_fd
                    )
                    if item is None:
                        break
                    cohort.append(item)
                    blocked.append(item)
                    if stop_is_requested():
                        break
            except BaseException:
                close_fd(release_read_fd)
                close_release_gate(gate)
                if cohort:
                    abort_blocked(cohort, reason="launch_barrier_failed")
                    for item in cohort:
                        if item in blocked:
                            blocked.remove(item)
                raise
            close_fd(release_read_fd)
            if len(cohort) != len(planned):
                close_release_gate(gate)
                if cohort:
                    abort_blocked(cohort, reason="aborted_by_gate")
                    for item in cohort:
                        if item in blocked:
                            blocked.remove(item)
                return [], None
            return cohort, gate

        try:
            initial_cohort, initial_gate = spawn_cohort(commands)
            if initial_gate is None:
                return
            activate(initial_cohort, initial_gate)

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
                    emit_outcome(
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

                stop_requested = stop_is_requested()
                deadline_reached = monotonic() >= deadline
                if stop_requested or deadline_reached:
                    reason = (
                        "deadline_terminated"
                        if deadline_reached and not stop_requested
                        else "aborted_by_gate"
                    )
                    _stop_processes(running, monotonic=monotonic, sleep=sleep)
                    for item in list(running):
                        emit_outcome(
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

                replacements: list[WorkerCommand] = []
                for command in completed:
                    if stop_is_requested():
                        break
                    replacements.append(
                        WorkerCommand(
                            worker_id=command.worker_id,
                            iteration=command.iteration + 1,
                            scope=command.scope,
                            argv=command.argv,
                            browser_session_owner=command.browser_session_owner,
                            flaresolverr_endpoint=command.flaresolverr_endpoint,
                        )
                    )
                if replacements:
                    replacement_cohort, replacement_gate = spawn_cohort(
                        replacements
                    )
                    if replacement_gate is not None:
                        activate(replacement_cohort, replacement_gate)
                if stop_is_requested():
                    continue
                if len(running) != WORKER_COUNT:
                    raise RuntimeError(
                        f"capacity supervisor lost worker slots: {len(running)}/"
                        f"{WORKER_COUNT}"
                    )
                sleep(min(_POLL_SECONDS, max(0.0, deadline - monotonic())))
        finally:
            if blocked:
                abort_blocked(blocked, reason="aborted_by_gate")
                blocked.clear()
            if running:
                try:
                    _stop_processes(running, monotonic=monotonic, sleep=sleep)
                finally:
                    for item in running:
                        item.stdout_handle.close()
                        item.stderr_handle.close()


def _admitted_running_container(
    deployment: ProductionDeployment, service: str
) -> tuple[str, str]:
    records = [
        record
        for record in deployment.running_admission.get("images", [])
        if isinstance(record, Mapping) and record.get("service") == service
    ]
    if len(records) != 1:
        raise RuntimeError(f"admitted {service} container identity is unavailable")
    container_id = records[0].get("container_id")
    image_id = records[0].get("image_id")
    if (
        not isinstance(container_id, str)
        or re.fullmatch(r"[0-9a-f]{64}", container_id) is None
        or not isinstance(image_id, str)
        or re.fullmatch(r"sha256:[0-9a-f]{64}", image_id) is None
    ):
        raise RuntimeError(f"admitted {service} container identity is invalid")
    return container_id, image_id


def _containerized_worker_argv(command: WorkerCommand) -> tuple[str, ...]:
    if tuple(command.argv[:2]) != (sys.executable, str(WORKFLOW_SCRIPT)):
        raise RuntimeError("production worker bypasses the admitted workflow")
    arguments = list(command.argv[2:])
    if arguments.count("--mode") != 1:
        raise RuntimeError("production worker mode is invalid")
    mode_index = arguments.index("--mode") + 1
    if mode_index >= len(arguments):
        raise RuntimeError("production worker mode is incomplete")
    mode = arguments[mode_index]
    if mode == CACHE_CAPACITY_MODE:
        if (
            command.browser_session_owner is not None
            or command.flaresolverr_endpoint is not None
        ):
            raise RuntimeError("cache worker received network control")
    elif mode == DIRECT_DIAGNOSTIC_MODE:
        if (
            command.browser_session_owner is None
            or command.flaresolverr_endpoint != REQUIRED_FLARESOLVERR_ENDPOINT
        ):
            raise RuntimeError("diagnostic worker control is incomplete")
    else:
        raise RuntimeError("production worker mode is unsupported")
    if arguments.count("--catalog") != 1:
        raise RuntimeError("production worker catalog argument is invalid")
    catalog_index = arguments.index("--catalog") + 1
    if catalog_index >= len(arguments):
        raise RuntimeError("production worker catalog argument is incomplete")
    arguments[catalog_index] = _CONTAINER_CATALOG_PATH
    return (_CONTAINER_WORKFLOW_PATH, *arguments)


def _cleanup_stale_capacity_workers(
    *,
    worker_runtime: AdmittedWorkerRuntime,
    owner: str,
    stale_worker_image_id: Optional[str],
) -> tuple[str, ...]:
    module = worker_runtime.container_runtime_module
    if (
        worker_runtime.execution_mode != "exact-scheduler-image-v1"
        or module is None
    ):
        raise RuntimeError("admitted container runtime is unavailable")
    owner = _validate_session_owner(owner)
    if stale_worker_image_id is None:
        if tuple(module.find_stale_owner_containers(owner)):
            raise RuntimeError(
                "legacy owner state cannot authenticate stale worker containers"
            )
        return ()
    scheduler_image_id = _validated_worker_image_id(stale_worker_image_id)
    assert scheduler_image_id is not None
    removed = module.cleanup_stale_owner_containers(
        owner=owner,
        scheduler_image_id=scheduler_image_id,
    )
    return tuple(removed)


def _run_container_round(
    commands: Sequence[WorkerCommand],
    *,
    deployment: ProductionDeployment,
    worker_runtime: Optional[AdmittedWorkerRuntime] = None,
    deadline: float,
    on_sample: Callable[[bool], None],
    on_outcome: Callable[[WorkerOutcome], None],
    should_stop: Callable[[], bool],
    before_launch: Callable[[], None],
    monotonic: Callable[[], float],
    sleep: Callable[[float], None],
) -> None:
    """Keep four exact container slots occupied until stop or deadline."""

    if len(commands) != WORKER_COUNT or tuple(
        command.worker_id for command in commands
    ) != tuple(range(WORKER_COUNT)):
        raise ValueError("container capacity round requires workers 0,1,2,3")
    if (
        worker_runtime is None
        or worker_runtime.execution_mode != "exact-scheduler-image-v1"
        or worker_runtime.runtime_root is None
        or worker_runtime.source_circuit_root is None
        or worker_runtime.container_runtime_module is None
    ):
        raise RuntimeError("container capacity round requires admitted runtime")
    # The lifecycle owner names and authenticates the disposable worker
    # containers.  It is deliberately separate from browser control: cache
    # workers still need an exact cleanup identity but must receive no browser
    # session or FlareSolverr authority.
    owners = {
        command.session_owner or command.browser_session_owner
        for command in commands
    }
    if len(owners) != 1 or None in owners:
        raise RuntimeError("container capacity workers require one owner")
    owner = _validate_session_owner(str(next(iter(owners))))
    scheduler_container_id, scheduler_image_id = _admitted_running_container(
        deployment, "airflow-scheduler"
    )
    del scheduler_container_id
    flaresolverr_container_id, _flaresolverr_image_id = (
        _admitted_running_container(deployment, "flaresolverr")
    )
    module = worker_runtime.container_runtime_module
    workers = tuple(
        module.WorkerSpec(
            worker_index=command.worker_id,
            workload_argv=_containerized_worker_argv(command),
            iteration=command.iteration,
        )
        for command in commands
    )
    started_at = monotonic()
    active_commands = {command.worker_id: command for command in commands}
    worker_started_at = {
        (command.worker_id, command.iteration): started_at for command in commands
    }
    emitted: set[tuple[int, int]] = set()
    lifecycle_outcomes: list[Any] = []

    def accept_sample(sample: Any) -> None:
        snapshots = tuple(getattr(sample, "containers", ()))
        if len(snapshots) != WORKER_COUNT or tuple(
            getattr(snapshot, "worker_index", None) for snapshot in snapshots
        ) != tuple(range(WORKER_COUNT)):
            raise RuntimeError("worker container sample is incomplete")
        memory_values = tuple(
            getattr(snapshot, "memory_usage_bytes", None)
            for snapshot in snapshots
        )
        pids_values = tuple(
            getattr(snapshot, "pids_current", None) for snapshot in snapshots
        )
        if any(
            type(value) is not int or value < 0
            for value in (*memory_values, *pids_values)
        ):
            raise RuntimeError("worker container resource sample is invalid")
        worker_runtime.worker_container_memory_bytes = sum(memory_values)
        worker_runtime.worker_container_pids = sum(pids_values)
        worker_runtime.worker_container_evidence = tuple(
            {
                "worker_id": snapshot.worker_index,
                "iteration": snapshot.iteration,
                "container_id": snapshot.container_id,
                "status": snapshot.status,
                "running": snapshot.running,
                "exit_code": snapshot.exit_code,
                "oom_killed": snapshot.oom_killed,
                "memory_usage_bytes": memory_values[index],
                "pids_current": pids_values[index],
            }
            for index, snapshot in enumerate(snapshots)
        )
        # The host monitors every resource sample but retains only its slower
        # configured report cadence.
        on_sample(False)

    def accept_worker_result(result: Any) -> None:
        worker_id = getattr(result, "worker_index", None)
        iteration = getattr(result, "iteration", None)
        if type(worker_id) is not int or type(iteration) is not int:
            raise RuntimeError("container worker result identity is invalid")
        command = active_commands.get(worker_id)
        if command is None or command.iteration != iteration:
            raise RuntimeError("container worker result is not the active slot")
        if type(result.stderr_bytes) is not int or result.stderr_bytes < 0:
            raise RuntimeError("container worker stderr evidence is invalid")
        key = (worker_id, iteration)
        if key in emitted:
            raise RuntimeError("container worker result was delivered twice")
        worker_start = worker_started_at.pop(key, None)
        if worker_start is None:
            raise RuntimeError("container worker start time is unavailable")
        emitted.add(key)
        on_outcome(
            WorkerOutcome(
                worker_id=worker_id,
                iteration=iteration,
                scope=command.scope,
                returncode=0,
                report=result.stdout_json,
                elapsed_seconds=max(0.0, monotonic() - worker_start),
                stderr_bytes=result.stderr_bytes,
                stderr_sha256=result.stderr_sha256,
            )
        )

    def replacement(previous: Any) -> Any:
        worker_id = getattr(previous, "worker_index", None)
        iteration = getattr(previous, "iteration", None)
        if type(worker_id) is not int or type(iteration) is not int:
            raise RuntimeError("completed container worker identity is invalid")
        command = active_commands.get(worker_id)
        if command is None or command.iteration != iteration:
            raise RuntimeError("completed container worker is not the active slot")
        completed_key = (worker_id, iteration)
        if completed_key not in emitted:
            raise RuntimeError("completed container result was not delivered")
        emitted.remove(completed_key)
        next_command = WorkerCommand(
            worker_id=command.worker_id,
            iteration=command.iteration + 1,
            scope=command.scope,
            argv=command.argv,
            browser_session_owner=command.browser_session_owner,
            flaresolverr_endpoint=command.flaresolverr_endpoint,
            session_owner=command.session_owner,
        )
        active_commands[worker_id] = next_command
        worker_started_at[(worker_id, next_command.iteration)] = monotonic()
        return module.WorkerSpec(
            worker_index=worker_id,
            workload_argv=_containerized_worker_argv(next_command),
            iteration=next_command.iteration,
        )

    outcome = module.run_capacity_containers(
        scheduler_image_id=scheduler_image_id,
        flaresolverr_container_id=flaresolverr_container_id,
        owner=owner,
        workers=workers,
        runtime_root=worker_runtime.runtime_root,
        source_circuit_root=worker_runtime.source_circuit_root,
        before_release=before_launch,
        on_sample=accept_sample,
        stop_requested=should_stop,
        deadline_reached=lambda: monotonic() >= deadline,
        on_outcome=lifecycle_outcomes.append,
        replacement_worker=replacement,
        on_worker_result=accept_worker_result,
        monotonic=monotonic,
        sleep=sleep,
    )
    if lifecycle_outcomes != [outcome]:
        raise RuntimeError("container lifecycle outcome was not delivered exactly once")
    if outcome.cleanup_complete:
        worker_runtime.worker_container_memory_bytes = 0
        worker_runtime.worker_container_pids = 0
        worker_runtime.worker_container_evidence = ()
    else:
        raise _WorkerTerminationUnverified(
            "exact worker container cleanup could not be proved"
        )

    terminal_results = {
        (result.worker_index, result.iteration): result
        for result in outcome.worker_results
    }
    pending_commands = tuple(
        command
        for command in active_commands.values()
        if (command.worker_id, command.iteration) not in emitted
    )

    if outcome.status == "completed":
        for command in pending_commands:
            key = (command.worker_id, command.iteration)
            result = terminal_results.get(key)
            if (
                result is None
                or type(result.stderr_bytes) is not int
                or result.stderr_bytes < 0
            ):
                raise RuntimeError("container worker results are incomplete")
            emitted.add(key)
            on_outcome(
                WorkerOutcome(
                    worker_id=command.worker_id,
                    iteration=command.iteration,
                    scope=command.scope,
                    returncode=outcome.exit_codes[command.worker_id],
                    report=result.stdout_json,
                    elapsed_seconds=max(
                        0.0, monotonic() - worker_started_at[key]
                    ),
                    stderr_bytes=result.stderr_bytes,
                    stderr_sha256=result.stderr_sha256,
                )
            )
        return

    if outcome.status in {"deadline", "stopped"}:
        termination_reason = (
            "deadline_terminated"
            if outcome.status == "deadline"
            else "aborted_by_gate"
        )
        for command in pending_commands:
            key = (command.worker_id, command.iteration)
            result = terminal_results.get(key)
            on_outcome(
                WorkerOutcome(
                    worker_id=command.worker_id,
                    iteration=command.iteration,
                    scope=command.scope,
                    returncode=outcome.exit_codes[command.worker_id],
                    report=None,
                    elapsed_seconds=max(
                        0.0, monotonic() - worker_started_at[key]
                    ),
                    stderr_bytes=(result.stderr_bytes if result is not None else 0),
                    stderr_sha256=(
                        result.stderr_sha256
                        if result is not None
                        else hashlib.sha256(b"").hexdigest()
                    ),
                    termination_reason=termination_reason,
                )
            )
        return

    reportable_failures = {
        (command.worker_id, command.iteration)
        for command in pending_commands
        if _is_reportable_container_worker_failure(
            outcome,
            terminal_results.get((command.worker_id, command.iteration)),
            outcome.exit_codes[command.worker_id],
        )
    }
    for command in pending_commands:
        key = (command.worker_id, command.iteration)
        result = terminal_results.get(key)
        returncode = outcome.exit_codes[command.worker_id]
        if key in reportable_failures:
            on_outcome(
                WorkerOutcome(
                    worker_id=command.worker_id,
                    iteration=command.iteration,
                    scope=command.scope,
                    returncode=returncode,
                    report=result.stdout_json,
                    elapsed_seconds=max(
                        0.0, monotonic() - worker_started_at[key]
                    ),
                    stderr_bytes=result.stderr_bytes,
                    stderr_sha256=result.stderr_sha256,
                )
            )
            continue
        if (
            reportable_failures
            and returncode == _CAPACITY_BOOTSTRAP_LIVENESS_EXIT_CODE
        ):
            on_outcome(
                WorkerOutcome(
                    worker_id=command.worker_id,
                    iteration=command.iteration,
                    scope=command.scope,
                    returncode=returncode,
                    report=None,
                    elapsed_seconds=max(
                        0.0, monotonic() - worker_started_at[key]
                    ),
                    stderr_bytes=(
                        result.stderr_bytes
                        if result is not None
                        and type(result.stderr_bytes) is int
                        and result.stderr_bytes >= 0
                        else 0
                    ),
                    stderr_sha256=(
                        result.stderr_sha256
                        if result is not None
                        else hashlib.sha256(b"").hexdigest()
                    ),
                    termination_reason="aborted_by_peer_failure",
                )
            )
            continue
        on_outcome(
            WorkerOutcome(
                worker_id=command.worker_id,
                iteration=command.iteration,
                scope=command.scope,
                returncode=returncode,
                report=None,
                elapsed_seconds=max(
                    0.0, monotonic() - worker_started_at[key]
                ),
                stderr_bytes=0,
                stderr_sha256=hashlib.sha256(b"").hexdigest(),
                launch_error=_safe_message(outcome.reason),
            )
        )


def _default_dependencies(
    deployment: ProductionDeployment,
) -> CapacityDependencies:
    def prepare_session_ownership(
        args: argparse.Namespace,
        *,
        monotonic: Callable[[], float],
        sleep: Callable[[float], None],
        worker_runtime: AdmittedWorkerRuntime,
    ) -> _SessionOwnershipLease:
        _scheduler_container_id, worker_image_id = _admitted_running_container(
            deployment, "airflow-scheduler"
        )

        def finalize_worker_artifacts(owner: str) -> bool:
            module = worker_runtime.container_runtime_module
            if module is None:
                return False
            module.cleanup_owner_host_artifacts(
                owner=_validate_session_owner(owner)
            )
            return worker_runtime.close()

        return _prepare_session_ownership(
            args,
            monotonic=monotonic,
            sleep=sleep,
            worker_image_id=worker_image_id,
            cleanup_stale_workers=lambda owner, stale_image_id: _cleanup_stale_capacity_workers(
                worker_runtime=worker_runtime,
                owner=owner,
                stale_worker_image_id=stale_image_id,
            ),
            finalize_worker_artifacts=finalize_worker_artifacts,
            browser_network_control=(
                _capacity_mode(args) == DIRECT_DIAGNOSTIC_MODE
            ),
        )

    return CapacityDependencies(
        monotonic=time.monotonic,
        sleep=time.sleep,
        inspect_containers=lambda names: _inspect_containers(names, deployment),
        sample_rss=_sample_process_rss,
        runtime_identity=lambda args: _runtime_identity(
            args, deployment=deployment
        ),
        run_round=lambda commands, **kwargs: _run_container_round(
            commands, deployment=deployment, **kwargs
        ),
        prepare_session_ownership=prepare_session_ownership,
        containerized_workers=True,
    )


def _scope_values(args: argparse.Namespace) -> tuple[str, ...]:
    raw = getattr(args, "scopes", None)
    values = raw if raw else DEFAULT_SCOPES
    return tuple(str(value).strip() for value in values)


def _capacity_mode(args: argparse.Namespace) -> str:
    return str(getattr(args, "mode", DIRECT_DIAGNOSTIC_MODE))


def _container_values(args: argparse.Namespace) -> tuple[str, ...]:
    raw = getattr(args, "containers", None)
    values = (*DEFAULT_CONTAINERS, *(raw or ()))
    return tuple(dict.fromkeys(str(value).strip() for value in values))


def _runtime_identity_label(path: Path) -> str:
    try:
        return path.relative_to(REPO_ROOT).as_posix()
    except ValueError:
        return f"external:{path.name}"


def _read_admitted_worker_bytes(path: Path, *, label: str) -> bytes:
    """Capture one root-owned regular file without following any path symlink."""

    path = Path(path)
    if (
        not path.is_absolute()
        or not path.name
        or path != Path(os.path.abspath(path))
    ):
        raise RuntimeError(f"worker runtime path is not canonical: {label}")
    directory_flags = (
        os.O_RDONLY
        | os.O_DIRECTORY
        | getattr(os, "O_CLOEXEC", 0)
        | getattr(os, "O_NOFOLLOW", 0)
    )
    file_flags = (
        os.O_RDONLY
        | getattr(os, "O_CLOEXEC", 0)
        | getattr(os, "O_NOFOLLOW", 0)
        | getattr(os, "O_NONBLOCK", 0)
    )
    directory_fd = os.open("/", directory_flags)
    file_fd = -1
    try:
        for component in path.parts[1:-1]:
            child_fd = os.open(component, directory_flags, dir_fd=directory_fd)
            os.close(directory_fd)
            directory_fd = child_fd
            metadata = os.fstat(directory_fd)
            sticky_root_directory = (
                metadata.st_uid == 0
                and metadata.st_mode & stat.S_ISVTX
                and metadata.st_mode & 0o002
            )
            if metadata.st_uid != 0 or (
                metadata.st_mode & 0o022 and not sticky_root_directory
            ):
                raise RuntimeError(
                    f"worker runtime parent metadata is invalid: {label}"
                )
        file_fd = os.open(path.name, file_flags, dir_fd=directory_fd)
        before = os.fstat(file_fd)
        if (
            not stat.S_ISREG(before.st_mode)
            or before.st_uid != 0
            or before.st_nlink != 1
            or before.st_mode & 0o022
        ):
            raise RuntimeError(f"worker runtime file metadata is invalid: {label}")
        chunks: list[bytes] = []
        size = 0
        while chunk := os.read(file_fd, 1024 * 1024):
            size += len(chunk)
            if size > 16 * 1024 * 1024:
                raise RuntimeError(f"worker runtime file is too large: {label}")
            chunks.append(chunk)
        after = os.fstat(file_fd)
        entry = os.stat(path.name, dir_fd=directory_fd, follow_symlinks=False)
    except OSError as exc:
        raise RuntimeError(f"worker runtime file is unavailable: {label}") from exc
    finally:
        if file_fd >= 0:
            os.close(file_fd)
        os.close(directory_fd)
    identity_fields = _PROTECTED_INPUT_IDENTITY_FIELDS
    identity = tuple(getattr(before, field) for field in identity_fields)
    if (
        identity != tuple(getattr(after, field) for field in identity_fields)
        or identity != tuple(getattr(entry, field) for field in identity_fields)
        or size != before.st_size
    ):
        raise RuntimeError(f"worker runtime file changed while read: {label}")
    return b"".join(chunks)


def _sealed_memfd(name: str, payload: bytes, *, executable: bool = False) -> int:
    if not payload:
        raise RuntimeError("worker runtime sealed payload is empty")
    required = (
        getattr(os, "MFD_ALLOW_SEALING", 0),
        getattr(fcntl, "F_ADD_SEALS", 0),
        getattr(fcntl, "F_GET_SEALS", 0),
        getattr(fcntl, "F_SEAL_SEAL", 0),
        getattr(fcntl, "F_SEAL_SHRINK", 0),
        getattr(fcntl, "F_SEAL_GROW", 0),
        getattr(fcntl, "F_SEAL_WRITE", 0),
    )
    if not hasattr(os, "memfd_create") or any(value == 0 for value in required):
        raise RuntimeError("sealed memfd worker runtime is unavailable")
    descriptor = os.memfd_create(
        name,
        flags=getattr(os, "MFD_CLOEXEC", 0) | os.MFD_ALLOW_SEALING,
    )
    try:
        view = memoryview(payload)
        while view:
            written = os.write(descriptor, view)
            if written <= 0:
                raise RuntimeError("worker runtime memfd accepted no bytes")
            view = view[written:]
        os.lseek(descriptor, 0, os.SEEK_SET)
        os.fchmod(descriptor, 0o500 if executable else 0o400)
        seals = (
            fcntl.F_SEAL_WRITE
            | fcntl.F_SEAL_GROW
            | fcntl.F_SEAL_SHRINK
            | fcntl.F_SEAL_SEAL
        )
        fcntl.fcntl(descriptor, fcntl.F_ADD_SEALS, seals)
        if fcntl.fcntl(descriptor, fcntl.F_GET_SEALS) != seals:
            raise RuntimeError("worker runtime memfd seals are incomplete")
        os.set_inheritable(descriptor, False)
        return descriptor
    except BaseException:
        os.close(descriptor)
        raise


def _deterministic_worker_zip(files: Mapping[str, bytes]) -> bytes:
    output = io.BytesIO()
    with zipfile.ZipFile(
        output,
        mode="w",
        compression=zipfile.ZIP_STORED,
        allowZip64=False,
    ) as archive:
        for relative, payload in sorted(files.items()):
            info = zipfile.ZipInfo(relative, date_time=(1980, 1, 1, 0, 0, 0))
            info.create_system = 3
            info.external_attr = 0o100400 << 16
            info.compress_type = zipfile.ZIP_STORED
            archive.writestr(info, payload)
    payload = output.getvalue()
    with zipfile.ZipFile(io.BytesIO(payload), mode="r") as archive:
        if archive.testzip() is not None:
            raise RuntimeError("worker runtime zip integrity check failed")
    return payload


def _trusted_worker_site_packages() -> Path:
    path = (
        Path(sys.prefix)
        / "lib"
        / f"python{sys.version_info.major}.{sys.version_info.minor}"
        / "site-packages"
    )
    if path != Path(os.path.abspath(path)):
        raise RuntimeError("worker dependency site-packages path is invalid")
    current = Path("/")
    try:
        for component in path.parts[1:]:
            current /= component
            metadata = current.lstat()
            if (
                not stat.S_ISDIR(metadata.st_mode)
                or metadata.st_uid != 0
                or metadata.st_mode & 0o022
            ):
                raise RuntimeError(
                    "worker dependency site-packages metadata is invalid"
                )
    except OSError as exc:
        raise RuntimeError("worker dependency site-packages is unavailable") from exc
    return path


def _load_worker_runtime_contract(raw: bytes) -> Mapping[str, str]:
    def unique_object(pairs: list[tuple[str, Any]]) -> dict[str, Any]:
        result: dict[str, Any] = {}
        for key, value in pairs:
            if key in result:
                raise ValueError("duplicate worker runtime contract key")
            result[key] = value
        return result

    try:
        document = json.loads(raw.decode("utf-8"), object_pairs_hook=unique_object)
    except (UnicodeDecodeError, ValueError, json.JSONDecodeError) as exc:
        raise RuntimeError("worker runtime contract is invalid") from exc
    if not isinstance(document, dict) or set(document) != {
        "business_dataset_count",
        "files",
        "parser_version",
        "report_schema_version",
        "schema_version",
    } or document.get("schema_version") != 1:
        raise RuntimeError("worker runtime contract schema is invalid")
    files = document.get("files")
    if not isinstance(files, dict) or any(
        not isinstance(relative, str)
        or not isinstance(digest, str)
        or re.fullmatch(r"[0-9a-f]{64}", digest) is None
        for relative, digest in files.items()
    ):
        raise RuntimeError("worker runtime contract file map is invalid")
    return files


class _ExactRuntimeTreeOwner:
    """Idempotent cleanup for one owner-derived private runtime directory."""

    def __init__(self, path: Path) -> None:
        metadata = path.lstat()
        self.name = str(path)
        self._identity = (metadata.st_dev, metadata.st_ino)
        self._cleaned = False

    def cleanup(self) -> None:
        if self._cleaned:
            return
        path = Path(self.name)
        try:
            metadata = path.lstat()
        except FileNotFoundError:
            self._cleaned = True
            return
        if (
            not stat.S_ISDIR(metadata.st_mode)
            or metadata.st_uid != 0
            or metadata.st_gid != 0
            or stat.S_IMODE(metadata.st_mode) != 0o700
            or (metadata.st_dev, metadata.st_ino) != self._identity
        ):
            raise OSError("worker runtime owner directory identity changed")
        shutil.rmtree(path)
        if path.exists():
            raise OSError("worker runtime owner directory remained after cleanup")
        self._cleaned = True


def _runtime_tree_owner(session_owner: Optional[str]) -> Any:
    if session_owner is None:
        return TemporaryDirectory(prefix="whoscored-capacity-runtime-")
    owner = _validate_session_owner(session_owner)
    parent = Path("/tmp")
    metadata = parent.lstat()
    if (
        not stat.S_ISDIR(metadata.st_mode)
        or metadata.st_uid != 0
        or metadata.st_gid != 0
        or not metadata.st_mode & stat.S_ISVTX
    ):
        raise RuntimeError("capacity runtime parent directory is unsafe")
    path = parent / f"{_HOST_RUNTIME_OWNER_PREFIX}{owner}"
    created = False
    try:
        path.mkdir(mode=0o700)
        created = True
        os.chown(path, 0, 0)
        os.chmod(path, 0o700)
    except OSError as exc:
        if created:
            shutil.rmtree(path, ignore_errors=True)
        raise RuntimeError("capacity runtime owner directory is unavailable") from exc
    return _ExactRuntimeTreeOwner(path)


def _materialize_worker_runtime_tree(
    files: Mapping[str, bytes], *, session_owner: Optional[str] = None
) -> tuple[Any, Path, Path, str]:
    """Create one private, read-only tree for an exact image worker bind."""

    if not files:
        raise RuntimeError("worker runtime tree is empty")
    normalized: dict[str, bytes] = {}
    for raw_relative, payload in files.items():
        relative = PurePosixPath(raw_relative)
        if (
            not raw_relative
            or relative.is_absolute()
            or relative.as_posix() != raw_relative
            or any(part in {"", ".", ".."} for part in relative.parts)
            or not payload
        ):
            raise RuntimeError("worker runtime tree member is invalid")
        normalized[raw_relative] = payload
    if len(normalized) != len(files):
        raise RuntimeError("worker runtime tree members are duplicated")

    owner = _runtime_tree_owner(session_owner)
    root = Path(owner.name) / "root"
    try:
        root.mkdir(mode=0o755)
        directories = {root}
        for relative, payload in sorted(normalized.items()):
            target = root.joinpath(*PurePosixPath(relative).parts)
            parent = target.parent
            parent.mkdir(mode=0o755, parents=True, exist_ok=True)
            current = parent
            while current != root:
                directories.add(current)
                current = current.parent
            descriptor = os.open(
                target,
                os.O_WRONLY
                | os.O_CREAT
                | os.O_EXCL
                | getattr(os, "O_CLOEXEC", 0)
                | getattr(os, "O_NOFOLLOW", 0),
                0o400,
            )
            try:
                view = memoryview(payload)
                while view:
                    written = os.write(descriptor, view)
                    if written <= 0:  # pragma: no cover - defensive filesystem contract
                        raise OSError("worker runtime snapshot accepted no bytes")
                    view = view[written:]
                os.fsync(descriptor)
                os.fchmod(descriptor, 0o444)
                metadata = os.fstat(descriptor)
            finally:
                os.close(descriptor)
            if (
                not stat.S_ISREG(metadata.st_mode)
                or metadata.st_uid != 0
                or metadata.st_nlink != 1
                or stat.S_IMODE(metadata.st_mode) != 0o444
                or metadata.st_size != len(payload)
            ):
                raise RuntimeError("worker runtime snapshot metadata is invalid")
        directories.add(root)
        for directory in sorted(directories, key=lambda item: len(item.parts), reverse=True):
            os.chmod(directory, 0o555)
            descriptor = os.open(
                directory,
                os.O_RDONLY
                | os.O_DIRECTORY
                | getattr(os, "O_CLOEXEC", 0)
                | getattr(os, "O_NOFOLLOW", 0),
            )
            try:
                metadata = os.fstat(descriptor)
                os.fsync(descriptor)
            finally:
                os.close(descriptor)
            if (
                not stat.S_ISDIR(metadata.st_mode)
                or metadata.st_uid != 0
                or stat.S_IMODE(metadata.st_mode) != 0o555
            ):
                raise RuntimeError("worker runtime snapshot directory is mutable")
        aggregate = hashlib.sha256(b"whoscored-capacity-runtime-tree-v1\0")
        for relative, expected in sorted(normalized.items()):
            captured = _read_admitted_worker_bytes(
                root / relative, label=f"snapshot:{relative}"
            )
            if captured != expected:
                raise RuntimeError("worker runtime snapshot differs from admitted bytes")
            aggregate.update(relative.encode("utf-8"))
            aggregate.update(b"\0")
            aggregate.update(hashlib.sha256(captured).digest())
        source_circuit_root = Path(owner.name) / "source-circuit"
        source_circuit_root.mkdir(mode=0o770)
        os.chmod(source_circuit_root, 0o770)
        source_metadata = source_circuit_root.stat()
        if (
            not stat.S_ISDIR(source_metadata.st_mode)
            or source_metadata.st_uid != 0
            or source_metadata.st_gid != 0
            or stat.S_IMODE(source_metadata.st_mode) != 0o770
        ):
            raise RuntimeError("worker source-circuit directory is invalid")
        return owner, root, source_circuit_root, aggregate.hexdigest()
    except BaseException:
        # TemporaryDirectory cannot remove a 0555 tree until owner permissions
        # are restored.  Make only this private tree writable for cleanup.
        for directory, child_directories, _files in os.walk(
            root, topdown=False, followlinks=False
        ):
            for child in child_directories:
                try:
                    os.chmod(Path(directory) / child, 0o700)
                except OSError:
                    pass
            try:
                os.chmod(directory, 0o700)
            except OSError:
                pass
        owner.cleanup()
        raise


def _load_admitted_container_runtime(
    source: bytes, *, source_path: Path
) -> tuple[types.ModuleType, str]:
    """Execute the contract-pinned container supervisor from captured bytes."""

    if not source or not source_path.is_absolute():
        raise RuntimeError("container runtime source is invalid")
    source_sha256 = hashlib.sha256(source).hexdigest()
    module_name = (
        "_whoscored_capacity_container_runtime_"
        f"{source_sha256[:16]}_{secrets.token_hex(8)}"
    )
    module = types.ModuleType(module_name)
    module.__file__ = str(source_path)
    module.__package__ = ""
    module.__source_sha256__ = source_sha256
    sys.modules[module_name] = module
    try:
        code = compile(source, str(source_path), "exec", dont_inherit=True)
        exec(code, module.__dict__)
        required_callables = (
            "WorkerSpec",
            "cleanup_owner_host_artifacts",
            "cleanup_stale_owner_containers",
            "run_capacity_containers",
        )
        if any(not callable(getattr(module, name, None)) for name in required_callables):
            raise RuntimeError("container runtime API is incomplete")
        if (
            getattr(module, "WORKER_COUNT", None) != WORKER_COUNT
            or getattr(module, "WORKLOAD_PATH", None) != _CONTAINER_WORKFLOW_PATH
            or getattr(module, "EXPECTED_PYTHON", None) != "3.11"
            or getattr(module, "EXPECTED_CURL_CFFI", None)
            != REQUIRED_CURL_CFFI_VERSION
            or getattr(module, "DOCKER_CLI", None) != str(_LOCAL_DOCKER_CLI)
            or getattr(module, "HOST_RUNTIME_OWNER_PREFIX", None)
            != _HOST_RUNTIME_OWNER_PREFIX
        ):
            raise RuntimeError("container runtime constants are not admitted")
    except BaseException:
        if sys.modules.get(module_name) is module:
            del sys.modules[module_name]
        raise
    return module, module_name


def _admit_worker_runtime(
    args: argparse.Namespace,
    *,
    expected_identity: Mapping[str, Any],
    containerized: bool = False,
) -> AdmittedWorkerRuntime:
    actual_whoscored_modules = tuple(
        sorted(
            path.relative_to(REPO_ROOT).as_posix()
            for path in (REPO_ROOT / "scrapers/whoscored").glob("*.py")
        )
    )
    expected_whoscored_modules = tuple(
        relative
        for relative in _WORKER_BUNDLE_PATHS
        if relative.startswith("scrapers/whoscored/") and relative.endswith(".py")
    )
    if actual_whoscored_modules != expected_whoscored_modules:
        raise RuntimeError("worker bundle does not cover the exact WhoScored package")
    required_worker_paths = (
        *_WORKER_BUNDLE_PATHS,
        _WORKER_WORKFLOW_RELATIVE,
        _WORKER_HELPER_RELATIVE,
    )
    lock_payload = _read_admitted_worker_bytes(
        REPO_ROOT / _WORKER_RUNTIME_CONTRACT_RELATIVE,
        label=_WORKER_RUNTIME_CONTRACT_RELATIVE,
    )
    runtime_contract = _load_worker_runtime_contract(lock_payload)
    runtime_tree_files: dict[str, bytes] = {}
    for relative, expected_sha256 in sorted(runtime_contract.items()):
        path = PurePosixPath(relative)
        if (
            path.is_absolute()
            or path.as_posix() != relative
            or any(part in {"", ".", ".."} for part in path.parts)
        ):
            raise RuntimeError("worker runtime contract path is invalid")
        payload = _read_admitted_worker_bytes(
            REPO_ROOT / relative, label=relative
        )
        if hashlib.sha256(payload).hexdigest() != expected_sha256:
            raise RuntimeError(
                f"worker runtime tree differs from runtime contract: {relative}"
            )
        runtime_tree_files[relative] = payload
    runtime_tree_files[_WORKER_RUNTIME_CONTRACT_RELATIVE] = lock_payload
    captured: dict[str, bytes] = {}
    for relative in required_worker_paths:
        payload = runtime_tree_files.get(relative)
        if payload is None:
            payload = _read_admitted_worker_bytes(REPO_ROOT / relative, label=relative)
        captured[relative] = payload
    catalog_path = Path(str(args.catalog))
    catalog_payload = _read_admitted_worker_bytes(catalog_path, label="catalog")
    python_path: Optional[Path] = None
    python_payload: Optional[bytes] = None
    unshare_payload: Optional[bytes] = None
    if not containerized:
        python_path = Path(sys.executable).resolve(strict=True)
        python_payload = _read_admitted_worker_bytes(
            python_path, label="worker Python interpreter"
        )
        unshare_payload = _read_admitted_worker_bytes(
            WORKER_NAMESPACE_EXECUTABLE,
            label="worker PID-namespace helper",
        )
        if hashlib.sha256(unshare_payload).hexdigest() != REQUIRED_UNSHARE_SHA256:
            raise RuntimeError("worker PID-namespace helper hash mismatch")
    for relative in _WORKER_BUNDLE_PATHS:
        if relative == _WORKER_RUNTIME_CONTRACT_RELATIVE:
            continue
        expected_sha256 = runtime_contract.get(relative)
        actual_sha256 = hashlib.sha256(captured[relative]).hexdigest()
        if expected_sha256 != actual_sha256:
            raise RuntimeError(
                f"worker bundle differs from runtime contract: {relative}"
            )
    hashes = {
        relative: hashlib.sha256(payload).hexdigest()
        for relative, payload in captured.items()
    }
    catalog_label = _runtime_identity_label(catalog_path)
    hashes[catalog_label] = hashlib.sha256(catalog_payload).hexdigest()
    if python_path is not None and python_payload is not None and unshare_payload is not None:
        hashes[_runtime_identity_label(python_path)] = hashlib.sha256(
            python_payload
        ).hexdigest()
        hashes[_runtime_identity_label(WORKER_NAMESPACE_EXECUTABLE)] = hashlib.sha256(
            unshare_payload
        ).hexdigest()
    identity_hashes = expected_identity.get("file_sha256")
    if not isinstance(identity_hashes, Mapping) or any(
        identity_hashes.get(label) != digest for label, digest in hashes.items()
    ):
        raise RuntimeError("sealed worker bytes differ from admitted runtime identity")
    zip_files = {
        relative: captured[relative] for relative in _WORKER_BUNDLE_PATHS
    }
    zip_files["__main__.py"] = captured[_WORKER_WORKFLOW_RELATIVE]
    bundle_payload = _deterministic_worker_zip(zip_files)
    descriptors: list[int] = []
    runtime_tree_owner: Any = None
    container_module: Optional[types.ModuleType] = None
    container_module_name: Optional[str] = None
    try:
        if containerized:
            container_source = runtime_tree_files.get(
                _WORKER_CONTAINER_RUNTIME_RELATIVE
            )
            if container_source is None:
                raise RuntimeError("container runtime is absent from the contract")
            container_module, container_module_name = _load_admitted_container_runtime(
                container_source,
                source_path=REPO_ROOT / _WORKER_CONTAINER_RUNTIME_RELATIVE,
            )
            return AdmittedWorkerRuntime(
                bundle_fd=-1,
                helper_fd=-1,
                catalog_fd=-1,
                python_fd=-1,
                unshare_fd=-1,
                site_packages=Path(
                    "/home/airflow/.local/lib/python3.11/site-packages"
                ),
                file_sha256=hashes,
                bundle_sha256=hashlib.sha256(bundle_payload).hexdigest(),
                execution_mode="exact-scheduler-image-v1",
                container_runtime_module=container_module,
                container_runtime_module_name=container_module_name,
                pending_runtime_tree_files=dict(runtime_tree_files),
            )
        (
            runtime_tree_owner,
            runtime_root,
            source_circuit_root,
            runtime_tree_sha256,
        ) = _materialize_worker_runtime_tree(runtime_tree_files)
        descriptors.append(_sealed_memfd("whoscored-capacity-bundle", bundle_payload))
        descriptors.append(
            _sealed_memfd(
                "whoscored-capacity-bootstrap",
                captured[_WORKER_HELPER_RELATIVE],
            )
        )
        descriptors.append(
            _sealed_memfd("whoscored-capacity-catalog", catalog_payload)
        )
        descriptors.append(
            _sealed_memfd(
                "whoscored-capacity-python", python_payload, executable=True
            )
        )
        descriptors.append(
            _sealed_memfd(
                "whoscored-capacity-unshare", unshare_payload, executable=True
            )
        )
        return AdmittedWorkerRuntime(
            bundle_fd=descriptors[0],
            helper_fd=descriptors[1],
            catalog_fd=descriptors[2],
            python_fd=descriptors[3],
            unshare_fd=descriptors[4],
            site_packages=_trusted_worker_site_packages(),
            file_sha256=hashes,
            bundle_sha256=hashlib.sha256(bundle_payload).hexdigest(),
            runtime_root=runtime_root,
            source_circuit_root=source_circuit_root,
            runtime_tree_sha256=runtime_tree_sha256,
            runtime_tree_owner=runtime_tree_owner,
        )
    except BaseException:
        for descriptor in descriptors:
            os.close(descriptor)
        if (
            container_module is not None
            and container_module_name is not None
            and sys.modules.get(container_module_name) is container_module
        ):
            del sys.modules[container_module_name]
        if runtime_tree_owner is not None:
            runtime_tree_owner.cleanup()
        raise


def _materialize_admitted_container_runtime(
    runtime: AdmittedWorkerRuntime, *, session_owner: str
) -> None:
    if (
        runtime.execution_mode != "exact-scheduler-image-v1"
        or runtime.container_runtime_module is None
        or runtime.runtime_root is not None
        or runtime.source_circuit_root is not None
        or runtime.runtime_tree_owner is not None
        or not isinstance(runtime.pending_runtime_tree_files, Mapping)
    ):
        raise RuntimeError("pending container runtime cannot be materialized")
    owner = _validate_session_owner(session_owner)
    (
        runtime_tree_owner,
        runtime_root,
        source_circuit_root,
        runtime_tree_sha256,
    ) = _materialize_worker_runtime_tree(
        runtime.pending_runtime_tree_files, session_owner=owner
    )
    runtime.runtime_tree_owner = runtime_tree_owner
    runtime.runtime_root = runtime_root
    runtime.source_circuit_root = source_circuit_root
    runtime.runtime_tree_sha256 = runtime_tree_sha256
    runtime.session_owner = owner
    runtime.pending_runtime_tree_files = None


def _sealed_worker_argv(
    argv: Sequence[str], runtime: AdmittedWorkerRuntime
) -> tuple[str, ...]:
    if tuple(argv[:2]) != (sys.executable, str(WORKFLOW_SCRIPT)):
        raise RuntimeError("production worker command bypasses the sealed workflow")
    arguments = list(argv[2:])
    try:
        catalog_index = arguments.index("--catalog") + 1
    except ValueError as exc:
        raise RuntimeError("production worker command omits the sealed catalog") from exc
    if catalog_index >= len(arguments):
        raise RuntimeError("production worker catalog argument is incomplete")
    arguments[catalog_index] = f"/proc/self/fd/{runtime.catalog_fd}"
    return (
        f"/proc/self/fd/{runtime.python_fd}",
        "-I",
        "-S",
        "-B",
        "-u",
        f"/proc/self/fd/{runtime.bundle_fd}",
        *arguments,
    )


def _runtime_files(
    args: argparse.Namespace, *, include_host_worker_runtime: bool = True
) -> tuple[Path, ...]:
    catalog = Path(str(args.catalog)).resolve()
    files = {
        *(REPO_ROOT / relative for relative in _RUNTIME_STATIC_PATHS),
        *sorted((REPO_ROOT / "scrapers" / "whoscored").glob("*.py")),
        catalog,
    }
    if include_host_worker_runtime:
        files.update((Path(sys.executable).resolve(), WORKER_NAMESPACE_EXECUTABLE))
    missing = sorted(str(path) for path in files if not path.is_file())
    if missing:
        raise RuntimeError("runtime identity files are missing: " + ", ".join(missing))
    return tuple(sorted((path.resolve() for path in files), key=str))


def _run_local_git(arguments: Sequence[str]) -> subprocess.CompletedProcess[str]:
    try:
        metadata = _LOCAL_GIT_CLI.lstat()
    except OSError as exc:
        raise RuntimeError("trusted Git executable is unavailable") from exc
    if (
        not stat.S_ISREG(metadata.st_mode)
        or metadata.st_uid != 0
        or metadata.st_mode & 0o022
        or metadata.st_mode & 0o111 == 0
    ):
        raise RuntimeError("trusted Git executable metadata is invalid")
    if not arguments or any(type(value) is not str or not value for value in arguments):
        raise RuntimeError("trusted Git arguments are invalid")
    try:
        return subprocess.run(
            [str(_LOCAL_GIT_CLI), *arguments],
            cwd=REPO_ROOT,
            env=dict(_LOCAL_GIT_ENVIRONMENT),
            stdin=subprocess.DEVNULL,
            check=True,
            capture_output=True,
            text=True,
            timeout=10,
        )
    except (OSError, subprocess.SubprocessError) as exc:
        raise RuntimeError("cannot bind canary evidence to git revision") from exc


def _runtime_identity(
    args: argparse.Namespace,
    *,
    deployment: Optional[ProductionDeployment] = None,
) -> Mapping[str, Any]:
    files = _runtime_files(
        args, include_host_worker_runtime=deployment is None
    )
    hashes: dict[str, str] = {}
    repository_paths: list[str] = []
    for path in files:
        label = _runtime_identity_label(path)
        if not label.startswith("external:"):
            repository_paths.append(label)
        payload = _read_admitted_worker_bytes(path, label=label)
        hashes[label] = hashlib.sha256(payload).hexdigest()
    deployment_evidence: Optional[dict[str, Any]] = None
    if deployment is not None:
        expected_argument_paths = [
            (
                "deployment_attestation",
                "deployment attestation",
                deployment.deployment_attestation_path,
            ),
            (
                "common_digest_override",
                "common digest override",
                deployment.common_digest_override_path,
            ),
        ]
        if deployment.admission_mode == DIRECT_DIAGNOSTIC_MODE:
            expected_argument_paths.extend(
                (
                    (
                        "gateway_digest_override",
                        "gateway digest override",
                        deployment.gateway_digest_override_path,
                    ),
                    (
                        "provider_policy",
                        "provider policy",
                        deployment.provider_policy_path,
                    ),
                    (
                        "owner_secret_file",
                        "provider-policy owner key",
                        deployment.owner_secret_file_path,
                    ),
                    (
                        "deployment_admission_receipt",
                        "deployment admission receipt",
                        deployment.deployment_admission_receipt_path,
                    ),
                )
            )
        if any(
            expected is None
            or
            _absolute_evidence_path(
                getattr(args, argument, None), label=label
            )
            != expected
            for argument, label, expected in expected_argument_paths
        ):
            raise RuntimeError("production deployment evidence path changed")
        labels = [snapshot.label for snapshot in deployment.protected_inputs]
        if len(labels) != len(set(labels)):
            raise RuntimeError("production deployment input labels are duplicated")
        for expected in deployment.protected_inputs:
            current = _protected_input_snapshot(
                expected.path,
                label=expected.label,
                private=expected.private,
            )
            if current != expected:
                raise RuntimeError("production deployment evidence changed")
            hashes[f"external:{expected.label}"] = current.sha256
        deployment_evidence = deployment.evidence()
    revision_result = _run_local_git(("rev-parse", "HEAD"))
    status_result = _run_local_git(
        (
            "status",
            "--porcelain",
            "--untracked-files=all",
            "--",
            *repository_paths,
        )
    )
    worker_image_id: Optional[str] = None
    if deployment is None:
        python_executable = str(Path(sys.executable).resolve(strict=True))
        python_prefix = str(sys.prefix)
        python_version = sys.version.split()[0]
        dependency_versions = {"curl_cffi": _installed_curl_cffi_version()}
    else:
        scheduler_records = [
            record
            for record in deployment.running_admission.get("images", [])
            if isinstance(record, Mapping)
            and record.get("service") == "airflow-scheduler"
        ]
        if len(scheduler_records) != 1:
            raise RuntimeError("production scheduler image identity is unavailable")
        worker_image_id = scheduler_records[0].get("image_id")
        if (
            not isinstance(worker_image_id, str)
            or re.fullmatch(r"sha256:[0-9a-f]{64}", worker_image_id) is None
        ):
            raise RuntimeError("production scheduler image identity is invalid")
        python_executable = "/usr/local/bin/python"
        python_prefix = "/usr/local"
        python_version = "3.11"
        dependency_versions = {"curl_cffi": REQUIRED_CURL_CFFI_VERSION}
    identity_material = {
        "file_sha256": hashes,
        "python_executable": python_executable,
        "python_prefix": python_prefix,
        "python_version": python_version,
        "dependency_versions": dependency_versions,
        "worker_image_id": worker_image_id,
        "production_deployment": deployment_evidence,
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
        "python_version": python_version,
        "dependency_versions": dependency_versions,
        "worker_image_id": worker_image_id,
        "production_deployment": deployment_evidence,
    }


def _runtime_identity_pin_error(
    identity: Mapping[str, Any],
    *,
    deployment: Optional[ProductionDeployment] = None,
) -> Optional[str]:
    if identity.get("git_clean") is not True:
        return "runtime identity checkout is not clean"
    production_evidence = identity.get("production_deployment")
    if not isinstance(production_evidence, Mapping):
        return "runtime identity omitted production deployment evidence"
    if deployment is not None:
        if identity.get("git_revision") != deployment.release_revision:
            return "runtime identity release revision differs from admission"
        if dict(production_evidence) != deployment.evidence():
            return "runtime identity deployment evidence differs from admission"
        scheduler_records = [
            record
            for record in deployment.running_admission.get("images", [])
            if isinstance(record, Mapping)
            and record.get("service") == "airflow-scheduler"
        ]
        if (
            len(scheduler_records) != 1
            or identity.get("worker_image_id")
            != scheduler_records[0].get("image_id")
            or identity.get("python_executable") != "/usr/local/bin/python"
            or identity.get("python_prefix") != "/usr/local"
            or identity.get("python_version") != "3.11"
        ):
            return "runtime identity does not match the admitted scheduler image"
    dependency_versions = identity.get("dependency_versions")
    if not isinstance(dependency_versions, Mapping):
        return "runtime identity omitted dependency versions"
    if dependency_versions.get("curl_cffi") != REQUIRED_CURL_CFFI_VERSION:
        return (
            "runtime identity does not match production "
            f"curl_cffi=={REQUIRED_CURL_CFFI_VERSION}"
        )
    if deployment is None:
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
    mode = _capacity_mode(args)
    if mode not in {CACHE_CAPACITY_MODE, DIRECT_DIAGNOSTIC_MODE}:
        return "capacity mode is invalid"
    if mode == CACHE_CAPACITY_MODE and duration != DEFAULT_DURATION_SECONDS:
        return "cache-capacity-v1 requires exactly 21600 seconds"
    if not math.isfinite(sample_interval) or not 0.1 <= sample_interval <= 300:
        return "sample-interval-seconds must be in 0.1..300"
    if (
        mode == CACHE_CAPACITY_MODE
        and sample_interval != DEFAULT_SAMPLE_INTERVAL_SECONDS
    ):
        return "cache-capacity-v1 requires exactly 30 second sampling"
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
    catalog = Path(str(getattr(args, "catalog", "")))
    expected_catalog = REPO_ROOT / "configs/medallion/competitions.yaml"
    if catalog != expected_catalog:
        return "capacity catalog must be the canonical production catalog"
    try:
        deployment_attestation = _absolute_evidence_path(
            getattr(args, "deployment_attestation", None),
            label="deployment attestation",
        )
        base_evidence_paths = (
            deployment_attestation,
            _absolute_evidence_path(
                getattr(args, "common_digest_override", None),
                label="common digest override",
            ),
        )
        evidence_paths = base_evidence_paths
        if mode == DIRECT_DIAGNOSTIC_MODE:
            evidence_paths = (
                *base_evidence_paths,
                _absolute_evidence_path(
                    getattr(args, "gateway_digest_override", None),
                    label="gateway digest override",
                ),
                _absolute_evidence_path(
                    getattr(args, "provider_policy", None),
                    label="provider policy",
                ),
                _absolute_evidence_path(
                    getattr(args, "owner_secret_file", None),
                    label="provider-policy owner key",
                ),
                _absolute_evidence_path(
                    getattr(args, "deployment_admission_receipt", None),
                    label="deployment admission receipt",
                ),
            )
    except (TypeError, ValueError) as exc:
        return str(exc)
    if len(evidence_paths) != len(set(evidence_paths)):
        return "production deployment evidence files must be distinct"
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
    mode = _capacity_mode(args)
    scopes = _scope_values(args)
    commands: list[WorkerCommand] = []
    for worker_id in range(WORKER_COUNT):
        scope = scopes[(iteration * WORKER_COUNT + worker_id) % len(scopes)]
        argv = (
            sys.executable,
            str(WORKFLOW_SCRIPT),
            "--mode",
            mode,
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
                browser_session_owner=(
                    None if mode == CACHE_CAPACITY_MODE else owner
                ),
                flaresolverr_endpoint=(
                    None
                    if mode == CACHE_CAPACITY_MODE
                    else REQUIRED_FLARESOLVERR_ENDPOINT
                ),
                session_owner=owner,
            )
        )
    return commands


def _gate_documents(
    accumulator: CapacityAccumulator,
    *,
    elapsed_seconds: float,
    requested_duration_seconds: float,
    mode: str,
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
        and elapsed_seconds >= requested_duration_seconds
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
            "name": "network_isolation",
            "passed": (
                mode != CACHE_CAPACITY_MODE
                or (
                    accumulator.network_requests == 0
                    and not accumulator.traffic_evidence_violations
                )
            ),
            "observed_network_requests": accumulator.network_requests,
            "required_network_requests": 0,
        },
        {
            "name": "cache_seed",
            "passed": (
                mode != CACHE_CAPACITY_MODE
                or (
                    accumulator.seed_sha256 == EXPECTED_CACHE_SEED_SHA256
                    and not accumulator.seed_evidence_violations
                )
            ),
            "observed_seed_sha256": accumulator.seed_sha256,
            "required_seed_sha256": EXPECTED_CACHE_SEED_SHA256,
            "violations": list(accumulator.seed_evidence_violations),
        },
        {
            "name": "cache_cleanup",
            "passed": (
                mode != CACHE_CAPACITY_MODE
                or not accumulator.cleanup_evidence_violations
            ),
            "violations": list(accumulator.cleanup_evidence_violations),
        },
        {
            "name": "memory",
            "passed": accumulator.max_aggregate_memory_bytes <= MAX_RSS_BYTES,
            "observed_max_harness_rss_bytes": accumulator.max_harness_rss_bytes,
            "observed_max_container_memory_bytes": (
                accumulator.max_container_memory_bytes
            ),
            "observed_max_worker_container_memory_bytes": (
                accumulator.max_worker_container_memory_bytes
            ),
            "observed_max_worker_container_pids": (
                accumulator.max_worker_container_pids
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
            "required_entities": (
                ["matches", "previews", "profiles", "multistage"]
                if mode == CACHE_CAPACITY_MODE
                else ["schedule", "matches", "previews", "profiles"]
            ),
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


def _worker_runtime_evidence(
    runtime: Optional[AdmittedWorkerRuntime],
    identity: Mapping[str, Any],
) -> Optional[dict[str, Any]]:
    if runtime is None:
        return None
    return {
        "bundle_sha256": (
            runtime.bundle_sha256
            if runtime.execution_mode == "sealed-host-v1"
            else None
        ),
        "execution_mode": runtime.execution_mode,
        "file_count": len(runtime.file_sha256),
        "runtime_cleanup_complete": runtime.runtime_cleanup_complete,
        "runtime_tree_sha256": runtime.runtime_tree_sha256,
        "worker_image_id": identity.get("worker_image_id"),
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

    mode = _capacity_mode(args)
    validation_error = _validate_args(args)
    production_deployment: Optional[ProductionDeployment] = None
    if validation_error is None and dependencies is None:
        try:
            production_deployment = _validate_production_deployment(args)
        except Exception as exc:
            validation_error = (
                "production deployment validation failed: " + _safe_message(exc)
            )
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

    if dependencies is None:
        if production_deployment is None:
            return 2, {
                "canary_version": CANARY_VERSION,
                "status": "configuration_error",
                "error": "production deployment validation returned no binding",
                "publishes": False,
                "writes_bronze": False,
                "executes_ddl": False,
                "worker_count": WORKER_COUNT,
            }
        deps = _default_dependencies(production_deployment)
    else:
        deps = dependencies
    containers = _container_values(args)
    worker_runtime: Optional[AdmittedWorkerRuntime] = None
    pre_session_identity: Optional[dict[str, Any]] = None
    pre_session_containers: Optional[dict[str, dict[str, Any]]] = None
    if production_deployment is not None:
        try:
            pre_session_identity = dict(deps.runtime_identity(args))
            pin_error = _runtime_identity_pin_error(
                pre_session_identity,
                deployment=production_deployment,
            )
            if pin_error:
                raise RuntimeError(pin_error)
            pre_session_containers = {
                name: dict(value)
                for name, value in deps.inspect_containers(containers).items()
            }
            violations = [
                *_container_gate_violations(
                    pre_session_containers, pre_session_containers
                ),
                *_container_admission_violations(
                    pre_session_containers, containers
                ),
            ]
            if violations:
                raise RuntimeError("; ".join(sorted(set(violations))))
        except Exception as exc:
            return 2, {
                "canary_version": CANARY_VERSION,
                "status": "configuration_error",
                "error": "pre-session production admission failed: "
                + _safe_message(exc),
                "publishes": False,
                "writes_bronze": False,
                "executes_ddl": False,
                "worker_count": WORKER_COUNT,
                "workers_launched": 0,
            }
        try:
            assert pre_session_identity is not None
            worker_runtime = _admit_worker_runtime(
                args,
                expected_identity=pre_session_identity,
                containerized=True,
            )
        except Exception as exc:
            return 2, {
                "canary_version": CANARY_VERSION,
                "status": "configuration_error",
                "error": "sealed worker runtime admission failed: "
                + _safe_message(exc),
                "publishes": False,
                "writes_bronze": False,
                "executes_ddl": False,
                "worker_count": WORKER_COUNT,
                "workers_launched": 0,
            }
    prepare_ownership = (
        deps.prepare_session_ownership or _prepare_noop_session_ownership
    )
    try:
        ownership_arguments: dict[str, Any] = {
            "monotonic": deps.monotonic,
            "sleep": deps.sleep,
        }
        if deps.containerized_workers:
            if worker_runtime is None:
                raise RuntimeError("container worker runtime was not admitted")
            ownership_arguments["worker_runtime"] = worker_runtime
        ownership = prepare_ownership(args, **ownership_arguments)
    except _SessionOwnershipPreparationError as exc:
        if worker_runtime is not None:
            worker_runtime.close()
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
    except Exception as exc:
        if worker_runtime is not None:
            worker_runtime.close()
        return 2, {
            "canary_version": CANARY_VERSION,
            "status": "configuration_error",
            "error": "session/worker ownership preparation failed: "
            + _safe_message(exc),
            "publishes": False,
            "writes_bronze": False,
            "executes_ddl": False,
            "worker_count": WORKER_COUNT,
            "workers_launched": 0,
        }

    if deps.containerized_workers:
        assert worker_runtime is not None
        try:
            _materialize_admitted_container_runtime(
                worker_runtime, session_owner=ownership.owner
            )
        except Exception as exc:
            try:
                cleanup_evidence = dict(ownership.abort_before_workers())
            except Exception as cleanup_exc:
                cleanup_evidence = _ownership_evidence(
                    lock_acquired=True,
                    preflight=ownership.preflight,
                    final={
                        **_empty_cleanup_result(required=True, verified=False),
                        "error_count": 1,
                        "error_sha256": [
                            _cleanup_error_hash("abort", cleanup_exc)
                        ],
                    },
                    state_file_removed=False,
                )
            finally:
                ownership.close()
                worker_runtime.close()
            return 2, {
                "canary_version": CANARY_VERSION,
                "status": "configuration_error",
                "error": "container runtime materialization failed: "
                + _safe_message(exc),
                "publishes": False,
                "writes_bronze": False,
                "executes_ddl": False,
                "worker_count": WORKER_COUNT,
                "workers_launched": 0,
                "session_cleanup": cleanup_evidence,
            }

    accumulator = CapacityAccumulator()
    if pre_session_containers is not None:
        accumulator.baseline_containers = pre_session_containers
    duration = float(args.duration_seconds)
    sample_interval = float(args.sample_interval_seconds)
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
        pin_error = _runtime_identity_pin_error(
            current,
            deployment=production_deployment,
        )
        if pin_error and pin_error not in accumulator.runtime_identity_violations:
            accumulator.runtime_identity_violations.append(pin_error)
        current_hashes = current.get("file_sha256")
        if worker_runtime is not None and (
            not isinstance(current_hashes, Mapping)
            or any(
                current_hashes.get(label) != digest
                for label, digest in worker_runtime.file_sha256.items()
            )
        ):
            message = "runtime identity differs from sealed worker bytes"
            if message not in accumulator.runtime_identity_violations:
                accumulator.runtime_identity_violations.append(message)
        for field_name in (
            "git_revision",
            "manifest_sha256",
            "python_executable",
            "python_prefix",
            "dependency_versions",
            "production_deployment",
        ):
            if current.get(field_name) != accumulator.runtime_identity.get(field_name):
                message = f"runtime identity changed: {field_name}"
                if message not in accumulator.runtime_identity_violations:
                    accumulator.runtime_identity_violations.append(message)
        if accumulator.runtime_identity_violations:
            stop_reasons.add("runtime_identity")

    def record_container_violations(
        current: Mapping[str, Mapping[str, Any]],
    ) -> list[str]:
        violations = [
            *_container_gate_violations(
                accumulator.baseline_containers, current
            ),
            *_container_admission_violations(current, containers),
        ]
        for violation in sorted(set(violations)):
            if violation not in accumulator.container_violations:
                accumulator.container_violations.append(violation)
        if violations:
            stop_reasons.add("container_restart_oom")
        return violations

    def verify_before_launch() -> None:
        verify_runtime_identity()
        if accumulator.runtime_identity_violations:
            raise RuntimeError("runtime identity changed before worker launch")
        try:
            current = {
                name: dict(value)
                for name, value in deps.inspect_containers(containers).items()
            }
            violations = record_container_violations(current)
        except Exception as exc:
            message = "pre-launch container admission failed: " + _safe_message(exc)
            if message not in accumulator.container_violations:
                accumulator.container_violations.append(message)
            stop_reasons.add("container_restart_oom")
            raise RuntimeError(message) from exc
        if violations:
            raise RuntimeError("container identity changed before worker launch")

    def capture_sample(force: bool = False) -> None:
        nonlocal last_sample_at
        now = deps.monotonic()
        record_sample = bool(
            force
            or last_sample_at is None
            or now - last_sample_at >= sample_interval
        )
        worker_resource_sample = bool(
            worker_runtime is not None
            and worker_runtime.worker_container_evidence
        )
        if not record_sample and not worker_resource_sample:
            return
        try:
            verify_runtime_identity()
            current = {
                name: dict(value)
                for name, value in deps.inspect_containers(containers).items()
            }
            if not accumulator.baseline_containers:
                accumulator.baseline_containers = {
                    name: dict(value) for name, value in current.items()
                }
            record_container_violations(current)

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
            worker_container_memory_bytes = (
                _as_nonnegative_int(
                    worker_runtime.worker_container_memory_bytes,
                    field_name="worker_container_memory_bytes",
                )
                if worker_runtime is not None
                else 0
            )
            worker_container_pids = (
                _as_nonnegative_int(
                    worker_runtime.worker_container_pids,
                    field_name="worker_container_pids",
                )
                if worker_runtime is not None
                else 0
            )
            container_memory_bytes += worker_container_memory_bytes
            aggregate_memory_bytes = rss_bytes + container_memory_bytes
            accumulator.max_harness_rss_bytes = max(
                accumulator.max_harness_rss_bytes, rss_bytes
            )
            accumulator.max_container_memory_bytes = max(
                accumulator.max_container_memory_bytes, container_memory_bytes
            )
            accumulator.max_worker_container_memory_bytes = max(
                accumulator.max_worker_container_memory_bytes,
                worker_container_memory_bytes,
            )
            accumulator.max_worker_container_pids = max(
                accumulator.max_worker_container_pids,
                worker_container_pids,
            )
            accumulator.max_aggregate_memory_bytes = max(
                accumulator.max_aggregate_memory_bytes, aggregate_memory_bytes
            )
            if aggregate_memory_bytes > MAX_RSS_BYTES:
                stop_reasons.add("memory")

            if record_sample:
                if len(accumulator.samples) >= MAX_RETAINED_SAMPLES:
                    raise RuntimeError("capacity sample retention bound exceeded")
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
                    "worker_container_memory_bytes": (
                        worker_container_memory_bytes
                    ),
                    "worker_container_pids": worker_container_pids,
                    "aggregate_memory_bytes": aggregate_memory_bytes,
                    "rss_process_count": _as_nonnegative_int(
                        rss.get("process_count", 0), field_name="process_count"
                    ),
                    "containers": [current[name] for name in containers],
                    "worker_containers": (
                        [
                            dict(value)
                            for value in worker_runtime.worker_container_evidence
                        ]
                        if worker_runtime is not None
                        else []
                    ),
                    }
                )
                last_sample_at = now
        except Exception as exc:
            message = _safe_message(exc)
            if message not in accumulator.monitoring_errors:
                accumulator.monitoring_errors.append(message)
            stop_reasons.add("monitoring_evidence")

    def accept_outcome(outcome: WorkerOutcome) -> None:
        _accept_outcome(accumulator, outcome, expected_mode=mode)
        if accumulator.paid_bytes or accumulator.paid_route_requests:
            stop_reasons.add("paid_traffic")
        if mode == CACHE_CAPACITY_MODE and accumulator.network_requests:
            stop_reasons.add("network_isolation")
        if accumulator.traffic_evidence_violations:
            stop_reasons.add("paid_traffic")
        if accumulator.seed_evidence_violations:
            stop_reasons.add("cache_seed")
        if accumulator.cleanup_evidence_violations:
            stop_reasons.add("cache_cleanup")
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
            current_identity = dict(deps.runtime_identity(args))
            pin_error = _runtime_identity_pin_error(
                current_identity,
                deployment=production_deployment,
            )
            if pin_error:
                accumulator.runtime_identity_violations.append(pin_error)
                stop_reasons.add("runtime_identity")
            elif (
                pre_session_identity is not None
                and current_identity != pre_session_identity
            ):
                accumulator.runtime_identity_violations.append(
                    "runtime identity changed during session preparation"
                )
                stop_reasons.add("runtime_identity")
            else:
                accumulator.runtime_identity = (
                    pre_session_identity or current_identity
                )
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
            round_arguments: dict[str, Any] = {}
            if worker_runtime is not None:
                round_arguments["worker_runtime"] = worker_runtime
            deps.run_round(
                commands,
                deadline=deadline,
                on_sample=capture_sample,
                on_outcome=accept_outcome,
                should_stop=lambda: bool(stop_reasons),
                before_launch=verify_before_launch,
                monotonic=deps.monotonic,
                sleep=deps.sleep,
                **round_arguments,
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
                        if worker_runtime is not None:
                            worker_runtime.close()
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
        mode=mode,
    )
    gates.append(cleanup_gate)
    worker_runtime_cleanup_ok = (
        worker_runtime is None or worker_runtime.runtime_cleanup_complete
    )
    gates.append(
        {
            "name": "worker_runtime_cleanup",
            "passed": worker_runtime_cleanup_ok,
        }
    )
    status = "success" if all(gate["passed"] for gate in gates) else "failed"

    baseline_restarts = {
        name: int(value.get("restart_count", 0) or 0)
        for name, value in accumulator.baseline_containers.items()
    }
    observed_restart_deltas: dict[str, int] = {
        name: 0 for name in baseline_restarts
    }
    oom_killed = False
    for sample in accumulator.samples:
        for container in (
            *(sample.get("containers") or ()),
            *(sample.get("worker_containers") or ()),
        ):
            if not isinstance(container, Mapping):
                continue
            oom_killed = oom_killed or container.get("oom_killed") is True
            name = str(container.get("name") or container.get("container_id") or "")
            if name in baseline_restarts:
                observed_restart_deltas[name] = max(
                    observed_restart_deltas[name],
                    max(
                        0,
                        int(container.get("restart_count", 0) or 0)
                        - baseline_restarts[name],
                    ),
                )
    restart_count = sum(observed_restart_deltas.values())
    cache_cleanup_ok = not accumulator.cleanup_evidence_violations
    cleanup_status = (
        "success"
        if cleanup_gate["passed"] and worker_runtime_cleanup_ok and cache_cleanup_ok
        else "failed"
    )
    runtime_release_identity = {
        "release_revision": (
            production_deployment.release_revision
            if production_deployment is not None
            else accumulator.runtime_identity.get("git_revision")
        ),
        "manifest_sha256": accumulator.runtime_identity.get("manifest_sha256"),
        "worker_image_id": accumulator.runtime_identity.get("worker_image_id"),
        "git_clean": accumulator.runtime_identity.get("git_clean"),
    }
    report: dict[str, Any] = {
        "canary_version": CANARY_VERSION,
        "schema_version": CAPACITY_REPORT_SCHEMA_VERSION,
        "mode": mode,
        "status": status,
        "started_at": started_wall,
        "ended_at": _utc_now(),
        "duration_seconds_requested": duration,
        "duration_seconds_observed": round(elapsed, 3),
        "elapsed_seconds": round(elapsed, 3),
        "cleanup_elapsed_seconds": round(cleanup_elapsed, 3),
        "total_elapsed_seconds": round(total_elapsed, 3),
        "worker_count": WORKER_COUNT,
        "workers": WORKER_COUNT,
        "scopes": list(_scope_values(args)),
        "match_limit": int(args.match_limit),
        "profile_limit": int(args.profile_limit),
        "publishes": False,
        "writes_bronze": False,
        "executes_ddl": False,
        "raw_store_policy": (
            "exact content-addressed temporary cache"
            if mode == CACHE_CAPACITY_MODE
            else "per-process temporary local storage"
        ),
        "repository_policy": "per-process in-memory repository",
        "page_unit_definition": (
            "five reviewed cache source objects per successful workflow run "
            "(match, preview, profile, and two stage payloads); each object is one unit"
            if mode == CACHE_CAPACITY_MODE
            else "one unique successfully completed source target per workflow phase"
        ),
        "source_request_attempts": accumulator.source_request_attempts,
        "network_requests": accumulator.network_requests,
        "seed_sha256": accumulator.seed_sha256,
        "page_units": accumulator.page_units,
        "projected_page_units_per_day": round(
            _projected_page_units_per_day(accumulator.page_units, elapsed), 3
        ),
        "paid_bytes": accumulator.paid_bytes,
        "paid_proxy_bytes": accumulator.paid_bytes,
        "paid_route_requests": accumulator.paid_route_requests,
        "completed_runs": accumulator.completed_runs,
        "completed_by_worker": dict(accumulator.completed_by_worker),
        "completed_worker_seconds": round(accumulator.worker_seconds, 3),
        "max_source_stage_count": accumulator.max_source_stage_count,
        "deadline_truncations": accumulator.deadline_truncations,
        "max_harness_rss_bytes": accumulator.max_harness_rss_bytes,
        "max_container_memory_bytes": accumulator.max_container_memory_bytes,
        "max_worker_container_memory_bytes": (
            accumulator.max_worker_container_memory_bytes
        ),
        "max_worker_container_pids": accumulator.max_worker_container_pids,
        "max_aggregate_memory_bytes": accumulator.max_aggregate_memory_bytes,
        "peak_combined_rss_bytes": accumulator.max_aggregate_memory_bytes,
        "restart_count": restart_count,
        "oom_killed": oom_killed,
        "cleanup": {
            "status": cleanup_status,
            "cache_workspaces_removed": cache_cleanup_ok,
            "worker_runtime_removed": worker_runtime_cleanup_ok,
            "browser_sessions_removed": cleanup_gate["passed"],
        },
        "cleanup_status": cleanup_status,
        "production_deployment": (
            production_deployment.evidence()
            if production_deployment is not None
            else None
        ),
        "sealed_worker_runtime": _worker_runtime_evidence(
            worker_runtime, accumulator.runtime_identity
        ),
        "runtime_identity": accumulator.runtime_identity,
        "runtime_release_identity": runtime_release_identity,
        "session_cleanup": dict(cleanup_evidence),
        "baseline_containers": [
            accumulator.baseline_containers[name]
            for name in containers
            if name in accumulator.baseline_containers
        ],
        "stop_reasons": sorted(stop_reasons),
        "gates": gates,
        "samples": accumulator.samples,
        "run_summaries_total": accumulator.run_summaries_total,
        "run_summaries_retained": len(accumulator.run_summaries),
        "run_summaries_truncated": (
            accumulator.run_summaries_total > len(accumulator.run_summaries)
        ),
        "runs": accumulator.run_summaries,
    }
    # Prove that the supervisor itself emits one JSON-safe evidence document.
    report = _attach_report_sha256(report)
    return (0 if status == "success" else 1), report


def _write_report(path: Path, report: Mapping[str, Any]) -> None:
    payload = _canonical_json_bytes(report).decode("utf-8") + "\n"
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
        "--mode",
        choices=(CACHE_CAPACITY_MODE, DIRECT_DIAGNOSTIC_MODE),
        default=CACHE_CAPACITY_MODE,
        help=(
            "production is cache-capacity-v1; direct-diagnostic-v1 is not "
            "valid production capacity evidence"
        ),
    )
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
            "repeat to add a monitoring-only container; airflow-scheduler and "
            "flaresolverr are always required"
        ),
    )
    parser.add_argument(
        "--deployment-attestation",
        type=Path,
        required=True,
        help="root-owned ready-v1 deployment attestation used by admission",
    )
    parser.add_argument(
        "--common-digest-override",
        type=Path,
        required=True,
        help="root-owned common-project digest-only Compose file",
    )
    parser.add_argument(
        "--gateway-digest-override",
        type=Path,
        help=(
            "root-owned gateway-project digest-only Compose file; required "
            "only by direct-diagnostic-v1"
        ),
    )
    parser.add_argument(
        "--provider-policy",
        type=Path,
        help=(
            "active owner-signed provider-policy-v1; required only by "
            "direct-diagnostic-v1"
        ),
    )
    parser.add_argument(
        "--owner-secret-file",
        type=Path,
        help=(
            "root-owned provider-policy HMAC key; required only by "
            "direct-diagnostic-v1"
        ),
    )
    parser.add_argument(
        "--deployment-admission-receipt",
        type=Path,
        help=(
            "root-owned schema-v2 paid deployment receipt; required only by "
            "direct-diagnostic-v1"
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
            report = _attach_report_sha256(report)
            code = 1
    print(_canonical_json_bytes(report).decode("utf-8"))
    return code


if __name__ == "__main__":
    raise SystemExit(main())
