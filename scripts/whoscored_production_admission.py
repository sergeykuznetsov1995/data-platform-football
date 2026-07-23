#!/usr/bin/env python3
"""Bind a validated WhoScored release to immutable production containers.

The helper never creates, starts, restarts, or removes a protected workload
container.  Post-create admission runs one ephemeral, no-network, read-only,
capability-free probe from the attested scheduler digest solely to prove the
kernel applies ``docker-default`` in enforce mode.
"""

from __future__ import annotations

import sys as _bootstrap_sys


if __name__ == "__main__" and (
    not _bootstrap_sys.flags.isolated
    or not _bootstrap_sys.flags.no_site
    or not _bootstrap_sys.flags.ignore_environment
):
    _bootstrap_sys.modules["posix"]._exit(78)

import argparse
import errno
import hashlib
import hmac
import ipaddress
import json
import os
import re
import secrets
import stat
import subprocess
import types
from dataclasses import dataclass
from decimal import Decimal, InvalidOperation
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import Any, Callable, Mapping, Sequence


sys = _bootstrap_sys


_SCRIPT_PATH = Path(os.path.abspath(__file__))
_REPOSITORY_ROOT = _SCRIPT_PATH.parents[1]
_WHOSCORED_APPROVAL_PATH_RE = re.compile(
    r"/opt/airflow/secure/whoscored-approvals/"
    r"[A-Za-z0-9][A-Za-z0-9._:-]{0,127}\.json"
)


def _trusted_source_uids(*, require_protected: bool) -> frozenset[int]:
    """Return owners accepted for the exact sibling source load."""

    if require_protected:
        return frozenset({0})
    return frozenset({0, os.geteuid()})


def _load_exact_provenance_validator(*, require_protected: bool) -> types.ModuleType:
    """Execute the exact sibling validator without consulting import paths."""

    module_name = "_whoscored_exact_build_provenance_validator"
    if module_name in sys.modules:
        raise RuntimeError("WhoScored provenance validator module was preloaded")
    path = _REPOSITORY_ROOT / "scripts/validate_whoscored_build_provenance.py"
    components = path.absolute().parts[1:]
    directory_flags = os.O_RDONLY | os.O_CLOEXEC | os.O_DIRECTORY | os.O_NOFOLLOW
    file_flags = os.O_RDONLY | os.O_CLOEXEC | os.O_NOFOLLOW
    trusted_source_uids = _trusted_source_uids(require_protected=require_protected)
    parent_descriptor = os.open("/", directory_flags)
    descriptor = -1
    try:
        for component in components[:-1]:
            child = os.open(component, directory_flags, dir_fd=parent_descriptor)
            os.close(parent_descriptor)
            parent_descriptor = child
            parent = os.fstat(parent_descriptor)
            writable = parent.st_mode & 0o022
            sticky_root = (
                parent.st_uid == 0
                and parent.st_mode & stat.S_ISVTX
                and parent.st_mode & 0o002
            )
            if parent.st_uid not in trusted_source_uids or (
                writable and not sticky_root
            ):
                raise RuntimeError("WhoScored validator has an unsafe parent directory")
        descriptor = os.open(components[-1], file_flags, dir_fd=parent_descriptor)
        before = os.fstat(descriptor)
        if (
            not stat.S_ISREG(before.st_mode)
            or before.st_uid not in trusted_source_uids
            or before.st_nlink != 1
            or before.st_mode & 0o022
        ):
            raise RuntimeError("WhoScored provenance validator source is not protected")
        chunks: list[bytes] = []
        size = 0
        while True:
            chunk = os.read(descriptor, 1024 * 1024)
            if not chunk:
                break
            size += len(chunk)
            if size > 4 * 1024 * 1024:
                raise RuntimeError("WhoScored provenance validator source is too large")
            chunks.append(chunk)
        after = os.fstat(descriptor)
        entry = os.stat(components[-1], dir_fd=parent_descriptor, follow_symlinks=False)
        identity = (
            "st_dev",
            "st_ino",
            "st_mode",
            "st_uid",
            "st_size",
            "st_mtime_ns",
            "st_ctime_ns",
        )
        if any(
            getattr(before, field) != getattr(candidate, field)
            for candidate in (after, entry)
            for field in identity
        ):
            raise RuntimeError("WhoScored provenance validator changed while loading")
    finally:
        if descriptor >= 0:
            os.close(descriptor)
        os.close(parent_descriptor)
    module = types.ModuleType(module_name)
    module.__file__ = str(path)
    module.__package__ = ""
    sys.modules[module_name] = module
    try:
        exec(compile(b"".join(chunks), str(path), "exec"), module.__dict__)
    except BaseException:
        sys.modules.pop(module_name, None)
        raise
    if module.__file__ != str(path):
        raise RuntimeError("WhoScored provenance validator identity changed")
    module._whoscored_loaded_source_sha256 = hashlib.sha256(
        b"".join(chunks)
    ).hexdigest()
    return module


# Every privileged load stays root-only.  A non-root process may use its
# owner-protected checkout for offline validation, tests, and ``--help``;
# ``main`` rejects real admission unless it proves the privileged release.
try:
    provenance = _load_exact_provenance_validator(require_protected=os.geteuid() == 0)
except BaseException:
    if __name__ == "__main__":
        _bootstrap_sys.modules["posix"]._exit(78)
    raise


EXIT_CONFIG = 78
MAX_JSON_BYTES = 16 * 1024 * 1024
MAX_PROVIDER_QUOTA_RECEIPT_BYTES = 32 * 1024
MAX_PROVIDER_QUOTA_RECEIPT_AGE = timedelta(hours=24)
MAX_ROLLOUT_ACCEPTANCE_AGE = timedelta(hours=36)
ROLLOUT_ACCEPTANCE_TIMEOUT_SECONDS = 10 * 3600
MAX_CHARTER_HORIZON = timedelta(days=62)
_CURRENT_ROLLOUT_PATH = Path("/var/lib/data-platform/whoscored-authority/rollout.json")
_CURRENT_CHARTER_PATH = Path("/var/lib/data-platform/whoscored-authority/charter.json")
_ROLLOUT_MANIFEST_SCHEMA_VERSION = 4
_CHARTER_SCHEMA_VERSION = 5
_ROLLOUT_GENESIS_PROOF_SHA256 = hashlib.sha256(
    b"whoscored-rollout-promotion-genesis-v1"
).hexdigest()
_ROLLOUT_WAVE_CONTRACTS = {
    "wave-20": (20, False),
    "wave-70": (70, False),
    "wave-all": (2_000, True),
}
FBREF_CAMOUFOX_GEOIP_DATABASE_CONTAINER_PATH = (
    "/opt/airflow/secure/fbref-geoip/GeoLite2-City.mmdb"
)
FBREF_CAMOUFOX_GEOIP_DATABASE_SHA256 = (
    "0772278c513e6ab3c65e9ae53d6861f137ab696f91eec763a2e6fe76befd83b2"
)
FBREF_CAMOUFOX_GEOIP_DATABASE_SIZE = 66_164_133
_PROVIDER_QUOTA_RECEIPT_FIELDS = frozenset(
    {
        "schema_version",
        "status",
        "provider",
        "order_id",
        "plan",
        "quota_decimal_gb",
        "remaining_decimal_gb",
        "observed_at",
        "screenshot_path",
        "screenshot_sha256",
    }
)
_PROVIDER_POLICY_UNSIGNED_FIELDS = frozenset(
    {
        "schema_version",
        "source",
        "provider_id",
        "order_id",
        "plan_id",
        "valid_from",
        "valid_until",
        "receipt_sha256",
        "provider_quota_bytes",
        "safety_cap_bytes",
        "daily_cap_bytes",
        "monthly_cap_bytes",
        "order_cap_bytes",
        "signature_algorithm",
    }
)
_PROVIDER_POLICY_FIELDS = _PROVIDER_POLICY_UNSIGNED_FIELDS | {
    "document_sha256",
    "signature",
}
_ROLLOUT_MANIFEST_FIELDS = frozenset(
    {
        "schema_version",
        "cohort_id",
        "rollout_id",
        "wave_id",
        "max_scopes",
        "require_full_active",
        "ranked_scope_ids",
        "ranked_scope_ids_sha256",
        "ranking_basis_workload_sha256",
        "ranking_basis_scope_workloads",
        "runtime_sha256",
        "classifier_sha256",
        "promotion_acceptance_sha256",
        "promotion_terminal_receipt_sha256",
        "acceptance_mode",
        "bootstrap_slots",
        "capacity_receipt_sha256",
        "provider_order_cap_bytes",
    }
)
_CHARTER_UNSIGNED_FIELDS = frozenset(
    {
        "schema_version",
        "source",
        "provider_policy_sha256",
        "order_id",
        "billing_month",
        "cohort_id",
        "cohort_sha256",
        "rollout_id",
        "wave_id",
        "max_scopes",
        "require_full_active",
        "ranked_scope_ids_sha256",
        "runtime_sha256",
        "classifier_sha256",
        "promotion_acceptance_sha256",
        "promotion_terminal_receipt_sha256",
        "acceptance_mode",
        "bootstrap_slots",
        "capacity_receipt_sha256",
        "provider_order_cap_bytes",
        "valid_from",
        "valid_until",
        "daily_cap_bytes",
        "monthly_cap_bytes",
        "order_cap_bytes",
        "max_issuances",
        "signature_algorithm",
    }
)
_CHARTER_FIELDS = _CHARTER_UNSIGNED_FIELDS | {
    "document_sha256",
    "signature",
}
_ROLLOUT_ACCEPTANCE_AUTHORITY_FIELDS = frozenset(
    {
        "rollout_id",
        "wave_id",
        "max_scopes",
        "require_full_active",
        "cohort_sha256",
        "ranked_scope_ids_sha256",
        "runtime_sha256",
        "classifier_sha256",
        "promotion_acceptance_sha256",
        "promotion_terminal_receipt_sha256",
        "acceptance_mode",
        "bootstrap_slots",
        "capacity_receipt_sha256",
        "provider_order_cap_bytes",
    }
)
_ROLLOUT_AUTHORITY_REPORT_FIELDS = frozenset(
    {
        "authority",
        "authority_binding",
        "catalog_active_scope_count",
        "catalog_active_scopes_sha256",
        "charter_sha256",
        "cohort_id",
        "rollout_manifest_sha256",
    }
)
_CAPACITY_REPORT_FIELDS = frozenset(
    {
        "baseline_containers",
        "canary_version",
        "cleanup",
        "cleanup_status",
        "cleanup_elapsed_seconds",
        "completed_by_worker",
        "completed_runs",
        "completed_worker_seconds",
        "deadline_truncations",
        "duration_seconds_observed",
        "duration_seconds_requested",
        "elapsed_seconds",
        "ended_at",
        "executes_ddl",
        "gates",
        "match_limit",
        "max_aggregate_memory_bytes",
        "max_container_memory_bytes",
        "max_harness_rss_bytes",
        "max_source_stage_count",
        "max_worker_container_memory_bytes",
        "max_worker_container_pids",
        "mode",
        "network_requests",
        "oom_killed",
        "page_unit_definition",
        "page_units",
        "paid_bytes",
        "paid_proxy_bytes",
        "paid_route_requests",
        "peak_combined_rss_bytes",
        "production_deployment",
        "profile_limit",
        "projected_page_units_per_day",
        "publishes",
        "raw_store_policy",
        "report_sha256",
        "repository_policy",
        "restart_count",
        "runs",
        "run_summaries_retained",
        "run_summaries_total",
        "run_summaries_truncated",
        "runtime_identity",
        "runtime_release_identity",
        "samples",
        "schema_version",
        "scopes",
        "sealed_worker_runtime",
        "seed_sha256",
        "session_cleanup",
        "source_request_attempts",
        "started_at",
        "status",
        "stop_reasons",
        "total_elapsed_seconds",
        "worker_count",
        "workers",
        "writes_bronze",
    }
)
_CAPACITY_GATE_NAMES = frozenset(
    {
        "browser_session_cleanup",
        "cache_cleanup",
        "cache_seed",
        "container_restart_oom",
        "memory",
        "monitoring_evidence",
        "network_isolation",
        "non_publishing",
        "paid_traffic",
        "representative_workload",
        "runtime_identity",
        "sustained_duration",
        "throughput",
        "worker_health",
        "worker_runtime_cleanup",
    }
)
_CAPACITY_CANARY_VERSION = "whoscored-capacity-canary-v4"
_CAPACITY_MODE = "cache-capacity-v1"
_CAPACITY_SEED_SHA256 = (
    "eb3739448a42a00f3a5812c20a0885ea691306634b2922cda1dd2d7c8490fb4a"
)
_CAPACITY_DURATION_SECONDS = 21_600
_CAPACITY_WORKER_COUNT = 4
_CAPACITY_MIN_PAGE_UNITS_PER_DAY = 144_000
_CAPACITY_MAX_COMBINED_RSS_BYTES = 12 * 1024**3
_CAPACITY_MAX_RUN_SUMMARIES = 16
_CAPACITY_MAX_SAMPLES = 724
_CAPACITY_SAMPLE_INTERVAL_SECONDS = Decimal(30)
_CAPACITY_SAMPLE_CADENCE_TOLERANCE_SECONDS = Decimal("15")
_CAPACITY_FINAL_SAMPLE_TOLERANCE_SECONDS = Decimal("5")
_CAPACITY_WALL_CLOCK_TOLERANCE_SECONDS = Decimal("2")
_CAPACITY_PAGE_UNIT_DEFINITION = (
    "five reviewed cache source objects per successful workflow run "
    "(match, preview, profile, and two stage payloads); each object is one unit"
)
_CAPACITY_CURL_CFFI_VERSION = "0.15.0"
_CAPACITY_RUN_FIELDS = frozenset(
    {
        "cleanup_evidence_valid",
        "entities",
        "executes_ddl",
        "iteration",
        "mode",
        "network_requests",
        "page_units",
        "paid_bytes",
        "paid_route_requests",
        "process_elapsed_seconds",
        "publishes",
        "returncode",
        "scope",
        "seed_evidence_valid",
        "seed_sha256",
        "source_request_attempts",
        "source_stage_count",
        "status",
        "stderr_bytes",
        "stderr_sha256",
        "termination_reason",
        "traffic_evidence_valid",
        "worker_id",
        "workflow_elapsed_seconds",
        "writes_bronze",
    }
)
_CAPACITY_SAMPLE_FIELDS = frozenset(
    {
        "aggregate_memory_bytes",
        "completed_runs",
        "container_memory_bytes",
        "containers",
        "elapsed_seconds",
        "harness_rss_bytes",
        "page_units",
        "paid_bytes",
        "paid_route_requests",
        "projected_page_units_per_day",
        "rss_process_count",
        "source_request_attempts",
        "worker_container_memory_bytes",
        "worker_container_pids",
        "worker_containers",
    }
)
_CAPACITY_CONTAINER_FIELDS = frozenset(
    {
        "command_contract_ok",
        "compose_identity_ok",
        "healthy",
        "id",
        "image_id",
        "image_identity_contract_ok",
        "immutable_payload_contract_ok",
        "memory_limit_bytes",
        "memory_usage_bytes",
        "name",
        "oom_killed",
        "pid",
        "process_count",
        "production_admission_contract_ok",
        "published_endpoint_contract_ok",
        "restart_count",
        "running",
        "security_contract_ok",
        "status",
    }
)
_CAPACITY_WORKER_CONTAINER_FIELDS = frozenset(
    {
        "container_id",
        "exit_code",
        "iteration",
        "memory_usage_bytes",
        "oom_killed",
        "pids_current",
        "running",
        "status",
        "worker_id",
    }
)
_CAPACITY_RUNTIME_IDENTITY_FIELDS = frozenset(
    {
        "dependency_versions",
        "file_sha256",
        "git_clean",
        "git_revision",
        "manifest_sha256",
        "production_deployment",
        "python_executable",
        "python_prefix",
        "python_version",
        "worker_image_id",
    }
)
_CAPACITY_RUNTIME_CONTRACT_RELATIVE = "scrapers/whoscored/runtime_contract.lock"
_CAPACITY_RUNTIME_FILE_PATHS = frozenset(
    {
        ".dockerignore",
        "compose.seaweedfs-supervised.yaml",
        "compose.yaml",
        "configs/medallion/competitions.yaml",
        "configs/seaweedfs/S3ProxyCaddyfile",
        "docker/images/airflow/requirements-scraping.txt",
        "docker/images/airflow/whoscored-build-provenance-attestation.json",
        "docker/images/airflow/whoscored-build-provenance-manifest.json",
        "docker/images/airflow/whoscored_capacity_worker_bootstrap.py",
        "docker/images/flaresolverr-whoscored/Dockerfile",
        "docker/images/flaresolverr-whoscored/Dockerfile.dockerignore",
        "docker/images/flaresolverr-whoscored/entrypoint.sh",
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
        _CAPACITY_RUNTIME_CONTRACT_RELATIVE,
        "scrapers/whoscored/runtime_contract.py",
        "scrapers/whoscored/runtime_limits.py",
        "scrapers/whoscored/service.py",
        "scrapers/whoscored/source_circuit.py",
        "scrapers/whoscored/stage_feeds.py",
        "scrapers/whoscored/transport.py",
        "scripts/audit_seaweedfs_control_network.py",
        "scripts/audit_seaweedfs_runtime_container.py",
        "scripts/compose.sh",
        "scripts/flaresolverr_extended.py",
        "scripts/proxy_filter/filter_proxy.py",
        "scripts/research/bench_whoscored_capacity.py",
        "scripts/research/bench_whoscored_workflow.py",
        "scripts/research/whoscored_capacity_container_runtime.py",
        "scripts/research/whoscored_capacity_worker_exec.py",
        "scripts/seaweedfs_legacy_entrypoint.sh",
        "scripts/seaweedfs_lifecycle_lock.sh",
        "scripts/validate_seaweedfs_s3_identity_config.py",
        "scripts/validate_whoscored_build_provenance.py",
        "scripts/whoscored_production_admission.py",
    }
)
_CAPACITY_SEALED_RUNTIME_PATHS = frozenset(
    {
        "configs/medallion/competitions.yaml",
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
        _CAPACITY_RUNTIME_CONTRACT_RELATIVE,
        "scrapers/whoscored/runtime_contract.py",
        "scrapers/whoscored/runtime_limits.py",
        "scrapers/whoscored/service.py",
        "scrapers/whoscored/source_circuit.py",
        "scrapers/whoscored/stage_feeds.py",
        "scrapers/whoscored/transport.py",
        "scripts/research/bench_whoscored_workflow.py",
        "scripts/research/whoscored_capacity_worker_exec.py",
    }
)
PROTECTED_SERVICES = (
    "airflow-scheduler",
    "flaresolverr",
    "flaresolverr_whoscored_paid",
    "whoscored_paid_gateway",
    "whoscored_proxy_filter",
)
_PROTECTED_SERVICE_SET = frozenset(PROTECTED_SERVICES)
COMMON_PROTECTED_SERVICES = ("airflow-scheduler", "flaresolverr")
GATEWAY_PROTECTED_SERVICES = (
    "flaresolverr_whoscored_paid",
    "whoscored_paid_gateway",
    "whoscored_proxy_filter",
)
COMMON_PROJECT = "data-platform"
GATEWAY_PROJECT = "whoscored-gw"
_COMMON_ONLY_ENV_FILES = (
    Path("/root/data-platform-football/.env"),
    Path("/root/.secrets/whoscored-runtime-v2.env"),
    Path("/root/.secrets/whoscored-proxy-v2.env"),
)
_SERVICE_PROJECT = {
    **{service: COMMON_PROJECT for service in COMMON_PROTECTED_SERVICES},
    **{service: GATEWAY_PROJECT for service in GATEWAY_PROTECTED_SERVICES},
}
_LEGACY_CUTOVER_SERVICES = (
    "airflow-scheduler",
    "flaresolverr",
    *GATEWAY_PROTECTED_SERVICES,
)
_LEGACY_CUTOVER_NETWORKS = (
    "whoscored-paid-api",
    "whoscored-paid-browser",
    "whoscored-paid-direct-egress",
    "whoscored-paid-provider-egress",
)
_LEGACY_CUTOVER_RETAINED_NETWORKS = ("backend", "frontend", "storage")
_LEGACY_CUTOVER_RETAINED_MEMBERS = {
    "backend": ("airflow-scheduler", "flaresolverr"),
    "frontend": ("airflow-scheduler",),
    "storage": ("airflow-scheduler",),
}
_LEGACY_CUTOVER_ALL_NETWORKS = (
    *_LEGACY_CUTOVER_RETAINED_NETWORKS,
    *_LEGACY_CUTOVER_NETWORKS,
)
_LEGACY_CUTOVER_FULL_MEMBERS = {
    "whoscored-paid-api": (
        "airflow-scheduler",
        "whoscored_paid_gateway",
    ),
    "whoscored-paid-browser": (
        "flaresolverr_whoscored_paid",
        "whoscored_paid_gateway",
        "whoscored_proxy_filter",
    ),
    "whoscored-paid-direct-egress": ("whoscored_paid_gateway",),
    "whoscored-paid-provider-egress": ("whoscored_proxy_filter",),
}
_CREATE_CAPTURE_GATEWAY_NETWORKS = {
    "whoscored_paid_gateway": (
        "whoscored-paid-api",
        "whoscored-paid-browser",
        "whoscored-paid-direct-egress",
    ),
    "whoscored_proxy_filter": _LEGACY_CUTOVER_NETWORKS,
    "flaresolverr_whoscored_paid": _LEGACY_CUTOVER_NETWORKS,
}
_CUTOVER_ACTIVE_DAGRUN_STATES = ("queued", "restarting", "running")
_CUTOVER_ACTIVE_TASK_STATES = (
    "deferred",
    "queued",
    "restarting",
    "running",
    "scheduled",
    "sensing",
    "up_for_reschedule",
    "up_for_retry",
)
_CUTOVER_INDEPENDENT_TASK_STATES = ("queued", "restarting", "running")
_CUTOVER_QUIESCENCE_SQL = f"""
BEGIN TRANSACTION ISOLATION LEVEL REPEATABLE READ READ ONLY;
SET LOCAL statement_timeout = '30s';
SELECT jsonb_build_object(
  'active_dag_runs', (
    SELECT COALESCE(jsonb_agg(jsonb_build_object(
      'dag_id', dag_id, 'run_id', run_id, 'state', state
    ) ORDER BY dag_id, run_id), '[]'::jsonb)
    FROM dag_run
    WHERE state IN ({", ".join(repr(state) for state in _CUTOVER_ACTIVE_DAGRUN_STATES)})
  ),
  'active_task_instances', (
    SELECT COALESCE(jsonb_agg(jsonb_build_object(
      'dag_id', ti.dag_id, 'map_index', ti.map_index, 'run_id', ti.run_id,
      'state', ti.state, 'task_id', ti.task_id
    ) ORDER BY ti.dag_id, ti.run_id, ti.task_id, ti.map_index), '[]'::jsonb)
    FROM task_instance AS ti
    LEFT JOIN dag_run AS dr
      ON dr.dag_id = ti.dag_id AND dr.run_id = ti.run_id
    WHERE (
      dr.state IN ({", ".join(repr(state) for state in _CUTOVER_ACTIVE_DAGRUN_STATES)})
      AND ti.state IN ({", ".join(repr(state) for state in _CUTOVER_ACTIVE_TASK_STATES)})
    ) OR ti.state IN ({", ".join(repr(state) for state in _CUTOVER_INDEPENDENT_TASK_STATES)})
  ),
  'active_non_scheduler_jobs', (
    SELECT COALESCE(jsonb_agg(jsonb_build_object(
      'hostname', hostname, 'id', id, 'job_type', job_type,
      'latest_heartbeat', latest_heartbeat, 'state', state
    ) ORDER BY id), '[]'::jsonb)
    FROM job
    WHERE state = 'running'
      AND job_type <> 'SchedulerJob'
      AND latest_heartbeat >= transaction_timestamp() - interval '5 minutes'
  ),
  'active_fbref_crawl_runs', (
    SELECT COALESCE(jsonb_agg(jsonb_build_object(
      'run_id', run_id, 'run_type', run_type, 'status', status
    ) ORDER BY created_at, run_id), '[]'::jsonb)
    FROM fbref_control.crawl_run
    WHERE status IN ('pending', 'running')
  ),
  'active_fbref_publication_locks', (
    SELECT COALESCE(jsonb_agg(jsonb_build_object(
      'expires_at', expires_at, 'owner_dag_id', owner_dag_id,
      'owner_run_id', owner_run_id, 'source', source
    ) ORDER BY source), '[]'::jsonb)
    FROM fbref_control.publication_lock
    WHERE released_at IS NULL
  ),
  'observed_at', to_char(
    transaction_timestamp() AT TIME ZONE 'UTC',
    'YYYY-MM-DD"T"HH24:MI:SS.US"Z"'
  )
)::text;
COMMIT;
""".strip()
_CUTOVER_DAG_PAUSE_SQL = """
BEGIN TRANSACTION ISOLATION LEVEL REPEATABLE READ READ ONLY;
SET LOCAL statement_timeout = '30s';
SELECT jsonb_build_object(
  'dag_pause_states', (
    SELECT COALESCE(jsonb_agg(jsonb_build_object(
      'dag_id', dag_id, 'is_paused', is_paused
    ) ORDER BY dag_id), '[]'::jsonb)
    FROM dag
  ),
  'observed_at', to_char(
    transaction_timestamp() AT TIME ZONE 'UTC',
    'YYYY-MM-DD"T"HH24:MI:SS.US"Z"'
  )
)::text;
COMMIT;
""".strip()
_COMMON_EXTERNAL_NETWORKS = {
    "whoscored-paid-api": {
        "external": True,
        "ipam": {},
        "name": "dp-whoscored-paid-api",
    }
}
_NETWORK_PROJECT = {
    "backend": COMMON_PROJECT,
    "frontend": COMMON_PROJECT,
    "storage": COMMON_PROJECT,
    "whoscored-paid-api": GATEWAY_PROJECT,
    "whoscored-paid-browser": GATEWAY_PROJECT,
    "whoscored-paid-direct-egress": GATEWAY_PROJECT,
    "whoscored-paid-provider-egress": GATEWAY_PROJECT,
}
_DIGEST = re.compile(r"\A[0-9a-f]{64}\Z")
_ZERO_DIGEST = "0" * 64
_COMMIT = re.compile(r"\A[0-9a-f]{40}\Z")
_PINNED_IMAGE = re.compile(r"\A[^\s@]+@sha256:[0-9a-f]{64}\Z")
_IMAGE_ID = re.compile(r"\Asha256:[0-9a-f]{64}\Z")
_CONTAINER_ID = re.compile(r"\A[0-9a-f]{64}\Z")
_MAC_ADDRESS = re.compile(r"\A(?:[0-9a-f]{2}:){5}[0-9a-f]{2}\Z")
_PROJECT_NAME = re.compile(r"\A[a-z0-9][a-z0-9_-]*\Z")
_CONFIG_HASH = re.compile(r"\A[0-9a-f]{64}\Z")
_ROLLOUT_ID = re.compile(r"\A[A-Za-z0-9][A-Za-z0-9._:-]{0,127}\Z")
_COMPOSE_VERSION = re.compile(
    r"\A([0-9]+)\.([0-9]+)\.([0-9]+)(?:[-+][0-9A-Za-z.-]+)?\Z"
)
_REQUIRED_DOCKER_SECURITY_OPTIONS = frozenset(
    {"name=apparmor", "name=seccomp,profile=builtin"}
)
_EXPECTED_SECURITY_OPT = (
    "no-new-privileges:true",
    "apparmor=docker-default",
    "seccomp=builtin",
)
_DOCKER_CLI = Path("/usr/bin/docker")
_DOCKER_SOCKET = Path("/run/docker.sock")
_SYSTEM_PYTHON = Path("/usr/bin/python3")
_FORBIDDEN_CONTROL_ENV = frozenset(
    {
        "COMPOSE_DISABLE_ENV_FILE",
        "COMPOSE_ENV_FILES",
        "COMPOSE_FILE",
        "COMPOSE_PATH_SEPARATOR",
        "COMPOSE_PROFILES",
        "COMPOSE_PROJECT_NAME",
        "DOCKER_API_VERSION",
        "DOCKER_CERT_PATH",
        "DOCKER_CONFIG",
        "DOCKER_CONTEXT",
        "DOCKER_HOST",
        "DOCKER_TLS_VERIFY",
        "GCONV_PATH",
        "GLIBC_TUNABLES",
        "LD_AUDIT",
        "LD_LIBRARY_PATH",
        "LD_PRELOAD",
        "LOCPATH",
        "MALLOC_TRACE",
        "PYTHONPATH",
    }
)
_AIRFLOW_ENTRYPOINT = (
    "/usr/bin/dumb-init",
    "--",
    "/usr/local/bin/whoscored-production-entrypoint",
    "/entrypoint",
)
_EXPECTED_ENTRYPOINTS = {
    "airflow-scheduler": _AIRFLOW_ENTRYPOINT,
    "flaresolverr": ("/usr/bin/dumb-init", "--"),
    "flaresolverr_whoscored_paid": ("/usr/bin/dumb-init", "--"),
    "whoscored_paid_gateway": _AIRFLOW_ENTRYPOINT,
    "whoscored_proxy_filter": _AIRFLOW_ENTRYPOINT,
}
_EXPECTED_IMAGE_USER = {
    "airflow-scheduler": "50000:0",
    "flaresolverr": "1000:1000",
    "flaresolverr_whoscored_paid": "1000:1000",
    "whoscored_paid_gateway": "50000:0",
    "whoscored_proxy_filter": "50000:0",
}
_EXPECTED_WORKING_DIR = {
    "airflow-scheduler": "/opt/airflow",
    "flaresolverr": "/app",
    "flaresolverr_whoscored_paid": "/app",
    "whoscored_paid_gateway": "/opt/airflow",
    "whoscored_proxy_filter": "/opt/airflow",
}
_EXPECTED_COMMANDS = {
    "airflow-scheduler": ("scheduler",),
    "flaresolverr": ("/usr/local/bin/whoscored-flaresolverr-entrypoint",),
    "flaresolverr_whoscored_paid": (
        "/usr/local/bin/whoscored-flaresolverr-entrypoint",
    ),
    "whoscored_paid_gateway": (
        "python",
        "/opt/airflow/scripts/whoscored_paid_gateway.py",
        "--host",
        "0.0.0.0",
        "--port",
        "8898",
        "--proxy-url",
        "http://whoscored_proxy_filter:8900",
        "--proxy-control-url",
        "http://whoscored_proxy_filter:8899",
        "--flaresolverr-url",
        "http://flaresolverr_whoscored_paid:8191",
    ),
}
_SECURITY_POLICY = {
    "airflow-scheduler": {
        "cap_add": frozenset(),
        "read_only": False,
    },
    "flaresolverr": {
        "cap_add": frozenset(),
        "read_only": True,
    },
    "flaresolverr_whoscored_paid": {
        "cap_add": frozenset(),
        "read_only": True,
    },
    "whoscored_paid_gateway": {
        "cap_add": frozenset(),
        "read_only": True,
    },
    "whoscored_proxy_filter": {"cap_add": frozenset(), "read_only": True},
}
_CRITICAL_IMAGE_PATHS = {
    "airflow-scheduler": (
        "/bin/sh",
        "/entrypoint",
        "/lib",
        "/lib64",
        "/opt/airflow/runtime-contract",
        "/opt/legacy-scraper-venv",
        "/usr/bin/dumb-init",
        "/usr/local/bin/whoscored-production-entrypoint",
        "/usr/local/bin/whoscored-production-gate",
        "/usr/local/bin/whoscored-production-python",
        "/usr/local/lib/whoscored_runtime_startup.py",
        "/usr/local/libexec/whoscored-python-real",
        "/usr/local/libexec/whoscored_production_gate.py",
        "/usr/local/share/whoscored",
    ),
    "flaresolverr": (
        "/app/chromedriver",
        "/usr/bin/dumb-init",
        "/usr/local/bin/whoscored-flaresolverr-entrypoint",
        "/usr/local/libexec/whoscored",
        "/usr/local/share/whoscored",
    ),
    "flaresolverr_whoscored_paid": (
        "/app/chromedriver",
        "/usr/bin/dumb-init",
        "/usr/local/bin/whoscored-flaresolverr-entrypoint",
        "/usr/local/libexec/whoscored",
        "/usr/local/share/whoscored",
    ),
    "whoscored_paid_gateway": (
        "/bin/sh",
        "/entrypoint",
        "/lib",
        "/lib64",
        "/opt/airflow/runtime-contract",
        "/usr/bin/dumb-init",
        "/usr/local/bin/whoscored-production-entrypoint",
        "/usr/local/bin/whoscored-production-gate",
        "/usr/local/bin/whoscored-production-python",
        "/usr/local/lib/whoscored_runtime_startup.py",
        "/usr/local/libexec/whoscored-python-real",
        "/usr/local/libexec/whoscored_production_gate.py",
        "/usr/local/share/whoscored",
    ),
    "whoscored_proxy_filter": (
        "/bin/sh",
        "/entrypoint",
        "/lib",
        "/lib64",
        "/opt/airflow/runtime-contract",
        "/usr/bin/dumb-init",
        "/usr/local/bin/whoscored-production-entrypoint",
        "/usr/local/bin/whoscored-production-gate",
        "/usr/local/bin/whoscored-production-python",
        "/usr/local/lib/whoscored_runtime_startup.py",
        "/usr/local/libexec/whoscored-python-real",
        "/usr/local/libexec/whoscored_production_gate.py",
        "/usr/local/share/whoscored",
    ),
}
_ALLOWED_VOLUME_TARGETS = {
    "airflow-scheduler": {
        "/home/airflow/soccerdata": ("volume", False),
        "/opt/airflow/configs/fotmob": ("bind", True),
        "/opt/airflow/configs/medallion": ("bind", True),
        "/opt/airflow/configs/proxy_filter": ("bind", True),
        "/opt/airflow/configs/soccerdata": ("bind", True),
        "/opt/airflow/configs/sofascore": ("bind", True),
        "/opt/airflow/dags": ("bind", True),
        "/opt/airflow/fotmob-admission": ("bind", True),
        "/opt/airflow/logs": ("bind", False),
        "/opt/airflow/proxys.txt": ("bind", True),
        "/opt/airflow/runtime/sofascore/proxy_budget_canary.json": (
            "bind",
            True,
        ),
        "/opt/airflow/scrapers": ("bind", True),
        "/opt/airflow/scripts": ("bind", True),
        FBREF_CAMOUFOX_GEOIP_DATABASE_CONTAINER_PATH: ("bind", True),
        "/opt/airflow/secure/whoscored-approvals": ("bind", True),
        "/opt/airflow/secure/whoscored-scheduled-pointers": ("bind", True),
        "/opt/airflow/state/whoscored-proxy-filter": ("bind", True),
        "/opt/airflow/transform": ("bind", True),
        "/opt/airflow/webserver_config.py": ("bind", True),
    },
    "flaresolverr": {},
    "flaresolverr_whoscored_paid": {},
    "whoscored_paid_gateway": {
        "/opt/airflow/configs/medallion": ("bind", True),
        "/opt/airflow/dags": ("bind", True),
        "/opt/airflow/scrapers": ("bind", True),
        "/opt/airflow/scripts": ("bind", True),
        "/opt/airflow/secure/whoscored-alert-authority": ("bind", True),
        "/opt/airflow/state/whoscored-paid-gateway": ("bind", False),
    },
    "whoscored_proxy_filter": {
        "/opt/airflow/configs/medallion": ("bind", True),
        "/opt/airflow/configs/proxy_filter": ("bind", True),
        "/opt/airflow/dags": ("bind", True),
        "/opt/airflow/scrapers": ("bind", True),
        "/opt/airflow/scripts": ("bind", True),
        "/opt/airflow/state/whoscored-proxy-filter": ("bind", False),
    },
}
_RELEASE_BIND_TARGETS = {
    "airflow-scheduler": {
        "/opt/airflow/configs/fotmob": "configs/fotmob",
        "/opt/airflow/configs/medallion": "configs/medallion",
        "/opt/airflow/configs/proxy_filter": "configs/proxy_filter",
        "/opt/airflow/configs/soccerdata": "configs/soccerdata",
        "/opt/airflow/configs/sofascore": "configs/sofascore",
        "/opt/airflow/dags": "dags",
        "/opt/airflow/scrapers": "scrapers",
        "/opt/airflow/scripts": "scripts",
        "/opt/airflow/transform": "transform",
        "/opt/airflow/webserver_config.py": "configs/airflow/webserver_config.py",
    },
    "flaresolverr": {},
    "flaresolverr_whoscored_paid": {},
    "whoscored_paid_gateway": {
        "/opt/airflow/configs/medallion": "configs/medallion",
        "/opt/airflow/dags": "dags",
        "/opt/airflow/scrapers": "scrapers",
        "/opt/airflow/scripts": "scripts",
    },
    "whoscored_proxy_filter": {
        "/opt/airflow/configs/medallion": "configs/medallion",
        "/opt/airflow/configs/proxy_filter": "configs/proxy_filter",
        "/opt/airflow/dags": "dags",
        "/opt/airflow/scrapers": "scrapers",
        "/opt/airflow/scripts": "scripts",
    },
}
_RUNTIME_HOST_BIND_TARGETS = {
    (
        "airflow-scheduler",
        "/opt/airflow/fotmob-admission",
    ): "protected-directory",
    (
        "airflow-scheduler",
        FBREF_CAMOUFOX_GEOIP_DATABASE_CONTAINER_PATH,
    ): "fbref-geoip-database",
    ("airflow-scheduler", "/opt/airflow/logs"): "writable-directory",
    ("airflow-scheduler", "/opt/airflow/proxys.txt"): "protected-file",
    (
        "airflow-scheduler",
        "/opt/airflow/runtime/sofascore/proxy_budget_canary.json",
    ): "scheduler-readable-protected-file",
    (
        "airflow-scheduler",
        "/opt/airflow/secure/whoscored-approvals",
    ): "airflow-authority-directory",
    (
        "airflow-scheduler",
        "/opt/airflow/secure/whoscored-scheduled-pointers",
    ): "airflow-authority-directory",
    (
        "airflow-scheduler",
        "/opt/airflow/state/whoscored-proxy-filter",
    ): "writable-directory",
    (
        "whoscored_paid_gateway",
        "/opt/airflow/state/whoscored-paid-gateway",
    ): "writable-directory",
    (
        "whoscored_paid_gateway",
        "/opt/airflow/secure/whoscored-alert-authority",
    ): "protected-directory",
    (
        "whoscored_proxy_filter",
        "/opt/airflow/state/whoscored-proxy-filter",
    ): "writable-directory",
}
_AIRFLOW_RUNTIME_UID = 50_000
_ALLOWED_TMPFS = {
    "airflow-scheduler": {},
    "flaresolverr": {
        "/app/.config": frozenset(
            {
                "rw",
                "noexec",
                "nosuid",
                "nodev",
                "size=64m",
                "uid=1000",
                "gid=1000",
                "mode=0700",
            }
        ),
        "/app/.local": frozenset(
            {
                "rw",
                "noexec",
                "nosuid",
                "nodev",
                "size=64m",
                "uid=1000",
                "gid=1000",
                "mode=0700",
            }
        ),
        "/config": frozenset(
            {
                "rw",
                "noexec",
                "nosuid",
                "nodev",
                "size=16m",
                "uid=1000",
                "gid=1000",
                "mode=0700",
            }
        ),
        "/tmp": frozenset(
            {
                "rw",
                "exec",
                "nosuid",
                "nodev",
                "size=2g",
                "uid=1000",
                "gid=1000",
                "mode=1770",
            }
        ),
    },
    "flaresolverr_whoscored_paid": {
        "/app/.config": frozenset(
            {
                "rw",
                "noexec",
                "nosuid",
                "nodev",
                "size=64m",
                "uid=1000",
                "gid=1000",
                "mode=0700",
            }
        ),
        "/app/.local": frozenset(
            {
                "rw",
                "noexec",
                "nosuid",
                "nodev",
                "size=64m",
                "uid=1000",
                "gid=1000",
                "mode=0700",
            }
        ),
        "/config": frozenset(
            {
                "rw",
                "noexec",
                "nosuid",
                "nodev",
                "size=16m",
                "uid=1000",
                "gid=1000",
                "mode=0700",
            }
        ),
        "/tmp": frozenset(
            {
                "rw",
                "exec",
                "nosuid",
                "nodev",
                "size=2g",
                "uid=1000",
                "gid=1000",
                "mode=1770",
            }
        ),
    },
    "whoscored_paid_gateway": {
        "/tmp": frozenset(
            {
                "rw",
                "noexec",
                "nosuid",
                "nodev",
                "size=32m",
                "uid=50000",
                "gid=0",
                "mode=0700",
            }
        ),
    },
    "whoscored_proxy_filter": {
        "/tmp": frozenset(
            {
                "rw",
                "noexec",
                "nosuid",
                "nodev",
                "size=32m",
                "uid=50000",
                "gid=0",
                "mode=0700",
            }
        ),
    },
}
_EXPECTED_HEALTHCHECKS = {
    "airflow-scheduler": {
        "Interval": 30_000_000_000,
        "Retries": 5,
        "StartPeriod": 60_000_000_000,
        "Test": (
            "CMD-SHELL",
            'airflow jobs check --job-type SchedulerJob --hostname "$${HOSTNAME}"',
        ),
        "Timeout": 30_000_000_000,
    },
    "flaresolverr": {
        "Interval": 30_000_000_000,
        "Retries": 3,
        "StartPeriod": 30_000_000_000,
        "Test": ("CMD-SHELL", "curl -fsS http://localhost:8191/health || exit 1"),
        "Timeout": 10_000_000_000,
    },
    "flaresolverr_whoscored_paid": {
        "Interval": 10_000_000_000,
        "Retries": 5,
        "StartPeriod": 30_000_000_000,
        "Test": ("CMD-SHELL", "curl -fsS http://localhost:8191/health || exit 1"),
        "Timeout": 5_000_000_000,
    },
    "whoscored_paid_gateway": {
        "Interval": 10_000_000_000,
        "Retries": 5,
        "StartPeriod": 5_000_000_000,
        "Test": (
            "CMD",
            "curl",
            "--fail",
            "--silent",
            "http://localhost:8898/health",
        ),
        "Timeout": 3_000_000_000,
    },
    "whoscored_proxy_filter": {
        "Interval": 10_000_000_000,
        "Retries": 5,
        "StartPeriod": 5_000_000_000,
        "Test": (
            "CMD",
            "curl",
            "--fail",
            "--silent",
            "http://localhost:8899/health",
        ),
        "Timeout": 3_000_000_000,
    },
}
_ALLOWED_RENDERED_KEYS = {
    "airflow-scheduler": frozenset(
        {
            "cap_drop",
            "command",
            "container_name",
            "depends_on",
            "deploy",
            "entrypoint",
            "environment",
            "healthcheck",
            "image",
            "memswap_limit",
            "networks",
            "restart",
            "security_opt",
            "shm_size",
            "volumes",
        }
    ),
    "flaresolverr": frozenset(
        {
            "cap_drop",
            "command",
            "container_name",
            "deploy",
            "entrypoint",
            "environment",
            "healthcheck",
            "image",
            "networks",
            "ports",
            "read_only",
            "restart",
            "security_opt",
            "shm_size",
            "tmpfs",
        }
    ),
    "flaresolverr_whoscored_paid": frozenset(
        {
            "cap_drop",
            "command",
            "container_name",
            "deploy",
            "entrypoint",
            "environment",
            "healthcheck",
            "image",
            "networks",
            "read_only",
            "restart",
            "security_opt",
            "shm_size",
            "tmpfs",
        }
    ),
    "whoscored_paid_gateway": frozenset(
        {
            "cap_drop",
            "command",
            "container_name",
            "depends_on",
            "deploy",
            "entrypoint",
            "environment",
            "healthcheck",
            "image",
            "networks",
            "read_only",
            "restart",
            "security_opt",
            "tmpfs",
            "volumes",
        }
    ),
    "whoscored_proxy_filter": frozenset(
        {
            "cap_drop",
            "command",
            "container_name",
            "deploy",
            "entrypoint",
            "environment",
            "healthcheck",
            "image",
            "networks",
            "read_only",
            "restart",
            "security_opt",
            "tmpfs",
            "volumes",
        }
    ),
}
_EXPECTED_NETWORKS = {
    "airflow-scheduler": {
        "backend": None,
        "frontend": None,
        "storage": None,
        "whoscored-paid-api": None,
    },
    "flaresolverr": {"backend": None},
    "flaresolverr_whoscored_paid": {"whoscored-paid-browser": None},
    "whoscored_paid_gateway": {
        "whoscored-paid-api": None,
        "whoscored-paid-browser": None,
        "whoscored-paid-direct-egress": None,
    },
    "whoscored_proxy_filter": {
        "whoscored-paid-browser": None,
        "whoscored-paid-provider-egress": None,
    },
}
_EXPECTED_NETWORK_MODE = {
    "airflow-scheduler": "dp-backend",
    "flaresolverr": "dp-backend",
    "flaresolverr_whoscored_paid": "dp-whoscored-paid-browser",
    "whoscored_paid_gateway": "dp-whoscored-paid-api",
    "whoscored_proxy_filter": "dp-whoscored-paid-browser",
}
_EXPECTED_NETWORK_DEFINITIONS = {
    "backend": {"driver": "bridge", "ipam": {}, "name": "dp-backend"},
    "frontend": {"driver": "bridge", "ipam": {}, "name": "dp-frontend"},
    "storage": {"driver": "bridge", "ipam": {}, "name": "dp-storage"},
    "whoscored-paid-api": {
        "driver": "bridge",
        "internal": True,
        "ipam": {},
        "name": "dp-whoscored-paid-api",
    },
    "whoscored-paid-browser": {
        "driver": "bridge",
        "internal": True,
        "ipam": {},
        "name": "dp-whoscored-paid-browser",
    },
    "whoscored-paid-direct-egress": {
        "driver": "bridge",
        "ipam": {},
        "name": "dp-whoscored-paid-direct-egress",
    },
    "whoscored-paid-provider-egress": {
        "driver": "bridge",
        "ipam": {},
        "name": "dp-whoscored-paid-provider-egress",
    },
}
_EXPECTED_DEPENDS_ON = {
    "airflow-scheduler": {
        "airflow-init": {
            "condition": "service_completed_successfully",
            "required": True,
        },
        "airflow-webserver": {"condition": "service_healthy", "required": True},
    },
    "flaresolverr": None,
    "flaresolverr_whoscored_paid": None,
    "whoscored_paid_gateway": {
        "flaresolverr_whoscored_paid": {
            "condition": "service_healthy",
            "required": True,
        },
        "whoscored_proxy_filter": {
            "condition": "service_healthy",
            "required": True,
        },
    },
    "whoscored_proxy_filter": None,
}
_EXPECTED_DEPLOY = {
    "airflow-scheduler": {
        "placement": {},
        "resources": {
            "limits": {"memory": "17179869184"},
            "reservations": {"memory": "1073741824"},
        },
    },
    "flaresolverr": {
        "placement": {},
        "resources": {
            "limits": {"memory": "4294967296"},
            "reservations": {"memory": "536870912"},
        },
    },
    "flaresolverr_whoscored_paid": {
        "placement": {},
        "resources": {
            "limits": {"memory": "2147483648"},
            "reservations": {"memory": "536870912"},
        },
    },
    "whoscored_paid_gateway": {
        "placement": {},
        "resources": {
            "limits": {"memory": "268435456"},
            "reservations": {"memory": "67108864"},
        },
    },
    "whoscored_proxy_filter": {
        "placement": {},
        "resources": {
            "limits": {"memory": "268435456"},
            "reservations": {"memory": "67108864"},
        },
    },
}
_EXPECTED_CONTAINER_RESOURCES = {
    "airflow-scheduler": {
        "Memory": 17_179_869_184,
        "MemoryReservation": 1_073_741_824,
        "MemorySwap": 17_179_869_184,
    },
    "flaresolverr": {
        "Memory": 4_294_967_296,
        "MemoryReservation": 536_870_912,
        # Compose leaves memswap_limit unset for this service. Engine 29
        # normalizes that request to memory + an equal swap allowance in the
        # inspected HostConfig, even when the host currently has no swap.
        "MemorySwap": 8_589_934_592,
    },
    "flaresolverr_whoscored_paid": {
        "Memory": 2_147_483_648,
        "MemoryReservation": 536_870_912,
        "MemorySwap": 4_294_967_296,
    },
    "whoscored_paid_gateway": {
        "Memory": 268_435_456,
        "MemoryReservation": 67_108_864,
        "MemorySwap": 536_870_912,
    },
    "whoscored_proxy_filter": {
        "Memory": 268_435_456,
        "MemoryReservation": 67_108_864,
        "MemorySwap": 536_870_912,
    },
}
_SCHEDULER_ENVIRONMENT_NAMES = frozenset(
    """
    AIRFLOW__CELERY__BROKER_URL AIRFLOW__CELERY__RESULT_BACKEND
    AIRFLOW__CORE__DAGS_ARE_PAUSED_AT_CREATION AIRFLOW__CORE__EXECUTOR
    AIRFLOW__CORE__FERNET_KEY AIRFLOW__CORE__LOAD_EXAMPLES
    AIRFLOW__DATABASE__SQL_ALCHEMY_CONN AIRFLOW__WEBSERVER__EXPOSE_CONFIG
    AIRFLOW__WEBSERVER__SECRET_KEY ALERT_ENV FBREF_PROXY_CONTROL_TOKEN
    FBREF_CAMOUFOX_GEOIP_DATABASE_PATH FBREF_CONTROL_DB_URI FOTMOB_DEPLOY_GIT_SHA
    FOTMOB_SHARED_DEPLOYMENT_REPORT_PATH
    FBREF_PROXY_CONTROL_URL FBREF_PROXY_LEASE_TTL_SECONDS FBREF_RAW_S3_ENDPOINT
    FBREF_RAW_S3_SCHEME FBREF_RAW_STORE_URI FBREF_STAGE_JANITOR_MODE
    FOTMOB_RAW_S3_ENDPOINT
    FOTMOB_RAW_S3_SCHEME FOTMOB_RAW_STORE_URI ICEBERG_REST_WAREHOUSE
    ICEBERG_WAREHOUSE JAVA_HOME LEGACY_SCRAPER_PYTHON
    PROXY_FILTER_CONTROL_TOKEN PROXY_FILTER_LEDGER_PATH
    PROXY_FILTER_SOFASCORE_CANARY_HARD_CAP_BYTES PROXY_FILTER_URL
    S3_ACCESS_KEY S3_ENDPOINT S3_SCHEME S3_SECRET_KEY
    SEAWEEDFS_CUTOVER_BACKUP_MIBPS SEAWEEDFS_CUTOVER_FIXED_OVERHEAD_SECONDS
    SEAWEEDFS_CUTOVER_INVENTORY_MIBPS SEAWEEDFS_CUTOVER_MAX_DOWNTIME_SECONDS
    SEAWEEDFS_CUTOVER_REHEARSAL_INVENTORY
    SEAWEEDFS_CUTOVER_REHEARSAL_MAX_AGE_HOURS
    SEAWEEDFS_CUTOVER_VERIFY_MIBPS SOFASCORE_MANIFEST_BACKEND
    SOFASCORE_PROXY_BUDGET_ARTIFACT SOFASCORE_PROXY_BUDGET_ARTIFACT_ID
    SOFASCORE_PROXY_BUDGET_LEDGER
    SOFASCORE_PROXY_CONTROL_TOKEN SOFASCORE_PROXY_CONTROL_URL
    SOFASCORE_PROXY_LEASE_TTL_SECONDS SOFASCORE_RAW_STORE_URI
    SOFASCORE_REGISTRY_PATH SOFASCORE_PLAYER_ROTATION_MIN_LEAGUES
    SOFASCORE_PLAYER_ROTATION_MODULUS SOFASCORE_WORKLOAD_PLAN_DIR TELEGRAM_BOT_TOKEN
    TELEGRAM_CHAT_ID TM_NATIVE_V2_ENABLED TM_STANDING_POLICY_ENABLED
    TM_BACKFILL_PROXY_CONTROL_TOKEN TM_BACKFILL_PROXY_CONTROL_URL
    TM_PROXY_CONTROL_TOKEN TM_PROXY_CONTROL_URL TM_PROXY_LEASE_TTL_SECONDS
    TM_REQUIRE_METERED_PROXY TRANSFERMARKT_RAW_STORE_URI TRINO_HOST
    TRINO_PASSWORD TRINO_PORT WHOSCORED_BACKFILL_ASSUMED_REQUEST_UNITS_PER_DAY
    WHOSCORED_BACKFILL_MAX_NO_PROGRESS_RUNS WHOSCORED_BACKFILL_POOL
    WHOSCORED_BACKFILL_REQUEST_UNITS_PER_RUN
    WHOSCORED_BACKUP_DESTINATION_RETENTION_MODE
    WHOSCORED_BACKUP_DESTINATION_S3_ACCESS_KEY
    WHOSCORED_BACKUP_DESTINATION_S3_ENDPOINT
    WHOSCORED_BACKUP_DESTINATION_S3_REGION
    WHOSCORED_BACKUP_DESTINATION_S3_SCHEME
    WHOSCORED_BACKUP_DESTINATION_S3_SECRET_KEY
    WHOSCORED_BACKUP_DESTINATION_URI
    AIRFLOW__CORE__PLUGINS_FOLDER WHOSCORED_BACKUP_LOCAL_RETENTION_DAYS
    WHOSCORED_BACKUP_RESTORE_DRILL_EVIDENCE_PATH
    WHOSCORED_BACKUP_RESTORE_DRILL_MAX_AGE_HOURS
    WHOSCORED_BACKUP_RESTORE_S3_ACCESS_KEY
    WHOSCORED_BACKUP_RESTORE_S3_ENDPOINT WHOSCORED_BACKUP_RESTORE_S3_REGION
    WHOSCORED_BACKUP_RESTORE_S3_SCHEME WHOSCORED_BACKUP_RESTORE_S3_SECRET_KEY
    WHOSCORED_BACKUP_SOURCE_S3_ACCESS_KEY WHOSCORED_BACKUP_SOURCE_S3_ENDPOINT
    WHOSCORED_BACKUP_SOURCE_S3_REGION WHOSCORED_BACKUP_SOURCE_S3_SCHEME
    WHOSCORED_BACKUP_SOURCE_S3_SECRET_KEY
    WHOSCORED_BACKUP_RPO_HOURS WHOSCORED_BACKUP_RTO_HOURS
    WHOSCORED_BACKUP_WORKERS WHOSCORED_CATALOG_REQUESTS_PER_MINUTE
    WHOSCORED_DAILY_P95_LIMIT_HOURS WHOSCORED_DAILY_PROFILE_MAX_LIMIT
    WHOSCORED_DAILY_SLO_MIN_SAMPLES WHOSCORED_DAILY_SLO_WINDOW
    WHOSCORED_DIRECT_POOL WHOSCORED_DQ_POOL WHOSCORED_LOCK_DIR
    WHOSCORED_OPS_IO_ATTEMPTS WHOSCORED_OPS_RETRY_BASE_SECONDS
    WHOSCORED_OPS_STORE_URI
    WHOSCORED_PAID_BATCH_ENABLED WHOSCORED_PAID_GATEWAY_TOKEN
    WHOSCORED_PAID_GATEWAY_URL
    WHOSCORED_PROXY_APPROVAL_ROOT WHOSCORED_PROXY_CAMPAIGN_LEDGER_PATH
    WHOSCORED_SCHEDULED_PAID_MODE WHOSCORED_SCHEDULED_PAID_POINTER_ROOT
    WHOSCORED_RAW_IO_ATTEMPTS WHOSCORED_RAW_LOCK_DIR
    WHOSCORED_RAW_LOCK_TIMEOUT_SECONDS WHOSCORED_RAW_RETRY_BASE_SECONDS
    WHOSCORED_RAW_S3_ACCESS_KEY WHOSCORED_RAW_S3_ENDPOINT
    WHOSCORED_RAW_S3_SCHEME WHOSCORED_RAW_S3_SECRET_KEY
    WHOSCORED_RAW_SNAPSHOT_LOCK_TIMEOUT_SECONDS WHOSCORED_RAW_STORE_URI
    WHOSCORED_REQUEST_LEDGER_PATH WHOSCORED_RUN_RETENTION_DAYS
    WHOSCORED_SCOPE_WRITE_CHUNK_ROWS WHOSCORED_SCRAPER_PYTHON
    WHOSCORED_SOURCE_CIRCUIT_PATH WHOSCORED_SOURCE_CIRCUIT_WAIT
    WHOSCORED_SOURCE_POOL_SLOTS WHOSCORED_STRUCTURED_REQUESTS_PER_MINUTE
    """.split()
)
_EXPECTED_ENVIRONMENT_NAMES = {
    "airflow-scheduler": _SCHEDULER_ENVIRONMENT_NAMES,
    "flaresolverr": frozenset({"CAPTCHA_SOLVER", "LOG_HTML", "LOG_LEVEL", "TZ"}),
    "flaresolverr_whoscored_paid": frozenset(
        {
            "CAPTCHA_SOLVER",
            "LOG_HTML",
            "LOG_LEVEL",
            "TZ",
            "WHOSCORED_FLARESOLVERR_GATEWAY_SECRET",
            "WHOSCORED_FLARESOLVERR_PAID_EXCLUSIVE",
        }
    ),
    "whoscored_paid_gateway": frozenset(
        {
            "ALERT_ENV",
            "CONNECTION_CHECK_MAX_COUNT",
            "WHOSCORED_FLARESOLVERR_GATEWAY_SECRET",
            "WHOSCORED_PAID_ALERT_BINDING_PATH",
            "WHOSCORED_PAID_ALERT_HMAC_SECRET",
            "WHOSCORED_PAID_ALERT_RECEIPT_ROOT",
            "WHOSCORED_PAID_ALERT_AUTHORITY_ROOT",
            "WHOSCORED_PAID_ALERT_SECRET_PATH",
            "WHOSCORED_PAID_BATCH_ENABLED",
            "WHOSCORED_PAID_GATEWAY_TOKEN",
            "WHOSCORED_PROXY_APPROVAL_HMAC_SECRET",
            "WHOSCORED_PROXY_CONTROL_TOKEN",
        }
    ),
    "whoscored_proxy_filter": frozenset(
        {
            "CONNECTION_CHECK_MAX_COUNT",
            "PROXY_FILTER_ALLOW_FILE_FALLBACK",
            "PROXY_FILTER_CONTROL_TOKEN",
            "PROXY_POOL_JSON",
            "TM_PROXY_CONTROL_TOKEN",
            "WHOSCORED_PROVIDER_ORDER_CAP_BYTES",
            "WHOSCORED_PROVIDER_ORDER_ID",
            "WHOSCORED_PROVIDER_POLICY_SHA256",
            "WHOSCORED_PROXY_APPROVAL_HMAC_SECRET",
            "WHOSCORED_PROXY_CAMPAIGN_LEDGER_PATH",
            "WHOSCORED_PROXY_FILTER_DAILY_BUDGET_BYTES",
            "WHOSCORED_PROXY_FILTER_MAX_LEASE_BYTES",
            "WHOSCORED_PROXY_LEDGER_HMAC_SECRET",
        }
    ),
}
_FIXED_ENVIRONMENT = {
    "airflow-scheduler": {
        "AIRFLOW__CORE__DAGS_ARE_PAUSED_AT_CREATION": "true",
        "AIRFLOW__CORE__EXECUTOR": "LocalExecutor",
        "AIRFLOW__CORE__LOAD_EXAMPLES": "false",
        "AIRFLOW__CORE__PLUGINS_FOLDER": "/opt/airflow/dags/plugins",
        "AIRFLOW__WEBSERVER__EXPOSE_CONFIG": "false",
        "FBREF_PROXY_CONTROL_URL": "http://fbref_proxy_filter:8899",
        "FBREF_PROXY_LEASE_TTL_SECONDS": "7200",
        "FBREF_STAGE_JANITOR_MODE": "apply",
        "FBREF_CAMOUFOX_GEOIP_DATABASE_PATH": (
            FBREF_CAMOUFOX_GEOIP_DATABASE_CONTAINER_PATH
        ),
        "LEGACY_SCRAPER_PYTHON": "/opt/legacy-scraper-venv/bin/python",
        "PROXY_FILTER_LEDGER_PATH": (
            "/opt/airflow/state/whoscored-proxy-filter/paid_requests.jsonl"
        ),
        "PROXY_FILTER_URL": "",
        "SOFASCORE_PROXY_BUDGET_ARTIFACT": (
            "/opt/airflow/runtime/sofascore/proxy_budget_canary.json"
        ),
        "TM_BACKFILL_PROXY_CONTROL_URL": "http://proxy_filter:8899",
        "WHOSCORED_BACKFILL_POOL": "whoscored_direct_pool",
        "WHOSCORED_BACKUP_RESTORE_DRILL_EVIDENCE_PATH": (
            "/opt/airflow/logs/whoscored_backup/restore-drill-evidence.json"
        ),
        "WHOSCORED_BACKUP_RESTORE_DRILL_MAX_AGE_HOURS": "24",
        "WHOSCORED_BACKUP_RPO_HOURS": "24",
        "WHOSCORED_BACKUP_RTO_HOURS": "24",
        "WHOSCORED_DIRECT_POOL": "whoscored_direct_pool",
        "WHOSCORED_DQ_POOL": "whoscored_dq_pool",
        "WHOSCORED_LOCK_DIR": "/opt/airflow/logs/whoscored/commit_locks",
        "WHOSCORED_PAID_GATEWAY_URL": "http://whoscored_paid_gateway:8898",
        "WHOSCORED_PROXY_APPROVAL_ROOT": ("/opt/airflow/secure/whoscored-approvals"),
        "WHOSCORED_SCHEDULED_PAID_POINTER_ROOT": (
            "/opt/airflow/secure/whoscored-scheduled-pointers"
        ),
        "WHOSCORED_SCHEDULED_PAID_MODE": "required",
        "WHOSCORED_PROXY_CAMPAIGN_LEDGER_PATH": (
            "/opt/airflow/state/whoscored-proxy-filter/whoscored_campaigns.json"
        ),
        "WHOSCORED_RAW_LOCK_DIR": "/opt/airflow/logs/whoscored/raw_locks",
        "WHOSCORED_RAW_S3_ENDPOINT": "seaweedfs:8333",
        "WHOSCORED_RAW_S3_SCHEME": "http",
        "WHOSCORED_REQUEST_LEDGER_PATH": (
            "/opt/airflow/logs/whoscored/request_ledger.jsonl"
        ),
        "WHOSCORED_SCRAPER_PYTHON": "/usr/local/bin/python",
        "WHOSCORED_SOURCE_CIRCUIT_PATH": (
            "/opt/airflow/logs/whoscored/source-circuit-v1.json"
        ),
        "WHOSCORED_SOURCE_CIRCUIT_WAIT": "0",
    },
    "flaresolverr": {
        "CAPTCHA_SOLVER": "none",
        "LOG_HTML": "false",
        "LOG_LEVEL": "info",
        "TZ": "UTC",
    },
    "flaresolverr_whoscored_paid": {
        "CAPTCHA_SOLVER": "none",
        "LOG_HTML": "false",
        "LOG_LEVEL": "info",
        "TZ": "UTC",
        "WHOSCORED_FLARESOLVERR_PAID_EXCLUSIVE": "1",
    },
    "whoscored_paid_gateway": {
        "ALERT_ENV": "prod",
        "CONNECTION_CHECK_MAX_COUNT": "0",
        "WHOSCORED_PAID_ALERT_AUTHORITY_ROOT": (
            "/opt/airflow/secure/whoscored-alert-authority"
        ),
        "WHOSCORED_PAID_ALERT_RECEIPT_ROOT": (
            "/opt/airflow/state/whoscored-paid-gateway/alert-receipts"
        ),
    },
    "whoscored_proxy_filter": {
        "CONNECTION_CHECK_MAX_COUNT": "0",
        "PROXY_FILTER_ALLOW_FILE_FALLBACK": "false",
        "TM_PROXY_CONTROL_TOKEN": "",
        "WHOSCORED_PROXY_CAMPAIGN_LEDGER_PATH": (
            "/opt/airflow/state/whoscored-proxy-filter/whoscored_campaigns.json"
        ),
    },
}
_AIRFLOW_IMAGE_ENVIRONMENT = {
    "AIRFLOW_CONFIG": "/usr/local/share/whoscored/airflow.cfg",
    "GUNICORN_CMD_ARGS": "--worker-tmp-dir /dev/shm --no-control-socket",
    "LD_LIBRARY_PATH": "",
    "PATH": (
        "/opt/spark/bin:/usr/lib/jvm/java-17-openjdk-amd64/bin:/root/bin:"
        "/home/airflow/.local/bin:/usr/local/bin:/usr/local/sbin:"
        "/usr/local/bin:/usr/sbin:/usr/bin:/sbin:/bin"
    ),
    "PYTHONNOUSERSITE": "1",
    "PYTHONPATH": "",
    "PYTHONPYCACHEPREFIX": "/__whoscored_runtime_bytecode_disabled__",
    "PYTHONSAFEPATH": "1",
    "PYTHONDONTWRITEBYTECODE": "1",
}
_EXPECTED_IMAGE_ENVIRONMENT = {
    "airflow-scheduler": _AIRFLOW_IMAGE_ENVIRONMENT,
    "flaresolverr": {
        "PATH": "/usr/local/bin:/usr/local/sbin:/usr/local/bin:/usr/sbin:/usr/bin:/sbin:/bin"
    },
    "flaresolverr_whoscored_paid": {
        "PATH": "/usr/local/bin:/usr/local/sbin:/usr/local/bin:/usr/sbin:/usr/bin:/sbin:/bin"
    },
    "whoscored_paid_gateway": _AIRFLOW_IMAGE_ENVIRONMENT,
    "whoscored_proxy_filter": _AIRFLOW_IMAGE_ENVIRONMENT,
}
_EXPECTED_MASKED_PATHS = (
    "/proc/acpi",
    "/proc/asound",
    "/proc/interrupts",
    "/proc/kcore",
    "/proc/keys",
    "/proc/latency_stats",
    "/proc/sched_debug",
    "/proc/scsi",
    "/proc/timer_list",
    "/proc/timer_stats",
    "/sys/devices/virtual/powercap",
    "/sys/firmware",
)
_EXPECTED_READONLY_PATHS = (
    "/proc/bus",
    "/proc/fs",
    "/proc/irq",
    "/proc/sys",
    "/proc/sysrq-trigger",
)
_SAFE_IMAGE_LABEL_PREFIXES = (
    "org.apache.airflow.",
    "org.opencontainers.image.",
)
_EXPECTED_SHM_SIZE = {
    "airflow-scheduler": 536_870_912,
    "flaresolverr": 1_073_741_824,
    "flaresolverr_whoscored_paid": 1_073_741_824,
    "whoscored_paid_gateway": 67_108_864,
    "whoscored_proxy_filter": 67_108_864,
}
_EXPECTED_PORT_BINDINGS = {
    "airflow-scheduler": {},
    "flaresolverr": {"8191/tcp": ({"HostIp": "127.0.0.1", "HostPort": "8191"},)},
    "flaresolverr_whoscored_paid": {},
    "whoscored_paid_gateway": {},
    "whoscored_proxy_filter": {},
}


class AdmissionError(RuntimeError):
    """Raised when immutable deployment admission cannot be proven."""


@dataclass(frozen=True)
class ValidatedBindingsEvidence:
    """Exact snapshots and identities used to admit one release binding."""

    bindings: Mapping[str, str]
    build_attestation_raw: bytes
    build_attestation_identity: tuple[int, ...]
    build_manifest_raw: bytes
    build_manifest_identity: tuple[int, ...]
    deployment_attestation_raw: bytes
    deployment_attestation_identity: tuple[int, ...]
    validated_release_revision: str
    validated_payload_revision: str
    validated_manifest_sha256: str
    validated_source_tree_sha256: str
    validated_payload_image_ids: Mapping[str, str]


@dataclass(frozen=True)
class ProtectedComposeInputEvidence:
    """One fd-stable, protected Compose input captured by this admission."""

    raw: bytes
    identity: tuple[int, ...]

    @property
    def sha256(self) -> str:
        return hashlib.sha256(self.raw).hexdigest()


class _DuplicateKey(ValueError):
    pass


def _unique_object(pairs: list[tuple[str, Any]]) -> dict[str, Any]:
    result: dict[str, Any] = {}
    for key, value in pairs:
        if key in result:
            raise _DuplicateKey(key)
        result[key] = value
    return result


def _canonical_bytes(value: object) -> bytes:
    return (
        json.dumps(value, ensure_ascii=False, separators=(",", ":"), sort_keys=True)
        + "\n"
    ).encode("utf-8")


def _authority_canonical_bytes(value: object) -> bytes:
    """Mirror the no-newline canonical encoding used by the offline signer."""

    try:
        return json.dumps(
            value,
            ensure_ascii=False,
            allow_nan=False,
            separators=(",", ":"),
            sort_keys=True,
        ).encode("utf-8")
    except (TypeError, ValueError) as exc:
        raise AdmissionError("owner authority contains non-canonical JSON") from exc


def _stat_identity(value: os.stat_result) -> tuple[int, ...]:
    return (
        value.st_dev,
        value.st_ino,
        value.st_mode,
        value.st_uid,
        value.st_gid,
        value.st_nlink,
        value.st_size,
        value.st_mtime_ns,
        value.st_ctime_ns,
    )


def _read_regular_file(path: Path, *, label: str) -> tuple[bytes, tuple[int, ...]]:
    flags = os.O_RDONLY | getattr(os, "O_CLOEXEC", 0) | getattr(os, "O_NOFOLLOW", 0)
    try:
        descriptor = os.open(path, flags)
    except OSError as exc:
        raise AdmissionError(
            f"{label} is missing, symlinked, or unreadable: {path}"
        ) from exc
    try:
        before = os.fstat(descriptor)
        if not stat.S_ISREG(before.st_mode):
            raise AdmissionError(f"{label} is not a regular file: {path}")
        chunks: list[bytes] = []
        size = 0
        while True:
            chunk = os.read(descriptor, 1024 * 1024)
            if not chunk:
                break
            size += len(chunk)
            if size > MAX_JSON_BYTES:
                raise AdmissionError(f"{label} exceeds {MAX_JSON_BYTES} bytes")
            chunks.append(chunk)
        after = os.fstat(descriptor)
        if _stat_identity(before) != _stat_identity(after):
            raise AdmissionError(f"{label} changed while it was read: {path}")
        return b"".join(chunks), _stat_identity(after)
    except OSError as exc:
        raise AdmissionError(f"cannot read {label}: {path}") from exc
    finally:
        os.close(descriptor)


def _load_json_object(
    path: Path,
    *,
    label: str,
    canonical: bool,
) -> tuple[dict[str, Any], bytes, tuple[int, ...]]:
    raw, identity = _read_regular_file(path, label=label)
    try:
        value = json.loads(raw.decode("utf-8"), object_pairs_hook=_unique_object)
    except (_DuplicateKey, UnicodeDecodeError, json.JSONDecodeError) as exc:
        raise AdmissionError(f"{label} is not unambiguous JSON: {path}") from exc
    if not isinstance(value, dict):
        raise AdmissionError(f"{label} must contain one JSON object: {path}")
    if canonical and raw != _canonical_bytes(value):
        raise AdmissionError(f"{label} is not canonical JSON: {path}")
    return value, raw, identity


def _final_image_bindings(
    deployment: Mapping[str, Any],
    *,
    expected_payloads: Mapping[str, str],
) -> dict[str, str]:
    if set(deployment) != {
        "images",
        "provenance_manifest_sha256",
        "schema_version",
        "status",
    }:
        raise AdmissionError("deployment attestation schema is invalid")
    manifest_digest = deployment.get("provenance_manifest_sha256")
    if (
        deployment.get("schema_version") != 1
        or deployment.get("status") != "ready-v1"
        or not isinstance(manifest_digest, str)
        or _DIGEST.fullmatch(manifest_digest) is None
    ):
        raise AdmissionError("deployment attestation identity is invalid")
    images = deployment.get("images")
    if not isinstance(images, (list, tuple)):
        raise AdmissionError("deployment attestation images must be a sequence")
    observed_payloads: dict[str, str] = {}
    final_images: dict[str, str] = {}
    prior = ""
    for record in images:
        if not isinstance(record, Mapping) or set(record) != {
            "final_image",
            "payload_image_id",
            "service",
        }:
            raise AdmissionError("deployment image record schema is invalid")
        service = record.get("service")
        payload = record.get("payload_image_id")
        final_image = record.get("final_image")
        if (
            not isinstance(service, str)
            or not service
            or service <= prior
            or service in observed_payloads
            or not isinstance(payload, str)
            or _IMAGE_ID.fullmatch(payload) is None
            or not isinstance(final_image, str)
            or _PINNED_IMAGE.fullmatch(final_image) is None
        ):
            raise AdmissionError(
                "deployment image records are duplicated, unsorted, or mutable"
            )
        prior = service
        observed_payloads[service] = payload
        final_images[service] = final_image
    if observed_payloads != dict(expected_payloads):
        raise AdmissionError(
            "deployment attestation has extra, missing, or changed services"
        )
    protected = {
        service: final_images[service]
        for service in PROTECTED_SERVICES
        if service in final_images
    }
    if set(protected) != _PROTECTED_SERVICE_SET:
        raise AdmissionError(
            "deployment attestation does not bind every protected service"
        )
    return protected


def _validate_bindings_and_discovery(
    *,
    root: Path,
    attestation_path: Path,
    manifest_path: Path,
    deployment_attestation_path: Path,
) -> tuple[dict[str, str], Any]:

    if frozenset(provenance.PROTECTED_PRODUCTION_SERVICES) != _PROTECTED_SERVICE_SET:
        raise AdmissionError("validator and admission protected-service sets differ")
    try:
        discovery = provenance.validate(
            root,
            attestation_path=attestation_path,
            manifest_path=manifest_path,
            deployment_attestation_path=deployment_attestation_path,
            expect_blocked=False,
        )
    except provenance.ProvenanceError as exc:
        raise AdmissionError(str(exc)) from exc
    deployment = discovery.deployment_attestation
    deployment_raw = discovery.deployment_attestation_raw
    validator_final_images = discovery.deployment_final_images
    if (
        not isinstance(deployment, Mapping)
        or not isinstance(deployment_raw, bytes)
        or not deployment_raw
        or not isinstance(validator_final_images, Mapping)
    ):
        raise AdmissionError(
            "validator did not return its fd-pinned deployment attestation"
        )
    local_images = discovery.records.get("local_images")
    if not isinstance(local_images, list) or not local_images:
        raise AdmissionError("validated provenance has no local image records")
    expected_payloads: dict[str, str] = {}
    for record in local_images:
        if not isinstance(record, dict):
            raise AdmissionError("validated local image record is invalid")
        service = record.get("service")
        payload = record.get("payload_image_id")
        if (
            not isinstance(service, str)
            or not service
            or service in expected_payloads
            or not isinstance(payload, str)
            or _IMAGE_ID.fullmatch(payload) is None
        ):
            raise AdmissionError("validated local image bindings are invalid")
        expected_payloads[service] = payload
    if list(expected_payloads) != sorted(expected_payloads):
        raise AdmissionError("validated local image bindings are not sorted")
    bindings = _final_image_bindings(deployment, expected_payloads=expected_payloads)
    observed_final_images = {
        str(service): str(image) for service, image in validator_final_images.items()
    }
    images = deployment.get("images")
    assert isinstance(images, (list, tuple))
    parsed_final_images = {
        str(record["service"]): str(record["final_image"])
        for record in images
        if isinstance(record, Mapping)
    }
    if observed_final_images != parsed_final_images:
        raise AdmissionError("validator final-image bindings differ from attestation")
    return bindings, discovery


def validate_bindings(
    *,
    root: Path,
    attestation_path: Path,
    manifest_path: Path,
    deployment_attestation_path: Path,
) -> dict[str, str]:
    """Validate provenance and return every immutable protected image ref."""

    bindings, _ = _validate_bindings_and_discovery(
        root=root,
        attestation_path=attestation_path,
        manifest_path=manifest_path,
        deployment_attestation_path=deployment_attestation_path,
    )
    return bindings


def _evidence_identity(value: object, *, label: str) -> tuple[int, ...]:
    if (
        not isinstance(value, tuple)
        or len(value) != 9
        or any(type(item) is not int for item in value)
    ):
        raise AdmissionError(f"validator returned an invalid {label} identity")
    return value


def _evidence_raw(value: object, *, label: str) -> bytes:
    if not isinstance(value, bytes) or not value:
        raise AdmissionError(f"validator did not return its fd-pinned {label}")
    return value


def validate_bindings_with_evidence(
    *,
    root: Path,
    attestation_path: Path,
    manifest_path: Path,
    deployment_attestation_path: Path,
) -> ValidatedBindingsEvidence:
    """Return bindings and the exact fd snapshots used to validate them."""

    bindings, discovery = _validate_bindings_and_discovery(
        root=root,
        attestation_path=attestation_path,
        manifest_path=manifest_path,
        deployment_attestation_path=deployment_attestation_path,
    )
    release_revision = getattr(discovery, "validated_release_revision", None)
    payload_revision = getattr(discovery, "validated_payload_revision", None)
    manifest_digest = getattr(discovery, "validated_manifest_sha256", None)
    source_tree_digest = getattr(discovery, "validated_source_tree_sha256", None)
    payload_image_ids = getattr(discovery, "validated_payload_image_ids", None)
    if (
        not isinstance(release_revision, str)
        or _COMMIT.fullmatch(release_revision) is None
    ):
        raise AdmissionError("validator did not preserve the release revision")
    if (
        not isinstance(payload_revision, str)
        or _COMMIT.fullmatch(payload_revision) is None
    ):
        raise AdmissionError("validator did not preserve the payload revision")
    if (
        not isinstance(manifest_digest, str)
        or _DIGEST.fullmatch(manifest_digest) is None
    ):
        raise AdmissionError("validator did not preserve the manifest digest")
    if (
        not isinstance(source_tree_digest, str)
        or _DIGEST.fullmatch(source_tree_digest) is None
    ):
        raise AdmissionError("validator did not preserve the source-tree digest")
    if not isinstance(payload_image_ids, Mapping):
        raise AdmissionError("validator did not preserve payload image bindings")
    normalized_payloads: dict[str, str] = {}
    for service, image_id in payload_image_ids.items():
        if (
            not isinstance(service, str)
            or not service
            or service in normalized_payloads
            or not isinstance(image_id, str)
            or _IMAGE_ID.fullmatch(image_id) is None
        ):
            raise AdmissionError("validator returned invalid payload image bindings")
        normalized_payloads[service] = image_id
    local_images = discovery.records.get("local_images")
    expected_payloads = {
        str(record["service"]): str(record["payload_image_id"])
        for record in local_images
        if isinstance(record, Mapping)
    }
    if normalized_payloads != expected_payloads:
        raise AdmissionError("validator payload image bindings differ from manifest")
    return ValidatedBindingsEvidence(
        bindings=types.MappingProxyType(dict(bindings)),
        build_attestation_raw=_evidence_raw(
            getattr(discovery, "build_attestation_raw", None),
            label="build attestation",
        ),
        build_attestation_identity=_evidence_identity(
            getattr(discovery, "build_attestation_identity", None),
            label="build attestation",
        ),
        build_manifest_raw=_evidence_raw(
            getattr(discovery, "build_manifest_raw", None),
            label="build manifest",
        ),
        build_manifest_identity=_evidence_identity(
            getattr(discovery, "build_manifest_identity", None),
            label="build manifest",
        ),
        deployment_attestation_raw=_evidence_raw(
            getattr(discovery, "deployment_attestation_raw", None),
            label="deployment attestation",
        ),
        deployment_attestation_identity=_evidence_identity(
            getattr(discovery, "deployment_attestation_identity", None),
            label="deployment attestation",
        ),
        validated_release_revision=release_revision,
        validated_payload_revision=payload_revision,
        validated_manifest_sha256=manifest_digest,
        validated_source_tree_sha256=source_tree_digest,
        validated_payload_image_ids=types.MappingProxyType(normalized_payloads),
    )


def compose_override_bytes(
    bindings: Mapping[str, str],
    services: Sequence[str] = PROTECTED_SERVICES,
) -> bytes:
    if set(bindings) != _PROTECTED_SERVICE_SET:
        raise AdmissionError("override bindings must name every protected service")
    selected = tuple(services)
    if (
        not selected
        or len(selected) != len(set(selected))
        or any(service not in _PROTECTED_SERVICE_SET for service in selected)
    ):
        raise AdmissionError("override services must be a unique protected subset")
    lines = ["services:"]
    for service in selected:
        image = bindings[service]
        if not isinstance(image, str) or _PINNED_IMAGE.fullmatch(image) is None:
            raise AdmissionError(f"protected service has a mutable image: {service}")
        lines.extend(
            (
                f"  {service}:",
                "    build: !reset null",
                f"    image: {json.dumps(image, ensure_ascii=True)}",
            )
        )
    return ("\n".join(lines) + "\n").encode("ascii")


def _write_all(descriptor: int, payload: bytes) -> None:
    offset = 0
    while offset < len(payload):
        written = os.write(descriptor, payload[offset:])
        if written <= 0:
            raise AdmissionError("short write while creating admission output")
        offset += written


def _open_protected_output_parent(path: Path) -> tuple[int, str]:
    """Walk an absolute output path without symlinks and return its parent fd."""

    try:
        return provenance.open_protected_parent(path, label="admission output")
    except provenance.ProvenanceError as exc:
        raise AdmissionError(str(exc)) from exc
    except OSError as exc:
        raise AdmissionError(
            f"admission output has an unsafe or symlinked parent: {path}"
        ) from exc


def write_new_regular_file(path: Path, payload: bytes) -> None:
    """Publish a complete mode-0600 file atomically without overwriting."""

    if not path.is_absolute() or not path.name:
        raise AdmissionError("admission output path must be an absolute file path")
    directory, output_name = _open_protected_output_parent(path)
    temporary = f".{path.name}.tmp-{os.getpid()}-{secrets.token_hex(8)}"
    descriptor: int | None = None
    linked = False
    try:
        flags = (
            os.O_WRONLY
            | os.O_CREAT
            | os.O_EXCL
            | getattr(os, "O_CLOEXEC", 0)
            | getattr(os, "O_NOFOLLOW", 0)
        )
        descriptor = os.open(temporary, flags, 0o600, dir_fd=directory)
        os.fchown(descriptor, 0, 0)
        os.fchmod(descriptor, 0o600)
        _write_all(descriptor, payload)
        os.fsync(descriptor)
        os.close(descriptor)
        descriptor = None
        try:
            os.link(
                temporary,
                output_name,
                src_dir_fd=directory,
                dst_dir_fd=directory,
                follow_symlinks=False,
            )
        except OSError as exc:
            if exc.errno == errno.EEXIST:
                raise AdmissionError(
                    f"admission output already exists and will not be overwritten: {path}"
                ) from exc
            raise AdmissionError(f"cannot publish admission output: {path}") from exc
        linked = True
        os.fsync(directory)
    finally:
        if descriptor is not None:
            os.close(descriptor)
        try:
            os.unlink(temporary, dir_fd=directory)
        except FileNotFoundError:
            pass
        if linked:
            os.fsync(directory)
        os.close(directory)
    try:
        captured = provenance.read_protected_regular_file(
            path, label="admission output"
        )
    except provenance.ProvenanceError as exc:
        raise AdmissionError(str(exc)) from exc
    if not hmac.compare_digest(captured, payload):
        raise AdmissionError("published admission output differs from requested bytes")


def verify_override_snapshot(
    path: Path,
    bindings: Mapping[str, str],
    services: Sequence[str] = PROTECTED_SERVICES,
) -> tuple[bytes, tuple[int, ...]]:
    """Verify one protected override read and return that exact snapshot."""

    try:
        actual, identity = provenance.read_protected_regular_file_snapshot(
            path, label="production Compose override"
        )
    except provenance.ProvenanceError as exc:
        raise AdmissionError(str(exc)) from exc
    expected = compose_override_bytes(bindings, services)
    if not hmac.compare_digest(actual, expected):
        raise AdmissionError(
            "production Compose override differs from the attested digest-only model"
        )
    return actual, identity


def verify_override(
    path: Path,
    bindings: Mapping[str, str],
    services: Sequence[str] = PROTECTED_SERVICES,
) -> None:
    verify_override_snapshot(path, bindings, services)


def _string_sequence(value: object, *, label: str) -> tuple[str, ...] | None:
    if value is None:
        return None
    if not isinstance(value, list) or not all(isinstance(item, str) for item in value):
        raise AdmissionError(f"{label} must be a string sequence")
    return tuple(value)


def _normal_capabilities(value: object, *, label: str) -> frozenset[str]:
    sequence = _string_sequence(value, label=label) or ()
    normalized: set[str] = set()
    for item in sequence:
        capability = item.upper().removeprefix("CAP_")
        if not capability or capability in normalized:
            raise AdmissionError(f"{label} contains duplicate or invalid capabilities")
        normalized.add(capability)
    return frozenset(normalized)


def _forbidden_environment_names(
    environment: Mapping[str, str], *, include_empty: bool
) -> set[str]:
    exact = {
        "BASH_ENV",
        "BASHOPTS",
        "CDPATH",
        "ENV",
        "GCONV_PATH",
        "GLIBC_TUNABLES",
        "IFS",
        "LOCPATH",
        "MALLOC_TRACE",
        "NODE_OPTIONS",
        "PERL5OPT",
        "PYTHONHOME",
        "PYTHONBREAKPOINT",
        "PYTHONCASEOK",
        "PYTHONDEBUG",
        "PYTHONINSPECT",
        "PYTHONPATH",
        "PYTHONPLATLIBDIR",
        "PYTHONSTARTUP",
        "PYTHONUSERBASE",
        "PYTHONWARNINGS",
        "ALL_PROXY",
        "CURL_CA_BUNDLE",
        "HTTP_PROXY",
        "HTTPS_PROXY",
        "JDK_JAVA_OPTIONS",
        "JAVA_TOOL_OPTIONS",
        "REQUESTS_CA_BUNDLE",
        "SSL_CERT_FILE",
        "_JAVA_OPTIONS",
        "all_proxy",
        "http_proxy",
        "https_proxy",
        "RUBYOPT",
        "SHELLOPTS",
    }
    return {
        name
        for name, value in environment.items()
        if (include_empty or value)
        and (name in exact or name.startswith(("LD_", "DYLD_")))
    }


def _validate_whoscored_store_uris(environment: Mapping[str, str]) -> None:
    """Bind both WhoScored stores to the one admitted physical S3 bucket."""

    bucket = environment.get("ICEBERG_WAREHOUSE", "")
    if (
        re.fullmatch(r"[a-z0-9][a-z0-9.-]{1,61}[a-z0-9]", bucket) is None
        or ".." in bucket
        or ".-" in bucket
        or "-." in bucket
        or re.fullmatch(r"[0-9]+(?:\.[0-9]+){3}", bucket) is not None
    ):
        raise AdmissionError("rendered ICEBERG_WAREHOUSE bucket is invalid")
    expected = {
        "WHOSCORED_RAW_STORE_URI": f"s3://{bucket}/raw/whoscored",
        "WHOSCORED_OPS_STORE_URI": f"s3://{bucket}/ops/whoscored",
    }
    if any(
        not hmac.compare_digest(environment.get(name, ""), value)
        for name, value in expected.items()
    ):
        raise AdmissionError(
            "rendered WhoScored raw/ops stores differ from admitted ICEBERG_WAREHOUSE"
        )


def _validate_scheduler_store_uris(environment: Mapping[str, str]) -> None:
    """Bind every protected scheduler source store to its warehouse prefix."""

    _validate_whoscored_store_uris(environment)
    bucket = environment["ICEBERG_WAREHOUSE"]
    if not hmac.compare_digest(
        environment.get("TRANSFERMARKT_RAW_STORE_URI", ""),
        f"s3://{bucket}/raw/transfermarkt",
    ):
        raise AdmissionError(
            "rendered source raw/ops stores differ from admitted ICEBERG_WAREHOUSE"
        )


def _validate_rendered_environment(
    environment: Mapping[str, str],
    *,
    service: str,
    operational_cap_bytes: int = 1_000_000_000,
    provider_order_cap_bytes: int = 1_000_000_000,
) -> None:
    expected_names = _EXPECTED_ENVIRONMENT_NAMES[service]
    legacy_scheduler_names = (
        expected_names
        - {
            "WHOSCORED_SCHEDULED_PAID_MODE",
            "WHOSCORED_SCHEDULED_PAID_POINTER_ROOT",
        }
    ) | {"WHOSCORED_PROXY_APPROVAL_PATH"}
    legacy_scheduler = (
        service == "airflow-scheduler" and set(environment) == legacy_scheduler_names
    )
    if set(environment) != expected_names and not legacy_scheduler:
        raise AdmissionError(f"rendered environment names differ: {service}")
    if any(
        environment.get(name) != value
        for name, value in _FIXED_ENVIRONMENT[service].items()
        if not legacy_scheduler
        or name
        not in {
            "WHOSCORED_SCHEDULED_PAID_MODE",
            "WHOSCORED_SCHEDULED_PAID_POINTER_ROOT",
        }
    ):
        raise AdmissionError(f"rendered security environment differs: {service}")
    if service == "airflow-scheduler" and environment.get(
        "WHOSCORED_SOURCE_POOL_SLOTS"
    ) not in {"2", "3", "4"}:
        raise AdmissionError("rendered WhoScored source-pool size differs")
    if service in {"airflow-scheduler", "whoscored_paid_gateway"} and environment.get(
        "WHOSCORED_PAID_BATCH_ENABLED"
    ) not in {"0", "1"}:
        raise AdmissionError("rendered WhoScored paid-batch control differs")
    if service == "airflow-scheduler":
        _validate_scheduler_store_uris(environment)
        expected_sofascore_artifact_id = environment.get(
            "SOFASCORE_PROXY_BUDGET_ARTIFACT_ID", ""
        )
        if (
            _DIGEST.fullmatch(expected_sofascore_artifact_id) is None
            or expected_sofascore_artifact_id == _ZERO_DIGEST
        ):
            raise AdmissionError("rendered SofaScore artifact ID is invalid")
        approval_path = environment.get("WHOSCORED_PROXY_APPROVAL_PATH", "")
        if (
            legacy_scheduler
            and approval_path
            and _WHOSCORED_APPROVAL_PATH_RE.fullmatch(approval_path) is None
        ):
            raise AdmissionError("rendered WhoScored approval path differs")
        if len(environment.get("FBREF_PROXY_CONTROL_TOKEN", "").strip()) < 32:
            raise AdmissionError("rendered FBref proxy-control token is invalid")
        tm_boolean_names = (
            "TM_NATIVE_V2_ENABLED",
            "TM_STANDING_POLICY_ENABLED",
            "TM_REQUIRE_METERED_PROXY",
        )
        if any(
            environment.get(name) not in {"true", "false"} for name in tm_boolean_names
        ):
            raise AdmissionError("rendered Transfermarkt boolean controls differ")
        if environment.get("TM_NATIVE_V2_ENABLED") == "true" and (
            environment.get("TM_STANDING_POLICY_ENABLED") != "true"
            or environment.get("TM_REQUIRE_METERED_PROXY") != "true"
            or environment.get("TM_PROXY_CONTROL_URL") != "http://proxy_filter:8899"
            or len(environment.get("TM_PROXY_CONTROL_TOKEN", "").strip()) < 32
        ):
            raise AdmissionError(
                "rendered Transfermarkt paid controls are not fail-closed"
            )
        backfill_token = environment.get("TM_BACKFILL_PROXY_CONTROL_TOKEN", "").strip()
        if backfill_token and (
            len(backfill_token) < 32
            or backfill_token
            in {
                environment.get("PROXY_FILTER_CONTROL_TOKEN", "").strip(),
                environment.get("SOFASCORE_PROXY_CONTROL_TOKEN", "").strip(),
                environment.get("TM_PROXY_CONTROL_TOKEN", "").strip(),
            }
        ):
            raise AdmissionError(
                "rendered Transfermarkt backfill controls are not fail-closed"
            )
    if service == "whoscored_proxy_filter":
        if (
            re.fullmatch(
                r"[A-Za-z0-9][A-Za-z0-9._:-]{0,127}",
                environment.get("WHOSCORED_PROVIDER_ORDER_ID", ""),
            )
            is None
            or _DIGEST.fullmatch(
                environment.get("WHOSCORED_PROVIDER_POLICY_SHA256", "")
            )
            is None
        ):
            raise AdmissionError("rendered WhoScored provider-policy identity differs")
        _positive_capped_decimal(
            environment.get("WHOSCORED_PROVIDER_ORDER_CAP_BYTES", ""),
            flag="whoscored-provider-order-cap-bytes",
            maximum=1_000_000_000,
        )
        if (
            int(environment["WHOSCORED_PROVIDER_ORDER_CAP_BYTES"])
            != provider_order_cap_bytes
        ):
            raise AdmissionError("rendered WhoScored provider order cap differs")
        _positive_capped_decimal(
            environment.get("WHOSCORED_PROXY_FILTER_DAILY_BUDGET_BYTES", ""),
            flag="daily-budget-bytes",
            maximum=operational_cap_bytes,
        )
        if environment.get("WHOSCORED_PROXY_FILTER_MAX_LEASE_BYTES") != "2000000":
            raise AdmissionError("rendered max-lease-bytes differs from policy")
    if (
        service == "airflow-scheduler"
        and len(environment.get("WHOSCORED_PAID_GATEWAY_TOKEN", "").strip()) < 32
    ):
        raise AdmissionError("rendered WhoScored paid-gateway token is invalid")
    if service == "whoscored_paid_gateway":
        authority_root = Path("/opt/airflow/secure/whoscored-alert-authority")
        authority_paths = tuple(
            environment.get(name, "")
            for name in (
                "WHOSCORED_PAID_ALERT_SECRET_PATH",
                "WHOSCORED_PAID_ALERT_BINDING_PATH",
            )
        )
        if len(set(authority_paths)) != 2 or any(
            not value
            or value != Path(value).as_posix()
            or ".." in Path(value).parts
            or authority_root not in Path(value).parents
            for value in authority_paths
        ):
            raise AdmissionError(
                f"rendered paid-alert authority paths are invalid: {service}"
            )
    secret_names = {
        "flaresolverr_whoscored_paid": ("WHOSCORED_FLARESOLVERR_GATEWAY_SECRET",),
        "whoscored_paid_gateway": (
            "WHOSCORED_FLARESOLVERR_GATEWAY_SECRET",
            "WHOSCORED_PAID_ALERT_HMAC_SECRET",
            "WHOSCORED_PAID_GATEWAY_TOKEN",
            "WHOSCORED_PROXY_APPROVAL_HMAC_SECRET",
            "WHOSCORED_PROXY_CONTROL_TOKEN",
        ),
        "whoscored_proxy_filter": (
            "PROXY_FILTER_CONTROL_TOKEN",
            "WHOSCORED_PROXY_APPROVAL_HMAC_SECRET",
            "WHOSCORED_PROXY_LEDGER_HMAC_SECRET",
        ),
    }
    if any(
        len(environment.get(name, "").strip()) < 32
        for name in secret_names.get(service, ())
    ):
        raise AdmissionError(f"rendered boundary secret is invalid: {service}")


def _rendered_volumes(
    model: Mapping[str, Any], *, service: str
) -> tuple[tuple[Any, ...], ...]:
    volumes = model.get("volumes")
    if volumes is None:
        return ()
    if not isinstance(volumes, list):
        raise AdmissionError(f"rendered volumes are invalid: {service}")
    result: list[tuple[Any, ...]] = []
    targets: set[str] = set()
    for volume in volumes:
        if not isinstance(volume, dict):
            raise AdmissionError(f"rendered volume is invalid: {service}")
        volume_type = volume.get("type")
        source = volume.get("source", "")
        target = volume.get("target")
        read_only = volume.get("read_only", False)
        if volume_type not in ("bind", "volume"):
            raise AdmissionError(f"rendered volume identity is invalid: {service}")
        expected_keys = {"source", "target", "type", volume_type}
        if read_only:
            expected_keys.add("read_only")
        if (
            set(volume) != expected_keys
            or not isinstance(source, str)
            or not source
            or volume_type == "bind"
            and not Path(source).is_absolute()
            or not isinstance(target, str)
            or not target.startswith("/")
            or target in targets
            or not isinstance(read_only, bool)
        ):
            raise AdmissionError(f"rendered volume identity is invalid: {service}")
        options = volume.get(volume_type)
        if (
            volume_type == "bind"
            and options not in ({}, {"create_host_path": False})
            or volume_type == "volume"
            and options != {}
        ):
            raise AdmissionError(f"rendered volume options differ: {service}")
        targets.add(target)
        result.append((volume_type, source, target, read_only))
    return tuple(sorted(result))


def _tmpfs_model(
    model: Mapping[str, Any], *, service: str
) -> dict[str, frozenset[str]]:
    entries = model.get("tmpfs")
    if entries is None:
        return {}
    if not isinstance(entries, list):
        raise AdmissionError(f"rendered tmpfs is invalid: {service}")
    result: dict[str, frozenset[str]] = {}
    for entry in entries:
        if not isinstance(entry, str):
            raise AdmissionError(f"rendered tmpfs entry is invalid: {service}")
        target, separator, raw_options = entry.partition(":")
        options = raw_options.split(",") if separator else []
        if (
            not target.startswith("/")
            or target in result
            or not options
            or any(not option for option in options)
            or len(options) != len(set(options))
        ):
            raise AdmissionError(f"rendered tmpfs entry is invalid: {service}")
        result[target] = frozenset(options)
    return result


def _mount_shadows_image_path(service: str, target: str) -> bool:
    normalized = target.rstrip("/") or "/"
    return any(
        protected == normalized
        or protected.startswith(normalized + "/")
        or normalized.startswith(protected + "/")
        for protected in _CRITICAL_IMAGE_PATHS[service]
    )


def _duration_nanoseconds(value: object, *, label: str) -> int:
    if not isinstance(value, str) or not value:
        raise AdmissionError(f"{label} is not a duration")
    units = {
        "h": 3_600_000_000_000,
        "m": 60_000_000_000,
        "s": 1_000_000_000,
        "ms": 1_000_000,
        "us": 1_000,
        "ns": 1,
    }
    position = 0
    total = 0
    pattern = re.compile(r"([0-9]+)(h|ms|us|ns|m|s)")
    for match in pattern.finditer(value):
        if match.start() != position:
            raise AdmissionError(f"{label} is not a canonical duration")
        total += int(match.group(1)) * units[match.group(2)]
        position = match.end()
    if position != len(value) or total <= 0:
        raise AdmissionError(f"{label} is not a canonical duration")
    return total


def _healthcheck_projection(
    model: Mapping[str, Any], *, service: str
) -> dict[str, Any]:
    healthcheck = model.get("healthcheck")
    if not isinstance(healthcheck, dict) or set(healthcheck) != {
        "interval",
        "retries",
        "start_period",
        "test",
        "timeout",
    }:
        raise AdmissionError(f"rendered healthcheck schema differs: {service}")
    test = _string_sequence(
        healthcheck.get("test"), label=f"rendered healthcheck test for {service}"
    )
    retries = healthcheck.get("retries")
    if not isinstance(retries, int) or isinstance(retries, bool) or retries <= 0:
        raise AdmissionError(f"rendered healthcheck retries differ: {service}")
    projection = {
        "Interval": _duration_nanoseconds(
            healthcheck.get("interval"), label=f"healthcheck interval for {service}"
        ),
        "Retries": retries,
        "StartPeriod": _duration_nanoseconds(
            healthcheck.get("start_period"),
            label=f"healthcheck start period for {service}",
        ),
        "Test": test,
        "Timeout": _duration_nanoseconds(
            healthcheck.get("timeout"), label=f"healthcheck timeout for {service}"
        ),
    }
    if projection != _EXPECTED_HEALTHCHECKS[service]:
        raise AdmissionError(f"rendered healthcheck policy differs: {service}")
    return {
        **projection,
        "Test": tuple(item.replace("$$", "$") for item in projection["Test"] or ()),
    }


def _positive_capped_decimal(value: str, *, flag: str, maximum: int) -> None:
    if re.fullmatch(r"[1-9][0-9]*", value) is None or int(value) > maximum:
        raise AdmissionError(f"rendered {flag} exceeds admission policy")


def _command_projection(
    command: tuple[str, ...] | None,
    *,
    service: str,
    operational_cap_bytes: int = 1_000_000_000,
    provider_order_cap_bytes: int = 1_000_000_000,
) -> tuple[str, ...]:
    if service in {"flaresolverr", "flaresolverr_whoscored_paid"}:
        if command is not None:
            raise AdmissionError(
                "rendered FlareSolverr command bypasses baked preflight"
            )
        return _EXPECTED_COMMANDS[service]
    if service == "airflow-scheduler":
        if command != _EXPECTED_COMMANDS[service]:
            raise AdmissionError("rendered scheduler command differs")
        return command
    if service == "whoscored_paid_gateway":
        if command != _EXPECTED_COMMANDS[service]:
            raise AdmissionError("rendered WhoScored paid-gateway command differs")
        return command
    template = tuple(provenance.WHOSCORED_PROXY_COMMAND)
    if command is None or len(command) != len(template):
        raise AdmissionError("rendered WhoScored proxy command differs")
    variable_limits = {
        "${WHOSCORED_PROXY_FILTER_DAILY_BUDGET_BYTES:?set exact provider-policy daily cap in decimal bytes}": (
            "--daily-budget-bytes",
            operational_cap_bytes,
            None,
        ),
        "${WHOSCORED_PROVIDER_ORDER_CAP_BYTES:?set exact signed provider gross order cap in decimal bytes}": (
            "--whoscored-provider-order-cap-bytes",
            1_000_000_000,
            provider_order_cap_bytes,
        ),
        "${WHOSCORED_PROXY_FILTER_MAX_LEASE_BYTES:-2000000}": (
            "--max-lease-bytes",
            2_000_000,
            None,
        ),
        "${WHOSCORED_PROXY_FILTER_MAX_LEASE_TTL_SECONDS:-3600}": (
            "--max-lease-ttl-seconds",
            3_600,
            None,
        ),
        "${WHOSCORED_PROXY_FILTER_DAGRUN_BUDGET_BYTES:-1000000000}": (
            "--dagrun-budget-bytes",
            1_000_000_000,
            None,
        ),
        "${WHOSCORED_PROXY_FILTER_URL_BUDGET_BYTES:-2000000}": (
            "--url-budget-bytes",
            2_000_000,
            None,
        ),
        "${WHOSCORED_PROXY_FILTER_MAX_ACTIVE_LEASES:-2}": (
            "--max-active-leases",
            2,
            None,
        ),
    }
    for index, expected in enumerate(template):
        variable = variable_limits.get(expected)
        if variable is None:
            if command[index] != expected:
                raise AdmissionError("rendered WhoScored proxy command differs")
            continue
        flag, maximum, exact = variable
        if index == 0 or template[index - 1] != flag:
            raise AdmissionError(
                "WhoScored proxy command template is internally invalid"
            )
        _positive_capped_decimal(command[index], flag=flag, maximum=maximum)
        if exact is not None and int(command[index]) != exact:
            raise AdmissionError(f"rendered {flag} differs from provider policy")
    return command


def verify_rendered_compose(
    rendered: Mapping[str, Any],
    bindings: Mapping[str, str],
    *,
    provider_order_cap_bytes: int | None = None,
    selected_services: Sequence[str] = PROTECTED_SERVICES,
) -> dict[str, dict[str, Any]]:
    selected = tuple(selected_services)
    if (
        not selected
        or len(selected) != len(set(selected))
        or any(service not in _PROTECTED_SERVICE_SET for service in selected)
    ):
        raise AdmissionError("rendered protected service selection is invalid")
    if provider_order_cap_bytes is None:
        gross_cap_bytes = 1_000_000_000
    elif (
        isinstance(provider_order_cap_bytes, bool)
        or not isinstance(provider_order_cap_bytes, int)
        or not 1 <= provider_order_cap_bytes <= 1_000_000_000
    ):
        raise AdmissionError("provider order cap is invalid")
    else:
        gross_cap_bytes = provider_order_cap_bytes
    operational_cap_bytes = gross_cap_bytes * 95 // 100
    services = rendered.get("services")
    if not isinstance(services, dict):
        raise AdmissionError("rendered Compose model has no services mapping")
    expected_network_names = {
        name for service in selected for name in _EXPECTED_NETWORKS[service]
    }
    networks = rendered.get("networks")
    if (
        not isinstance(networks, dict)
        or set(networks) != expected_network_names
        or any(
            networks.get(name) != _EXPECTED_NETWORK_DEFINITIONS[name]
            for name in expected_network_names
        )
    ):
        raise AdmissionError("rendered protected network definitions differ")
    volumes = rendered.get("volumes")
    if not isinstance(volumes, dict) or volumes.get("soccerdata_cache") != {
        "name": "soccerdata_cache"
    }:
        raise AdmissionError("rendered protected volume definition differs")
    projections: dict[str, dict[str, Any]] = {}
    for service in selected:
        model = services.get(service)
        if not isinstance(model, dict):
            raise AdmissionError(
                f"rendered Compose model omits protected service: {service}"
            )
        if model.get("post_start") is not None or model.get("pre_stop") is not None:
            raise AdmissionError(
                f"rendered protected service has lifecycle hooks: {service}"
            )
        if model.get("build") is not None:
            raise AdmissionError(
                f"rendered protected service still has a build: {service}"
            )
        modeled_keys = set(model)
        if model.get("profiles") == ["whoscored-paid"]:
            modeled_keys.discard("profiles")
        if service == "whoscored_proxy_filter" and model.get("depends_on") == {
            "airflow-log-init": {
                "condition": "service_completed_successfully",
                "required": True,
            }
        }:
            modeled_keys.discard("depends_on")
        if modeled_keys != _ALLOWED_RENDERED_KEYS[service]:
            raise AdmissionError(
                f"rendered protected service has unmodeled fields: {service}"
            )
        if model.get("image") != bindings[service]:
            raise AdmissionError(
                f"rendered Compose image differs from deployment attestation: {service}"
            )
        if model.get("entrypoint") is not None:
            raise AdmissionError(
                f"rendered protected service overrides its image entrypoint: {service}"
            )
        if model.get("container_name") != service:
            raise AdmissionError(f"rendered container name differs: {service}")
        observed_depends_on = model.get("depends_on")
        legacy_filter_depends_on = {
            "airflow-log-init": {
                "condition": "service_completed_successfully",
                "required": True,
            }
        }
        if observed_depends_on != _EXPECTED_DEPENDS_ON[service] and not (
            service == "whoscored_proxy_filter"
            and observed_depends_on == legacy_filter_depends_on
        ):
            raise AdmissionError(f"rendered dependency policy differs: {service}")
        if model.get("deploy") != _EXPECTED_DEPLOY[service]:
            raise AdmissionError(f"rendered resource policy differs: {service}")
        if model.get("networks") != _EXPECTED_NETWORKS[service]:
            raise AdmissionError(f"rendered network policy differs: {service}")
        if model.get("restart") != "unless-stopped":
            raise AdmissionError(f"rendered restart policy differs: {service}")
        environment = model.get("environment")
        if not isinstance(environment, dict):
            raise AdmissionError(f"rendered environment policy differs: {service}")
        if any(
            not isinstance(name, str) or not isinstance(value, str)
            for name, value in environment.items()
        ):
            raise AdmissionError(f"rendered environment values differ: {service}")
        if _forbidden_environment_names(environment, include_empty=True):
            raise AdmissionError(f"rendered environment has loader controls: {service}")
        _validate_rendered_environment(
            environment,
            service=service,
            operational_cap_bytes=operational_cap_bytes,
            provider_order_cap_bytes=gross_cap_bytes,
        )
        if model.get("profiles") not in (None, ["whoscored-paid"]):
            raise AdmissionError(f"rendered protected profile differs: {service}")
        raw_shm_size = model.get("shm_size")
        if service in {"whoscored_paid_gateway", "whoscored_proxy_filter"}:
            if raw_shm_size is not None:
                raise AdmissionError(
                    f"rendered paid boundary shm_size differs: {service}"
                )
        elif raw_shm_size != str(_EXPECTED_SHM_SIZE[service]):
            raise AdmissionError(f"rendered shm_size differs: {service}")
        if service == "airflow-scheduler":
            if model.get("memswap_limit") != "17179869184":
                raise AdmissionError("rendered scheduler memswap limit differs")
        elif model.get("memswap_limit") is not None:
            raise AdmissionError(f"rendered memswap limit differs: {service}")
        rendered_ports = model.get("ports")
        if service == "flaresolverr":
            if rendered_ports != [
                {
                    "host_ip": "127.0.0.1",
                    "mode": "ingress",
                    "protocol": "tcp",
                    "published": "8191",
                    "target": 8191,
                }
            ]:
                raise AdmissionError("rendered FlareSolverr port policy differs")
        elif rendered_ports is not None:
            raise AdmissionError(f"rendered protected service adds ports: {service}")
        command = _string_sequence(
            model.get("command"), label=f"rendered command for {service}"
        )
        effective_command = _command_projection(
            command,
            service=service,
            operational_cap_bytes=operational_cap_bytes,
            provider_order_cap_bytes=gross_cap_bytes,
        )
        policy = _SECURITY_POLICY[service]
        privileged = model.get("privileged")
        if privileged is not None and privileged is not False:
            raise AdmissionError(f"rendered protected service is privileged: {service}")
        if bool(model.get("read_only", False)) is not policy["read_only"]:
            raise AdmissionError(f"rendered root filesystem policy differs: {service}")
        cap_add = _normal_capabilities(
            model.get("cap_add"), label=f"rendered cap_add for {service}"
        )
        cap_drop = _normal_capabilities(
            model.get("cap_drop"), label=f"rendered cap_drop for {service}"
        )
        security_opt = (
            _string_sequence(
                model.get("security_opt"), label=f"rendered security_opt for {service}"
            )
            or ()
        )
        if cap_add != policy["cap_add"] or cap_drop != frozenset({"ALL"}):
            raise AdmissionError(f"rendered capability policy differs: {service}")
        if security_opt != _EXPECTED_SECURITY_OPT:
            raise AdmissionError(f"rendered security options differ: {service}")
        if model.get("configs") or model.get("secrets"):
            raise AdmissionError(
                f"rendered protected service adds configs/secrets: {service}"
            )
        volumes = _rendered_volumes(model, service=service)
        tmpfs = _tmpfs_model(model, service=service)
        for _kind, _source, target, _read_only in volumes:
            if _mount_shadows_image_path(service, target):
                raise AdmissionError(
                    f"rendered mount shadows image trust path: {service}"
                )
        for target in tmpfs:
            if _mount_shadows_image_path(service, target):
                raise AdmissionError(
                    f"rendered tmpfs shadows image trust path: {service}"
                )
        volume_policy = {
            target: (kind, read_only) for kind, _source, target, read_only in volumes
        }
        expected_volume_policy = _ALLOWED_VOLUME_TARGETS[service]
        legacy_volume_policy = dict(expected_volume_policy)
        if service == "airflow-scheduler":
            legacy_volume_policy.pop(
                "/opt/airflow/secure/whoscored-scheduled-pointers", None
            )
        if (
            volume_policy != expected_volume_policy
            and volume_policy != legacy_volume_policy
        ):
            raise AdmissionError(f"rendered mount-target policy differs: {service}")
        if tmpfs != _ALLOWED_TMPFS[service]:
            raise AdmissionError(f"rendered tmpfs policy differs: {service}")
        projections[service] = {
            "cap_add": cap_add,
            "cap_drop": cap_drop,
            "command": effective_command,
            "environment": dict(environment),
            "healthcheck": _healthcheck_projection(model, service=service),
            "port_bindings": _EXPECTED_PORT_BINDINGS[service],
            "network_names": tuple(
                _EXPECTED_NETWORK_DEFINITIONS[name]["name"]
                for name in _EXPECTED_NETWORKS[service]
            ),
            "read_only": policy["read_only"],
            "security_opt": security_opt,
            "tmpfs": tmpfs,
            "volumes": volumes,
            "shm_size": _EXPECTED_SHM_SIZE[service],
        }
    if set(selected) != _PROTECTED_SERVICE_SET:
        return projections
    scheduler_environment = projections["airflow-scheduler"]["environment"]
    gateway_environment = projections["whoscored_paid_gateway"]["environment"]
    paid_browser_environment = projections["flaresolverr_whoscored_paid"]["environment"]
    filter_environment = projections["whoscored_proxy_filter"]["environment"]
    if (
        scheduler_environment["WHOSCORED_PAID_GATEWAY_TOKEN"]
        != gateway_environment["WHOSCORED_PAID_GATEWAY_TOKEN"]
        or scheduler_environment["WHOSCORED_PAID_BATCH_ENABLED"]
        != gateway_environment["WHOSCORED_PAID_BATCH_ENABLED"]
        or gateway_environment["WHOSCORED_FLARESOLVERR_GATEWAY_SECRET"]
        != paid_browser_environment["WHOSCORED_FLARESOLVERR_GATEWAY_SECRET"]
        or gateway_environment["WHOSCORED_PROXY_CONTROL_TOKEN"]
        != filter_environment["PROXY_FILTER_CONTROL_TOKEN"]
        or gateway_environment["WHOSCORED_PROXY_APPROVAL_HMAC_SECRET"]
        != filter_environment["WHOSCORED_PROXY_APPROVAL_HMAC_SECRET"]
    ):
        raise AdmissionError("rendered paid-boundary credentials differ")
    paid_secrets = tuple(
        value.encode("utf-8")
        for value in (
            scheduler_environment["WHOSCORED_PAID_GATEWAY_TOKEN"],
            gateway_environment["WHOSCORED_PROXY_APPROVAL_HMAC_SECRET"],
            gateway_environment["WHOSCORED_PROXY_CONTROL_TOKEN"],
            gateway_environment["WHOSCORED_FLARESOLVERR_GATEWAY_SECRET"],
            gateway_environment["WHOSCORED_PAID_ALERT_HMAC_SECRET"],
            filter_environment["WHOSCORED_PROXY_LEDGER_HMAC_SECRET"],
        )
    )
    generic_scheduler_secrets = tuple(
        scheduler_environment[name].encode("utf-8")
        for name in (
            "PROXY_FILTER_CONTROL_TOKEN",
            "SOFASCORE_PROXY_CONTROL_TOKEN",
        )
    )
    if any(
        hmac.compare_digest(left, right)
        for index, left in enumerate(paid_secrets)
        for right in paid_secrets[index + 1 :]
    ) or any(
        hmac.compare_digest(paid, generic)
        for paid in paid_secrets
        for generic in generic_scheduler_secrets
    ):
        raise AdmissionError("rendered paid-boundary secrets are not distinct")
    return projections


DockerRunner = Callable[[Sequence[str]], bytes]

_ROLLOUT_ACCEPTANCE_PROBE = """\
import json
import sys
from datetime import timezone

from airflow.models.dagrun import DagRun
from airflow.models.taskinstance import TaskInstance
from airflow.models.xcom import XCom
from airflow.utils.session import create_session
from airflow.utils.xcom import XCOM_RETURN_KEY
from dags.dag_backup_whoscored_storage import (
    validate_whoscored_backup_recovery_contract,
)
from dags.scripts.whoscored_ops_store import WhoScoredOpsStore
from dags.scripts.whoscored_rollout_acceptance import (
    idempotency_evidence,
    is_countable_scheduled_run,
    mapped_scope_dq_evidence,
    receipts_prefix,
    rollout_acceptance_status,
    run_evidence_sha256,
    scope_plan_sha256,
    terminal_task_states_evidence,
    validated_bootstrap_slo_evidence,
)
from dags.scripts.whoscored_bootstrap import normalize_bootstrap_authority
from scrapers.whoscored.runtime_contract import validate_runtime_contract


def utc_iso(value):
    if value is None or value.tzinfo is None:
        raise RuntimeError("WhoScored accepted DagRun logical date is invalid")
    return value.astimezone(timezone.utc).isoformat().replace("+00:00", "Z")


rollout_id = sys.argv[1]
expected_authority = json.loads(sys.argv[2])
expected_bootstrap = normalize_bootstrap_authority(expected_authority)
runtime = validate_runtime_contract(report_schema_version=3)
runtime_release = {
    "parser_version": runtime["parser_version"],
    "manifest_sha256": runtime["manifest_sha256"],
    "code_tree_sha256": runtime["code_tree_sha256"],
}
backup_recovery = validate_whoscored_backup_recovery_contract(
    full_revalidation=True
)
if backup_recovery.get("runtime_release") != runtime_release:
    raise RuntimeError("WhoScored restore drill release differs from runtime")
ops_store = WhoScoredOpsStore.from_env(optional=False)
if ops_store is None:
    raise RuntimeError("WhoScored operational store is required")
records = list(
    ops_store.iter_content_addressed_json(receipts_prefix(rollout_id))
)
acceptance = rollout_acceptance_status(records, rollout_id=rollout_id)
accepted_release = acceptance.get("release")
if acceptance.get("status") != "accepted":
    raise RuntimeError("WhoScored rollout acceptance is incomplete")
if accepted_release != runtime_release:
    raise RuntimeError("WhoScored accepted rollout release differs from runtime")
terminal_runs = acceptance.get("terminal_runs")
if not isinstance(terminal_runs, list) or len(terminal_runs) != 6:
    raise RuntimeError("WhoScored rollout acceptance lacks six terminal DagRuns")
run_ids = [run.get("run_id") for run in terminal_runs if isinstance(run, dict)]
expected_run_ids = [slot["run_id"] for slot in expected_bootstrap["bootstrap_slots"]]
if (
    len(run_ids) != 6
    or len(run_ids) != len(set(run_ids))
    or run_ids != expected_run_ids
    or acceptance.get("authority") != expected_authority
):
    raise RuntimeError("WhoScored rollout acceptance DagRun identities are invalid")
verified_terminal_runs = []
with create_session() as session:
    dag_runs = (
        session.query(DagRun)
        .filter(
            DagRun.dag_id == "dag_ingest_whoscored",
            DagRun.run_id.in_(run_ids),
        )
        .all()
    )
    if len(dag_runs) != len(run_ids):
        raise RuntimeError("WhoScored accepted DagRun is missing from metadata DB")
    dag_run_by_id = {dag_run.run_id: dag_run for dag_run in dag_runs}
    if len(dag_run_by_id) != len(run_ids):
        raise RuntimeError("WhoScored metadata DB returned duplicate DagRuns")
    latest_scheduled = (
        session.query(DagRun)
        .filter(
            DagRun.dag_id == "dag_ingest_whoscored",
            DagRun.run_id.like("scheduled__%"),
            DagRun.run_type == "scheduled",
            DagRun.external_trigger.is_(False),
        )
        .order_by(DagRun.execution_date.desc(), DagRun.run_id.desc())
        .first()
    )
    if (
        latest_scheduled is None
        or latest_scheduled.run_id != terminal_runs[-1].get("run_id")
        or utc_iso(latest_scheduled.execution_date)
        != terminal_runs[-1].get("logical_date")
        or str(latest_scheduled.state or "").lower().split(".")[-1] != "success"
        or latest_scheduled.end_date is None
        or not is_countable_scheduled_run(
            run_id=latest_scheduled.run_id,
            run_type=latest_scheduled.run_type,
            external_trigger=latest_scheduled.external_trigger,
            conf=latest_scheduled.conf,
        )
    ):
        raise RuntimeError(
            "WhoScored latest scheduler-created DagRun is not accepted"
        )
    latest_scheduled_run = {
        "completed_at": utc_iso(latest_scheduled.end_date),
        "logical_date": utc_iso(latest_scheduled.execution_date),
        "run_id": latest_scheduled.run_id,
        "state": "success",
    }
    for witness in terminal_runs:
        run_id = witness["run_id"]
        dag_run = dag_run_by_id.get(run_id)
        if (
            dag_run is None
            or str(dag_run.state or "").lower().split(".")[-1] != "success"
            or utc_iso(dag_run.execution_date) != witness.get("logical_date")
            or dag_run.end_date is None
            or dag_run.start_date is None
            or dag_run.end_date < dag_run.start_date
            or (dag_run.end_date - dag_run.start_date).total_seconds() > 21600
            or dag_run.end_date < dag_run.execution_date
            or not is_countable_scheduled_run(
                run_id=dag_run.run_id,
                run_type=dag_run.run_type,
                external_trigger=dag_run.external_trigger,
                conf=dag_run.conf,
            )
        ):
            raise RuntimeError("WhoScored accepted DagRun is no longer terminal green")
        task_rows = (
            session.query(
                TaskInstance.task_id,
                TaskInstance.map_index,
                TaskInstance.state,
            )
            .filter(
                TaskInstance.dag_id == "dag_ingest_whoscored",
                TaskInstance.run_id == run_id,
                TaskInstance.task_id != "seal_rollout_acceptance_and_pause",
            )
            .all()
        )
        task_state_values = [
            {
                "task_id": row.task_id,
                "map_index": row.map_index,
                "state": str(row.state or "").lower().split(".")[-1],
            }
            for row in task_rows
        ]
        task_states = terminal_task_states_evidence(task_state_values)
        if task_states != witness.get("task_states"):
            raise RuntimeError("WhoScored accepted TaskInstance states have drifted")
        xcom_rows = (
            session.query(XCom.map_index, XCom.value)
            .filter(
                XCom.dag_id == "dag_ingest_whoscored",
                XCom.run_id == run_id,
                XCom.task_id == "validate_active_scope",
                XCom.key == XCOM_RETURN_KEY,
            )
            .order_by(XCom.map_index.asc())
            .all()
        )
        expected_scope_dq = witness.get("scope_dq")
        expected_count = (
            expected_scope_dq.get("count")
            if isinstance(expected_scope_dq, dict)
            else None
        )
        if (
            isinstance(expected_count, bool)
            or not isinstance(expected_count, int)
            or [row.map_index for row in xcom_rows] != list(range(expected_count))
        ):
            raise RuntimeError("WhoScored accepted mapped DQ XCom set is not exact")
        scope_dq_values = [XCom.deserialize_value(row) for row in xcom_rows]
        scope_dq = mapped_scope_dq_evidence(scope_dq_values)
        if scope_dq != expected_scope_dq:
            raise RuntimeError("WhoScored accepted mapped DQ evidence has drifted")
        singleton_task_ids = {
            "alert_preflight": "validate_whoscored_paid_alert_delivery",
            "catalog_dq": "validate_whoscored_catalog",
            "daily_slo": "validate_whoscored_daily_slo",
            "profile_dq": "validate_profile_refresh",
            "runtime_preflight": "validate_whoscored_runtime",
            "scope_plan": "freeze_daily_scope_plan",
            "traffic_dq": "report_whoscored_traffic",
        }
        singleton_rows = (
            session.query(XCom)
            .filter(
                XCom.dag_id == "dag_ingest_whoscored",
                XCom.run_id == run_id,
                XCom.task_id.in_(tuple(singleton_task_ids.values())),
                XCom.key == XCOM_RETURN_KEY,
            )
            .all()
        )
        by_task_id = {}
        for row in singleton_rows:
            if row.map_index != -1 or row.task_id in by_task_id:
                raise RuntimeError(
                    "WhoScored accepted singleton XCom set is not exact"
                )
            by_task_id[row.task_id] = XCom.deserialize_value(row)
        if set(by_task_id) != set(singleton_task_ids.values()):
            raise RuntimeError("WhoScored accepted singleton XCom set is not exact")
        singleton_values = {
            name: by_task_id[task_id]
            for name, task_id in singleton_task_ids.items()
        }
        validated_bootstrap_slo_evidence(
            singleton_values["daily_slo"],
            scope=singleton_values["scope_plan"],
            release=runtime_release,
        )
        observed_idempotency = idempotency_evidence(
            scope_dq=scope_dq_values,
            profile_dq=singleton_values["profile_dq"],
        )
        if observed_idempotency != witness.get("idempotency"):
            raise RuntimeError("WhoScored accepted idempotency evidence has drifted")
        observed_scope_plan_sha256 = scope_plan_sha256(
            singleton_values["scope_plan"]
        )
        if observed_scope_plan_sha256 != witness.get("scope_plan_sha256"):
            raise RuntimeError("WhoScored accepted scope-plan XCom has drifted")
        observed_evidence_sha256 = run_evidence_sha256(
            scope_plan=singleton_values["scope_plan"],
            runtime_preflight=singleton_values["runtime_preflight"],
            catalog_dq=singleton_values["catalog_dq"],
            profile_dq=singleton_values["profile_dq"],
            traffic_dq=singleton_values["traffic_dq"],
            daily_slo=singleton_values["daily_slo"],
            alert_preflight=singleton_values["alert_preflight"],
            scope_dq=scope_dq_values,
            terminal_task_states=task_state_values,
        )
        if observed_evidence_sha256 != witness.get("evidence_sha256"):
            raise RuntimeError("WhoScored accepted green XCom evidence has drifted")
        verified_terminal_runs.append(
            {
                "completed_at": utc_iso(dag_run.end_date),
                "duration_seconds": int(
                    (dag_run.end_date - dag_run.start_date).total_seconds()
                ),
                "evidence_sha256": observed_evidence_sha256,
                "idempotency": observed_idempotency,
                "logical_date": witness["logical_date"],
                "run_id": run_id,
                "scope_dq": scope_dq,
                "scope_plan_sha256": observed_scope_plan_sha256,
                "task_states": task_states,
            }
        )
report = {
    "accepted_release": accepted_release,
    "accepted_waves": acceptance.get("accepted_waves"),
    "authority": acceptance.get("authority"),
    "authority_binding": "explicit-rollout-id",
    "backup_recovery": backup_recovery,
    "catalog": acceptance.get("catalog"),
    "final_wave_receipt_sha256": acceptance.get("final_wave_receipt_sha256"),
    "latest_scheduled_run": latest_scheduled_run,
    "missing_waves": acceptance.get("missing_waves"),
    "rollout_id": rollout_id,
    "runtime_release": runtime_release,
    "schema_version": 1,
    "status": "accepted",
    "terminal_runs": verified_terminal_runs,
}
print(json.dumps(report, ensure_ascii=False, separators=(",", ":"), sort_keys=True))
"""

_ROLLOUT_ISSUANCE_PROBE = """\
import json
import sys

from dags.scripts.whoscored_production_issuance import (
    verify_daily_issuance_rollout,
)


report = verify_daily_issuance_rollout(
    rollout_id=sys.argv[1],
    expected_scope_authority=json.loads(sys.argv[2]),
)
print(json.dumps(report, ensure_ascii=False, separators=(",", ":"), sort_keys=True))
"""


def _assert_clean_control_environment() -> None:
    supplied = sorted(
        name
        for name in os.environ
        if name in _FORBIDDEN_CONTROL_ENV or name.startswith(("LD_", "DYLD_"))
    )
    if supplied:
        raise AdmissionError(
            "host control environment must be unset: " + ", ".join(supplied)
        )


def _trusted_docker_environment() -> dict[str, str]:
    _assert_clean_control_environment()
    try:
        binary = _DOCKER_CLI.lstat()
        socket = _DOCKER_SOCKET.lstat()
    except OSError as exc:
        raise AdmissionError(
            "trusted Docker CLI or daemon socket is unavailable"
        ) from exc
    if (
        not stat.S_ISREG(binary.st_mode)
        or binary.st_uid != 0
        or binary.st_mode & 0o022
        or not binary.st_mode & 0o111
    ):
        raise AdmissionError("trusted Docker CLI identity is invalid")
    if (
        not stat.S_ISSOCK(socket.st_mode)
        or socket.st_uid != 0
        or socket.st_mode & 0o002
    ):
        raise AdmissionError("trusted Docker daemon socket identity is invalid")
    return {
        "DOCKER_HOST": "unix:///run/docker.sock",
        "HOME": "/nonexistent",
        "LANG": "C.UTF-8",
        "LC_ALL": "C.UTF-8",
        "PATH": "/usr/bin:/bin",
    }


def _run_docker_with_timeout(
    arguments: Sequence[str], *, timeout_seconds: int
) -> bytes:
    environment = _trusted_docker_environment()
    try:
        result = subprocess.run(
            (str(_DOCKER_CLI), *arguments),
            stdin=subprocess.DEVNULL,
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE,
            check=False,
            env=environment,
            timeout=timeout_seconds,
        )
    except (OSError, subprocess.TimeoutExpired) as exc:
        raise AdmissionError(
            f"Docker inspection failed: {' '.join(arguments)}"
        ) from exc
    if result.returncode != 0:
        message = result.stderr.decode("utf-8", errors="replace").strip()
        raise AdmissionError(
            f"Docker inspection failed ({result.returncode}): {message or arguments[0]}"
        )
    if len(result.stdout) > MAX_JSON_BYTES:
        raise AdmissionError("Docker inspection output is unreasonably large")
    return result.stdout


def _run_docker(arguments: Sequence[str]) -> bytes:
    return _run_docker_with_timeout(arguments, timeout_seconds=30)


def _run_rollout_acceptance_docker(arguments: Sequence[str]) -> bytes:
    """Allow bounded full backup reads without weakening normal inspection."""

    return _run_docker_with_timeout(
        arguments,
        timeout_seconds=ROLLOUT_ACCEPTANCE_TIMEOUT_SECONDS,
    )


def _rollout_acceptance_now() -> datetime:
    return datetime.now(timezone.utc)


def _validate_idempotency_witness(value: object, *, scope_count: int) -> None:
    scope_fields = {
        "scope_count",
        "exact_manifest_pair_count",
        "duplicate_counter_count",
        "physical_current_pair_count",
        "zero_mismatch_counter_count",
        "violation_count",
        "evidence_sha256",
    }
    profile_fields = scope_fields - {"scope_count"}
    if (
        not isinstance(value, dict)
        or set(value) != {"schema_version", "status", "scope", "profile"}
        or value.get("schema_version") != 1
        or value.get("status") != "green"
        or not isinstance(value.get("scope"), dict)
        or not isinstance(value.get("profile"), dict)
        or set(value["scope"]) != scope_fields
        or set(value["profile"]) != profile_fields
    ):
        raise AdmissionError("WhoScored idempotency witness schema is invalid")
    expected_scope = {
        "scope_count": scope_count,
        "exact_manifest_pair_count": scope_count * 5,
        "duplicate_counter_count": scope_count * 4,
        "physical_current_pair_count": scope_count * 11,
        "zero_mismatch_counter_count": scope_count * 6,
        "violation_count": 0,
    }
    expected_profile = {
        "exact_manifest_pair_count": 2,
        "duplicate_counter_count": 1,
        "physical_current_pair_count": 2,
        "zero_mismatch_counter_count": 5,
        "violation_count": 0,
    }
    if any(
        value["scope"].get(field) != expected
        for field, expected in expected_scope.items()
    ) or any(
        value["profile"].get(field) != expected
        for field, expected in expected_profile.items()
    ):
        raise AdmissionError("WhoScored idempotency witness is not exact and green")
    for witness in (value["scope"], value["profile"]):
        digest = witness.get("evidence_sha256")
        if not isinstance(digest, str) or _DIGEST.fullmatch(digest) is None:
            raise AdmissionError("WhoScored idempotency witness digest is invalid")


def _validate_verified_rollout_runs(
    value: object,
    *,
    catalog_active_scope_count: int,
    expected_scope_counts: Sequence[int] | None = None,
    expected_bootstrap_slots: Sequence[Mapping[str, Any]] | None = None,
    require_fresh_completion: bool = True,
) -> None:
    """Validate the compact metadata-DB witnesses returned by the fixed probe."""

    final_acceptance = expected_scope_counts is None
    if expected_scope_counts is None:
        expected_scope_counts = (
            min(20, catalog_active_scope_count),
            min(20, catalog_active_scope_count),
            min(70, catalog_active_scope_count),
            min(70, catalog_active_scope_count),
            catalog_active_scope_count,
            catalog_active_scope_count,
        )
    expected_scope_counts = tuple(expected_scope_counts)
    if not isinstance(value, list) or len(value) != len(expected_scope_counts):
        raise AdmissionError(
            "WhoScored rollout acceptance lacks six verified terminal DagRuns"
            if final_acceptance
            else "WhoScored rollout acceptance lacks its verified terminal DagRuns"
        )
    if not value:
        return
    seen_run_ids: set[str] = set()
    logical_dates: list[datetime] = []
    completed_dates: list[datetime] = []
    for position, item in enumerate(value):
        expected_fields = {
            "completed_at",
            "evidence_sha256",
            "idempotency",
            "logical_date",
            "run_id",
            "scope_dq",
            "scope_plan_sha256",
            "task_states",
        }
        if expected_bootstrap_slots is not None:
            expected_fields.add("duration_seconds")
        if not isinstance(item, dict) or set(item) != expected_fields:
            raise AdmissionError(
                "WhoScored verified terminal DagRun witness is invalid"
            )
        run_id = item.get("run_id")
        logical_raw = item.get("logical_date")
        completed_raw = item.get("completed_at")
        if (
            not isinstance(run_id, str)
            or not run_id.startswith("scheduled__")
            or len(run_id) > 250
            or run_id in seen_run_ids
            or not isinstance(logical_raw, str)
            or not isinstance(completed_raw, str)
        ):
            raise AdmissionError(
                "WhoScored verified terminal DagRun identity is invalid"
            )
        if expected_bootstrap_slots is not None:
            if len(expected_bootstrap_slots) != len(expected_scope_counts):
                raise AdmissionError(
                    "WhoScored signed bootstrap slot set is incomplete"
                )
            expected_slot = expected_bootstrap_slots[position]
            duration_seconds = item.get("duration_seconds")
            if (
                expected_slot.get("run_id") != run_id
                or expected_slot.get("logical_date") != logical_raw
                or isinstance(duration_seconds, bool)
                or not isinstance(duration_seconds, int)
                or not 0 <= duration_seconds <= 21_600
            ):
                raise AdmissionError(
                    "WhoScored verified DagRun differs from signed bootstrap SLO"
                )
        try:
            logical_date = datetime.fromisoformat(logical_raw.replace("Z", "+00:00"))
            completed_at = datetime.fromisoformat(completed_raw.replace("Z", "+00:00"))
            run_logical_date = datetime.fromisoformat(
                run_id.removeprefix("scheduled__").replace("Z", "+00:00")
            )
        except ValueError as exc:
            raise AdmissionError(
                "WhoScored verified terminal DagRun logical date is invalid"
            ) from exc
        canonical_logical = (
            logical_date.astimezone(timezone.utc).isoformat().replace("+00:00", "Z")
            if logical_date.tzinfo is not None
            else ""
        )
        canonical_completed = (
            completed_at.astimezone(timezone.utc).isoformat().replace("+00:00", "Z")
            if completed_at.tzinfo is not None
            else ""
        )
        if (
            canonical_logical != logical_raw
            or canonical_completed != completed_raw
            or completed_at < logical_date
            or run_logical_date.tzinfo is None
            or run_logical_date.astimezone(timezone.utc) != logical_date
            or (
                logical_date.hour,
                logical_date.minute,
                logical_date.second,
                logical_date.microsecond,
            )
            != (10, 0, 0, 0)
        ):
            raise AdmissionError(
                "WhoScored verified terminal DagRun logical date is invalid"
            )
        seen_run_ids.add(run_id)
        logical_dates.append(logical_date)
        completed_dates.append(completed_at)
        scope_dq = item.get("scope_dq")
        if (
            not isinstance(scope_dq, dict)
            or set(scope_dq) != {"count", "sha256", "scopes_sha256"}
            or isinstance(scope_dq.get("count"), bool)
            or scope_dq.get("count") != expected_scope_counts[position]
            or not isinstance(scope_dq.get("sha256"), str)
            or _DIGEST.fullmatch(scope_dq["sha256"]) is None
            or not isinstance(scope_dq.get("scopes_sha256"), str)
            or _DIGEST.fullmatch(scope_dq["scopes_sha256"]) is None
        ):
            raise AdmissionError(
                "WhoScored verified mapped scope DQ witness is invalid"
            )
        _validate_idempotency_witness(
            item.get("idempotency"), scope_count=scope_dq["count"]
        )
        task_states = item.get("task_states")
        if (
            not isinstance(task_states, dict)
            or set(task_states) != {"count", "sha256"}
            or isinstance(task_states.get("count"), bool)
            or not isinstance(task_states.get("count"), int)
            or task_states["count"] < scope_dq["count"] + 1
            or not isinstance(task_states.get("sha256"), str)
            or _DIGEST.fullmatch(task_states["sha256"]) is None
        ):
            raise AdmissionError(
                "WhoScored verified TaskInstance-state witness is invalid"
            )
        if any(
            not isinstance(item.get(field), str)
            or _DIGEST.fullmatch(item[field]) is None
            for field in ("scope_plan_sha256", "evidence_sha256")
        ):
            raise AdmissionError("WhoScored verified persisted-XCom witness is invalid")
    if (
        logical_dates != sorted(logical_dates)
        or len(set(logical_dates)) != len(expected_scope_counts)
        or any(
            logical_dates[position + 1] - logical_dates[position] != timedelta(days=1)
            for position in range(0, len(expected_scope_counts), 2)
        )
    ):
        raise AdmissionError(
            "WhoScored verified terminal DagRuns lack consecutive within-wave pairs"
        )
    if completed_dates != sorted(completed_dates):
        raise AdmissionError(
            "WhoScored verified terminal DagRun completions are not chronological"
        )
    age = _rollout_acceptance_now() - completed_dates[-1].astimezone(timezone.utc)
    if require_fresh_completion and (
        age < -timedelta(minutes=5) or age > MAX_ROLLOUT_ACCEPTANCE_AGE
    ):
        raise AdmissionError(
            "WhoScored final accepted scheduled DagRun is stale or future-dated"
        )


def _validate_current_rollout_projection(
    value: Mapping[str, Any], *, rollout_id: str
) -> dict[str, Any]:
    if (
        frozenset(value) != _ROLLOUT_AUTHORITY_REPORT_FIELDS
        or value.get("authority_binding") != "current-signed-rollout"
        or not isinstance(value.get("cohort_id"), str)
        or _ROLLOUT_ID.fullmatch(value["cohort_id"]) is None
    ):
        raise AdmissionError("current signed rollout authority is invalid")
    authority = value.get("authority")
    catalog_count = value.get("catalog_active_scope_count")
    if (
        not isinstance(authority, Mapping)
        or frozenset(authority) != _ROLLOUT_ACCEPTANCE_AUTHORITY_FIELDS
        or authority.get("rollout_id") != rollout_id
        or authority.get("wave_id") != "wave-all"
        or authority.get("max_scopes") != 2_000
        or authority.get("require_full_active") is not True
        or isinstance(catalog_count, bool)
        or not isinstance(catalog_count, int)
        or not 1 <= catalog_count <= 2_000
        or value.get("rollout_manifest_sha256") != authority.get("cohort_sha256")
    ):
        raise AdmissionError("current signed rollout authority is invalid")
    for field in (
        "cohort_sha256",
        "ranked_scope_ids_sha256",
        "runtime_sha256",
        "classifier_sha256",
        "promotion_acceptance_sha256",
        "promotion_terminal_receipt_sha256",
    ):
        if (
            not isinstance(authority.get(field), str)
            or _DIGEST.fullmatch(authority[field]) is None
        ):
            raise AdmissionError("current signed rollout authority is invalid")
    _normalized_bootstrap_authority(authority)
    for field in (
        "catalog_active_scopes_sha256",
        "charter_sha256",
        "rollout_manifest_sha256",
    ):
        if (
            not isinstance(value.get(field), str)
            or _DIGEST.fullmatch(value[field]) is None
        ):
            raise AdmissionError("current signed rollout authority is invalid")
    return dict(value)


def _validate_current_issuance_projection(
    value: Mapping[str, Any], *, rollout_id: str
) -> dict[str, Any]:
    """Validate any exact active wave without weakening final production GO."""

    if (
        frozenset(value) != _ROLLOUT_AUTHORITY_REPORT_FIELDS
        or value.get("authority_binding") != "current-signed-rollout"
        or not isinstance(value.get("cohort_id"), str)
        or _ROLLOUT_ID.fullmatch(value["cohort_id"]) is None
    ):
        raise AdmissionError("current signed issuance authority is invalid")
    authority = value.get("authority")
    catalog_count = value.get("catalog_active_scope_count")
    if not isinstance(authority, Mapping):
        raise AdmissionError("current signed issuance authority is invalid")
    wave_id = authority.get("wave_id")
    wave_contract = _ROLLOUT_WAVE_CONTRACTS.get(str(wave_id))
    if (
        frozenset(authority) != _ROLLOUT_ACCEPTANCE_AUTHORITY_FIELDS
        or authority.get("rollout_id") != rollout_id
        or wave_contract is None
        or (authority.get("max_scopes"), authority.get("require_full_active"))
        != wave_contract
        or isinstance(catalog_count, bool)
        or not isinstance(catalog_count, int)
        or not 1 <= catalog_count <= 2_000
        or value.get("rollout_manifest_sha256") != authority.get("cohort_sha256")
    ):
        raise AdmissionError("current signed issuance authority is invalid")
    for field in (
        "cohort_sha256",
        "ranked_scope_ids_sha256",
        "runtime_sha256",
        "classifier_sha256",
        "promotion_acceptance_sha256",
        "promotion_terminal_receipt_sha256",
    ):
        if (
            not isinstance(authority.get(field), str)
            or _DIGEST.fullmatch(authority[field]) is None
        ):
            raise AdmissionError("current signed issuance authority is invalid")
    _normalized_bootstrap_authority(authority)
    for field in (
        "catalog_active_scopes_sha256",
        "charter_sha256",
        "rollout_manifest_sha256",
    ):
        if (
            not isinstance(value.get(field), str)
            or _DIGEST.fullmatch(value[field]) is None
        ):
            raise AdmissionError("current signed issuance authority is invalid")
    promotion_proofs = (
        authority["promotion_acceptance_sha256"],
        authority["promotion_terminal_receipt_sha256"],
    )
    if (
        wave_id == "wave-20"
        and promotion_proofs
        != (
            _ROLLOUT_GENESIS_PROOF_SHA256,
            _ROLLOUT_GENESIS_PROOF_SHA256,
        )
    ) or (
        wave_id != "wave-20"
        and any(proof == _ROLLOUT_GENESIS_PROOF_SHA256 for proof in promotion_proofs)
    ):
        raise AdmissionError("current signed issuance promotion proof is invalid")
    return dict(value)


def _validate_live_backup_revalidation(
    value: object,
    *,
    source_uris: Sequence[str],
    receipt_key: str,
    receipt_sha256: str,
) -> None:
    fields = {
        "capability",
        "checked_at",
        "expected_retained_objects",
        "inventories",
        "receipt_key",
        "receipt_sha256",
        "status",
    }
    if (
        not isinstance(value, dict)
        or set(value) != fields
        or value.get("status") != "passed"
        or value.get("receipt_key") != receipt_key
        or value.get("receipt_sha256") != receipt_sha256
    ):
        raise AdmissionError("WhoScored live backup revalidation is invalid")
    checked_raw = value.get("checked_at")
    if not isinstance(checked_raw, str):
        raise AdmissionError("WhoScored live backup revalidation time is invalid")
    try:
        checked_at = datetime.strptime(checked_raw, "%Y-%m-%dT%H:%M:%SZ").replace(
            tzinfo=timezone.utc
        )
    except ValueError as exc:
        raise AdmissionError(
            "WhoScored live backup revalidation time is invalid"
        ) from exc
    live_age = _rollout_acceptance_now() - checked_at
    if live_age < -timedelta(minutes=5) or live_age > timedelta(minutes=5):
        raise AdmissionError("WhoScored live backup revalidation is stale")
    inventories = value.get("inventories")
    inventory_fields = {
        "checked_bytes",
        "checked_content_objects",
        "expected_content_bytes",
        "expected_content_objects",
        "inventory_key",
        "inventory_sha256",
        "marker_valid",
        "object_count",
        "objects_sha256",
        "source_uri",
        "total_bytes",
    }
    if (
        not isinstance(inventories, list)
        or len(inventories) != 2
        or [item.get("source_uri") for item in inventories if isinstance(item, dict)]
        != sorted(source_uris)
    ):
        raise AdmissionError("WhoScored live backup inventories are invalid")
    for item in inventories:
        if (
            not isinstance(item, dict)
            or set(item) != inventory_fields
            or item.get("marker_valid") is not True
            or not isinstance(item.get("inventory_key"), str)
            or re.fullmatch(
                r"backup-inventories/[0-9]{8}T[0-9]{12}Z-[0-9a-f]{16}-[0-9a-f]{64}\.json",
                item["inventory_key"],
            )
            is None
            or any(
                not isinstance(item.get(field), str)
                or _DIGEST.fullmatch(item[field]) is None
                for field in ("inventory_sha256", "objects_sha256")
            )
        ):
            raise AdmissionError("WhoScored live backup inventory is invalid")
        for field in (
            "checked_bytes",
            "checked_content_objects",
            "expected_content_bytes",
            "expected_content_objects",
            "object_count",
            "total_bytes",
        ):
            count = item.get(field)
            if isinstance(count, bool) or not isinstance(count, int) or count < 1:
                raise AdmissionError("WhoScored live backup inventory is invalid")
        if (
            item["checked_content_objects"] != item["expected_content_objects"]
            or item["checked_bytes"] != item["expected_content_bytes"]
            or item["expected_content_objects"] > item["object_count"]
            or item["expected_content_bytes"] > item["total_bytes"]
        ):
            raise AdmissionError("WhoScored live backup inventory is invalid")
    capability = value.get("capability")
    expected_retained_objects = value.get("expected_retained_objects")
    capability_fields = {
        "bucket",
        "checked_object_count",
        "default_retention_days",
        "default_retention_mode",
        "earliest_retain_until",
        "minimum_remaining_hours",
        "object_lock_enabled",
        "status",
        "versioning_status",
    }
    if (
        not isinstance(capability, dict)
        or set(capability) != capability_fields
        or capability.get("status") != "passed"
        or capability.get("versioning_status") != "Enabled"
        or capability.get("object_lock_enabled") != "Enabled"
        or capability.get("default_retention_mode") != "COMPLIANCE"
        or capability.get("minimum_remaining_hours") != 24
        or isinstance(expected_retained_objects, bool)
        or not isinstance(expected_retained_objects, int)
        or expected_retained_objects < 3
        or capability.get("checked_object_count") != expected_retained_objects
        or not isinstance(capability.get("bucket"), str)
        or re.fullmatch(r"[a-z0-9][a-z0-9.-]{1,61}[a-z0-9]", capability["bucket"])
        is None
    ):
        raise AdmissionError("WhoScored live Object Lock capability is invalid")
    for field in ("checked_object_count", "default_retention_days"):
        count = capability.get(field)
        if isinstance(count, bool) or not isinstance(count, int) or count < 1:
            raise AdmissionError("WhoScored live Object Lock capability is invalid")
    if capability["checked_object_count"] < 3:
        raise AdmissionError("WhoScored live Object Lock object set is incomplete")
    retain_raw = capability.get("earliest_retain_until")
    try:
        retain_until = datetime.strptime(str(retain_raw), "%Y-%m-%dT%H:%M:%SZ").replace(
            tzinfo=timezone.utc
        )
    except ValueError as exc:
        raise AdmissionError(
            "WhoScored live Object Lock retention time is invalid"
        ) from exc
    if retain_until < checked_at + timedelta(hours=24):
        raise AdmissionError("WhoScored live Object Lock retention is too short")


def _validated_rollout_runtime_release(value: object) -> dict[str, str]:
    fields = {"parser_version", "manifest_sha256", "code_tree_sha256"}
    if (
        not isinstance(value, dict)
        or set(value) != fields
        or value.get("parser_version") != "whoscored-parser-v8"
        or any(
            not isinstance(value.get(field), str)
            or _DIGEST.fullmatch(value[field]) is None
            for field in ("manifest_sha256", "code_tree_sha256")
        )
    ):
        raise AdmissionError("WhoScored admitted runtime release is invalid")
    return dict(value)


def _validate_backup_recovery_report(
    value: object, *, runtime_release: Mapping[str, str]
) -> None:
    recovery_fields = {
        "status",
        "rpo_hours",
        "rto_hours",
        "duration_seconds",
        "evidence_age_seconds",
        "runtime_release",
        "source_uris",
        "off_host_receipt_key",
        "off_host_receipt_sha256",
        "live_backup",
    }
    if (
        not isinstance(value, dict)
        or set(value) != recovery_fields
        or value.get("status") != "passed"
        or value.get("rpo_hours") != 24
        or value.get("rto_hours") != 24
        or value.get("runtime_release") != runtime_release
    ):
        raise AdmissionError("WhoScored raw+ops recovery evidence is not accepted")
    for field in ("duration_seconds", "evidence_age_seconds"):
        item = value.get(field)
        minimum = 1 if field == "duration_seconds" else 0
        if (
            isinstance(item, bool)
            or not isinstance(item, int)
            or not minimum <= item <= 24 * 3600
        ):
            raise AdmissionError("WhoScored raw+ops recovery timing is invalid")
    source_uris = value.get("source_uris")
    receipt_key = value.get("off_host_receipt_key")
    receipt_sha256 = value.get("off_host_receipt_sha256")
    receipt_match = (
        re.fullmatch(
            r"restore-drill-receipts/v2/[0-9]{8}T[0-9]{6}Z-([0-9a-f]{64})\.json",
            receipt_key,
        )
        if isinstance(receipt_key, str)
        else None
    )
    if (
        receipt_match is None
        or not isinstance(receipt_sha256, str)
        or _DIGEST.fullmatch(receipt_sha256) is None
        or receipt_match.group(1) != receipt_sha256
    ):
        raise AdmissionError("WhoScored off-host restore receipt is invalid")
    if (
        not isinstance(source_uris, list)
        or any(not isinstance(uri, str) for uri in source_uris)
        or source_uris != sorted(source_uris)
        or len(source_uris) != 2
        or len(set(source_uris)) != 2
    ):
        raise AdmissionError("WhoScored raw+ops recovery sources are invalid")
    _validate_live_backup_revalidation(
        value.get("live_backup"),
        source_uris=source_uris,
        receipt_key=receipt_key,
        receipt_sha256=receipt_sha256,
    )
    raw_sources = [uri for uri in source_uris if uri.endswith("/raw/whoscored")]
    ops_sources = [uri for uri in source_uris if uri.endswith("/ops/whoscored")]
    if len(raw_sources) != 1 or len(ops_sources) != 1:
        raise AdmissionError("WhoScored raw+ops recovery sources are invalid")
    raw_bucket = raw_sources[0].removeprefix("s3://").split("/", 1)[0]
    _validate_whoscored_store_uris(
        {
            "ICEBERG_WAREHOUSE": raw_bucket,
            "WHOSCORED_RAW_STORE_URI": raw_sources[0],
            "WHOSCORED_OPS_STORE_URI": ops_sources[0],
        }
    )


def verify_rollout_acceptance(
    rollout_id: str,
    *,
    rollout_authority: Mapping[str, Any],
    scheduler_container_id: str,
    runner: DockerRunner = _run_rollout_acceptance_docker,
) -> dict[str, Any]:
    """Replay the active signed rollout ledger inside the admitted scheduler."""

    if _ROLLOUT_ID.fullmatch(rollout_id) is None:
        raise AdmissionError("WhoScored rollout id is invalid")
    if _CONTAINER_ID.fullmatch(scheduler_container_id) is None:
        raise AdmissionError("admitted scheduler container id is invalid")
    rollout_authority = _validate_current_rollout_projection(
        rollout_authority, rollout_id=rollout_id
    )
    raw = runner(
        (
            "exec",
            "--workdir=/opt/airflow",
            scheduler_container_id,
            "/usr/local/bin/whoscored-production-python",
            "-c",
            _ROLLOUT_ACCEPTANCE_PROBE,
            rollout_id,
            _authority_canonical_bytes(rollout_authority["authority"]).decode("utf-8"),
        )
    )
    if not raw or len(raw) > 64 * 1024:
        raise AdmissionError("WhoScored rollout acceptance output is invalid")
    try:
        report = json.loads(raw.decode("utf-8"), object_pairs_hook=_unique_object)
    except (_DuplicateKey, UnicodeDecodeError, json.JSONDecodeError) as exc:
        raise AdmissionError(
            "WhoScored rollout acceptance output is not strict JSON"
        ) from exc
    expected_fields = {
        "accepted_release",
        "accepted_waves",
        "authority",
        "authority_binding",
        "backup_recovery",
        "catalog",
        "final_wave_receipt_sha256",
        "latest_scheduled_run",
        "missing_waves",
        "rollout_id",
        "runtime_release",
        "schema_version",
        "status",
        "terminal_runs",
    }
    if (
        not isinstance(report, dict)
        or set(report) != expected_fields
        or raw != _canonical_bytes(report)
        or report.get("schema_version") != 1
        or report.get("status") != "accepted"
        or report.get("rollout_id") != rollout_id
        or report.get("accepted_waves") != ["wave-20", "wave-70", "wave-all"]
        or report.get("missing_waves") != []
        or report.get("authority_binding") != "explicit-rollout-id"
    ):
        raise AdmissionError("WhoScored rollout acceptance output is not accepted")
    accepted_release = report.get("accepted_release")
    runtime_release = report.get("runtime_release")
    release_fields = {"parser_version", "manifest_sha256", "code_tree_sha256"}
    if (
        not isinstance(accepted_release, dict)
        or set(accepted_release) != release_fields
        or accepted_release != runtime_release
        or accepted_release.get("parser_version") != "whoscored-parser-v8"
        or any(
            not isinstance(accepted_release.get(field), str)
            or _DIGEST.fullmatch(accepted_release[field]) is None
            for field in ("manifest_sha256", "code_tree_sha256")
        )
    ):
        raise AdmissionError(
            "WhoScored accepted rollout release differs from admitted runtime"
        )
    backup_recovery = report.get("backup_recovery")
    recovery_fields = {
        "status",
        "rpo_hours",
        "rto_hours",
        "duration_seconds",
        "evidence_age_seconds",
        "runtime_release",
        "source_uris",
        "off_host_receipt_key",
        "off_host_receipt_sha256",
        "live_backup",
    }
    if (
        not isinstance(backup_recovery, dict)
        or set(backup_recovery) != recovery_fields
        or backup_recovery.get("status") != "passed"
        or backup_recovery.get("rpo_hours") != 24
        or backup_recovery.get("rto_hours") != 24
        or backup_recovery.get("runtime_release") != runtime_release
    ):
        raise AdmissionError("WhoScored raw+ops recovery evidence is not accepted")
    for field in ("duration_seconds", "evidence_age_seconds"):
        item = backup_recovery.get(field)
        minimum = 1 if field == "duration_seconds" else 0
        if (
            isinstance(item, bool)
            or not isinstance(item, int)
            or not minimum <= item <= 24 * 3600
        ):
            raise AdmissionError("WhoScored raw+ops recovery timing is invalid")
    source_uris = backup_recovery.get("source_uris")
    receipt_key = backup_recovery.get("off_host_receipt_key")
    receipt_sha256 = backup_recovery.get("off_host_receipt_sha256")
    receipt_match = (
        re.fullmatch(
            r"restore-drill-receipts/v2/[0-9]{8}T[0-9]{6}Z-([0-9a-f]{64})\.json",
            receipt_key,
        )
        if isinstance(receipt_key, str)
        else None
    )
    if (
        receipt_match is None
        or not isinstance(receipt_sha256, str)
        or _DIGEST.fullmatch(receipt_sha256) is None
        or receipt_match.group(1) != receipt_sha256
    ):
        raise AdmissionError("WhoScored off-host restore receipt is invalid")
    if (
        not isinstance(source_uris, list)
        or any(not isinstance(uri, str) for uri in source_uris)
        or source_uris != sorted(source_uris)
        or len(source_uris) != 2
        or len(set(source_uris)) != 2
    ):
        raise AdmissionError("WhoScored raw+ops recovery sources are invalid")
    _validate_live_backup_revalidation(
        backup_recovery.get("live_backup"),
        source_uris=source_uris,
        receipt_key=receipt_key,
        receipt_sha256=receipt_sha256,
    )
    raw_sources = [uri for uri in source_uris if uri.endswith("/raw/whoscored")]
    ops_sources = [uri for uri in source_uris if uri.endswith("/ops/whoscored")]
    if len(raw_sources) != 1 or len(ops_sources) != 1:
        raise AdmissionError("WhoScored raw+ops recovery sources are invalid")
    raw_bucket = raw_sources[0].removeprefix("s3://").split("/", 1)[0]
    _validate_whoscored_store_uris(
        {
            "ICEBERG_WAREHOUSE": raw_bucket,
            "WHOSCORED_RAW_STORE_URI": raw_sources[0],
            "WHOSCORED_OPS_STORE_URI": ops_sources[0],
        }
    )
    catalog = report.get("catalog")
    if (
        not isinstance(catalog, dict)
        or set(catalog) != {"active_scope_count", "active_scopes_sha256"}
        or isinstance(catalog.get("active_scope_count"), bool)
        or not isinstance(catalog.get("active_scope_count"), int)
        or catalog["active_scope_count"] < 1
        or not isinstance(catalog.get("active_scopes_sha256"), str)
        or _DIGEST.fullmatch(catalog["active_scopes_sha256"]) is None
        or not isinstance(report.get("final_wave_receipt_sha256"), str)
        or _DIGEST.fullmatch(report["final_wave_receipt_sha256"]) is None
    ):
        raise AdmissionError("WhoScored accepted rollout catalog evidence is invalid")
    expected_authority = rollout_authority.get("authority")
    if (
        not isinstance(expected_authority, Mapping)
        or frozenset(expected_authority) != _ROLLOUT_ACCEPTANCE_AUTHORITY_FIELDS
        or expected_authority.get("rollout_id") != rollout_id
        or report.get("authority") != dict(expected_authority)
        or catalog["active_scope_count"]
        != rollout_authority.get("catalog_active_scope_count")
        or catalog["active_scopes_sha256"]
        != rollout_authority.get("catalog_active_scopes_sha256")
        or accepted_release.get("code_tree_sha256")
        != expected_authority.get("runtime_sha256")
    ):
        raise AdmissionError(
            "WhoScored accepted rollout differs from current signed authority"
        )
    _validate_verified_rollout_runs(
        report.get("terminal_runs"),
        catalog_active_scope_count=catalog["active_scope_count"],
        expected_bootstrap_slots=expected_authority["bootstrap_slots"],
    )
    terminal_runs = report["terminal_runs"]
    latest_scheduled_run = report.get("latest_scheduled_run")
    if (
        not isinstance(latest_scheduled_run, dict)
        or set(latest_scheduled_run)
        != {"completed_at", "logical_date", "run_id", "state"}
        or latest_scheduled_run.get("state") != "success"
        or any(
            latest_scheduled_run.get(field) != terminal_runs[-1].get(field)
            for field in ("completed_at", "logical_date", "run_id")
        )
    ):
        raise AdmissionError(
            "WhoScored latest scheduler-created DagRun is not accepted"
        )
    report["authority_binding"] = "current-signed-rollout"
    report["rollout_authority"] = dict(rollout_authority)
    return report


def verify_issuance_rollout(
    rollout_id: str,
    *,
    rollout_authority: Mapping[str, Any],
    scheduler_container_id: str,
    runner: DockerRunner = _run_rollout_acceptance_docker,
) -> dict[str, Any]:
    """Replay the active wave's exact live predecessor proof before issuance."""

    if _ROLLOUT_ID.fullmatch(rollout_id) is None:
        raise AdmissionError("WhoScored issuance rollout id is invalid")
    if _CONTAINER_ID.fullmatch(scheduler_container_id) is None:
        raise AdmissionError("admitted scheduler container id is invalid")
    rollout_authority = _validate_current_issuance_projection(
        rollout_authority, rollout_id=rollout_id
    )
    expected_authority = rollout_authority["authority"]
    wave_id = expected_authority["wave_id"]
    expected_scope_authority = {
        **expected_authority,
        "catalog_active_scope_count": rollout_authority["catalog_active_scope_count"],
        "catalog_active_scopes_sha256": rollout_authority[
            "catalog_active_scopes_sha256"
        ],
    }
    raw = runner(
        (
            "exec",
            "--workdir=/opt/airflow",
            scheduler_container_id,
            "/usr/local/bin/whoscored-production-python",
            "-c",
            _ROLLOUT_ISSUANCE_PROBE,
            rollout_id,
            _authority_canonical_bytes(expected_scope_authority).decode("utf-8"),
        )
    )
    if not raw or len(raw) > 64 * 1024:
        raise AdmissionError("WhoScored issuance replay output is invalid")
    try:
        report = json.loads(raw.decode("utf-8"), object_pairs_hook=_unique_object)
    except (_DuplicateKey, UnicodeDecodeError, json.JSONDecodeError) as exc:
        raise AdmissionError(
            "WhoScored issuance replay output is not strict JSON"
        ) from exc
    if (
        not isinstance(report, dict)
        or set(report)
        != {
            "promotion",
            "rollout_id",
            "runtime_release",
            "schema_version",
            "status",
            "terminal_runs",
            "wave_id",
        }
        or raw != _canonical_bytes(report)
        or report.get("schema_version") != 1
        or report.get("status") != "live-authority-verified"
        or report.get("rollout_id") != rollout_id
        or report.get("wave_id") != wave_id
    ):
        raise AdmissionError("WhoScored issuance replay output is not accepted")

    runtime_release = _validated_rollout_runtime_release(report.get("runtime_release"))
    if runtime_release["code_tree_sha256"] != expected_authority["runtime_sha256"]:
        raise AdmissionError(
            "WhoScored issuance runtime differs from current signed authority"
        )
    promotion = report.get("promotion")
    promotion_fields = {
        "classifier_sha256",
        "promotion_acceptance_sha256",
        "receipt_sha256s",
        "release",
        "runtime_sha256",
        "schema_version",
        "source_cohort_sha256",
        "source_wave_id",
        "terminal_receipt_sha256",
    }
    if (
        not isinstance(promotion, dict)
        or set(promotion) != promotion_fields
        or promotion.get("schema_version") != 1
        or promotion.get("promotion_acceptance_sha256")
        != expected_authority["promotion_acceptance_sha256"]
        or promotion.get("terminal_receipt_sha256")
        != expected_authority["promotion_terminal_receipt_sha256"]
        or promotion.get("runtime_sha256") != expected_authority["runtime_sha256"]
        or promotion.get("classifier_sha256") != expected_authority["classifier_sha256"]
        or promotion.get("release") != runtime_release
    ):
        raise AdmissionError(
            "WhoScored live promotion proof differs from current signed authority"
        )
    receipt_sha256s = promotion.get("receipt_sha256s")
    expected_run_count = {"wave-20": 0, "wave-70": 2, "wave-all": 4}[wave_id]
    expected_source_wave = {
        "wave-20": None,
        "wave-70": "wave-20",
        "wave-all": "wave-70",
    }[wave_id]
    source_cohort_sha256 = promotion.get("source_cohort_sha256")
    if (
        promotion.get("source_wave_id") != expected_source_wave
        or not isinstance(receipt_sha256s, list)
        or len(receipt_sha256s) != expected_run_count
        or len(receipt_sha256s) != len(set(receipt_sha256s))
        or any(
            not isinstance(digest, str) or _DIGEST.fullmatch(digest) is None
            for digest in receipt_sha256s
        )
        or (expected_run_count == 0 and source_cohort_sha256 is not None)
        or (
            expected_run_count > 0
            and (
                not isinstance(source_cohort_sha256, str)
                or _DIGEST.fullmatch(source_cohort_sha256) is None
                or receipt_sha256s[-1]
                != expected_authority["promotion_terminal_receipt_sha256"]
            )
        )
    ):
        raise AdmissionError("WhoScored live promotion receipt chain is invalid")

    catalog_count = rollout_authority["catalog_active_scope_count"]
    expected_scope_counts = {
        "wave-20": (),
        "wave-70": (
            min(20, catalog_count),
            min(20, catalog_count),
        ),
        "wave-all": (
            min(20, catalog_count),
            min(20, catalog_count),
            min(70, catalog_count),
            min(70, catalog_count),
        ),
    }[wave_id]
    _validate_verified_rollout_runs(
        report.get("terminal_runs"),
        catalog_active_scope_count=catalog_count,
        expected_scope_counts=expected_scope_counts,
        require_fresh_completion=False,
    )
    return {
        "authority": dict(expected_authority),
        "authority_binding": "current-signed-rollout",
        "charter_sha256": rollout_authority["charter_sha256"],
        "promotion_acceptance_sha256": expected_authority[
            "promotion_acceptance_sha256"
        ],
        "promotion_terminal_receipt_sha256": expected_authority[
            "promotion_terminal_receipt_sha256"
        ],
        "rollout_id": rollout_id,
        "rollout_manifest_sha256": rollout_authority["rollout_manifest_sha256"],
        "schema_version": 1,
        "status": "live-authority-verified",
        "wave_id": wave_id,
    }


def _compose_arguments(
    *,
    root: Path,
    override_path: Path,
    env_files: Sequence[Path],
    project: str,
) -> tuple[str, ...]:
    if _PROJECT_NAME.fullmatch(project) is None:
        raise AdmissionError("Compose project name is invalid")
    config_files = (
        root / "compose.yaml",
        root / "compose.seaweedfs-supervised.yaml",
        override_path,
    )
    all_paths = (*config_files, *env_files)
    if any(not path.is_absolute() or "," in str(path) for path in all_paths):
        raise AdmissionError("Compose evidence paths must be absolute and comma-free")
    if (
        not env_files
        or len(env_files) != len(set(env_files))
        or len(all_paths) != len(set(all_paths))
    ):
        raise AdmissionError("Compose env files must be a non-empty unique sequence")
    for path in all_paths:
        _read_regular_file(path, label="Compose admission input")
    arguments: list[str] = ["compose", "--project-name", project]
    for env_file in env_files:
        arguments.extend(("--env-file", str(env_file)))
    arguments.extend(("--profile", "whoscored-paid"))
    for config_file in config_files:
        arguments.extend(("--file", str(config_file)))
    return tuple(arguments)


def render_attested_compose(
    bindings: Mapping[str, str],
    *,
    root: Path,
    override_path: Path,
    env_files: Sequence[Path],
    project: str,
    runner: DockerRunner = _run_docker,
    protected_inputs: Mapping[Path, bytes] | None = None,
) -> tuple[dict[str, dict[str, Any]], dict[str, str], tuple[Path, ...], dict[str, Any]]:
    """Render only the fixed production file set and capture Compose hashes."""

    verify_override(override_path, bindings)
    prefix = _compose_arguments(
        root=root,
        override_path=override_path,
        env_files=env_files,
        project=project,
    )
    config_files = (
        root / "compose.yaml",
        root / "compose.seaweedfs-supervised.yaml",
        override_path,
    )
    input_paths = (*config_files, *env_files)
    snapshots = {
        path: _read_regular_file(path, label="Compose admission input")
        for path in input_paths
    }
    if protected_inputs is not None and (
        set(protected_inputs) != set(input_paths)
        or any(
            not hmac.compare_digest(protected_inputs[path], snapshots[path][0])
            for path in input_paths
        )
    ):
        raise AdmissionError("protected Compose inputs differ from render snapshots")

    def assert_inputs_unchanged() -> None:
        for path, (expected_raw, expected_identity) in snapshots.items():
            actual_raw, actual_identity = _read_regular_file(
                path, label="Compose admission input"
            )
            if actual_identity != expected_identity or not hmac.compare_digest(
                actual_raw, expected_raw
            ):
                raise AdmissionError(f"Compose admission input changed: {path}")

    raw = runner((*prefix, "config", "--format", "json"))
    assert_inputs_unchanged()
    try:
        rendered = json.loads(raw.decode("utf-8"), object_pairs_hook=_unique_object)
    except (_DuplicateKey, UnicodeDecodeError, json.JSONDecodeError) as exc:
        raise AdmissionError("Docker Compose returned ambiguous rendered JSON") from exc
    if not isinstance(rendered, dict):
        raise AdmissionError("Docker Compose rendered model is not an object")
    projections = verify_rendered_compose(rendered, bindings)
    if protected_inputs is not None:
        _validate_bind_source_policy(projections, root=root)
    config_hashes: dict[str, str] = {}
    for service in PROTECTED_SERVICES:
        raw_hash = runner((*prefix, "config", "--hash", service))
        assert_inputs_unchanged()
        try:
            line = raw_hash.decode("ascii").strip()
        except UnicodeDecodeError as exc:
            raise AdmissionError(
                f"Compose config hash is non-ASCII: {service}"
            ) from exc
        fields = line.split()
        if (
            len(fields) != 2
            or fields[0] != service
            or _CONFIG_HASH.fullmatch(fields[1]) is None
        ):
            raise AdmissionError(f"Compose config hash is invalid: {service}")
        config_hashes[service] = fields[1]
    return projections, config_hashes, config_files, rendered


def _fixed_project_arguments(
    *,
    root: Path,
    project: str,
    override_path: Path,
    env_files: Sequence[Path],
) -> tuple[tuple[str, ...], tuple[Path, ...]]:
    if project == COMMON_PROJECT:
        config_files = (
            root / "compose.yaml",
            root / "compose.seaweedfs-supervised.yaml",
            override_path,
        )
    elif project == GATEWAY_PROJECT:
        config_files = (
            root / "deploy/whoscored/gateway.compose.yaml",
            override_path,
        )
    else:
        raise AdmissionError("Compose project is not a fixed WhoScored project")
    all_paths = (*config_files, *env_files)
    if any(not path.is_absolute() or "," in str(path) for path in all_paths):
        raise AdmissionError("Compose evidence paths must be absolute and comma-free")
    if (
        not env_files
        or len(env_files) != len(set(env_files))
        or len(all_paths) != len(set(all_paths))
    ):
        raise AdmissionError("Compose env files must be a non-empty unique sequence")
    for path in all_paths:
        _read_regular_file(path, label="Compose admission input")
    arguments: list[str] = [
        "compose",
        "--project-name",
        project,
        "--project-directory",
        str(root),
    ]
    for env_file in env_files:
        arguments.extend(("--env-file", str(env_file)))
    for config_file in config_files:
        arguments.extend(("--file", str(config_file)))
    return tuple(arguments), config_files


def render_attested_common_project(
    bindings: Mapping[str, str],
    *,
    root: Path,
    common_override_path: Path,
    env_files: Sequence[Path],
    runner: DockerRunner = _run_docker,
    protected_inputs: Mapping[Path, bytes] | None = None,
) -> tuple[
    dict[str, dict[str, Any]],
    dict[str, str],
    tuple[Path, ...],
    dict[str, Any],
]:
    """Render and fully verify only the ready common production project."""

    if set(bindings) != _PROTECTED_SERVICE_SET:
        raise AdmissionError("common render bindings omit a protected service")
    verify_override(common_override_path, bindings, COMMON_PROTECTED_SERVICES)
    prefix, config_files = _fixed_project_arguments(
        root=root,
        project=COMMON_PROJECT,
        override_path=common_override_path,
        env_files=env_files,
    )
    input_paths = (*config_files, *env_files)
    snapshots = {
        path: _read_regular_file(path, label="Compose admission input")
        for path in input_paths
    }
    if protected_inputs is not None and (
        set(protected_inputs) != set(snapshots)
        or any(
            not hmac.compare_digest(protected_inputs[path], snapshot[0])
            for path, snapshot in snapshots.items()
        )
    ):
        raise AdmissionError("protected common Compose inputs differ")

    raw = runner((*prefix, "config", "--format", "json"))
    try:
        rendered = json.loads(raw.decode("utf-8"), object_pairs_hook=_unique_object)
    except (_DuplicateKey, UnicodeDecodeError, json.JSONDecodeError) as exc:
        raise AdmissionError("common Compose returned ambiguous JSON") from exc
    services = rendered.get("services") if isinstance(rendered, dict) else None
    if (
        not isinstance(rendered, dict)
        or rendered.get("name") != COMMON_PROJECT
        or not isinstance(services, dict)
        or any(service not in services for service in COMMON_PROTECTED_SERVICES)
        or any(service in services for service in GATEWAY_PROTECTED_SERVICES)
    ):
        raise AdmissionError("rendered common Compose boundary differs")
    rendered_networks = rendered.get("networks")
    common_network_names = {
        name
        for service in COMMON_PROTECTED_SERVICES
        for name in _EXPECTED_NETWORKS[service]
    }
    owned_common_networks = common_network_names - set(_COMMON_EXTERNAL_NETWORKS)
    if (
        not isinstance(rendered_networks, dict)
        or any(
            rendered_networks.get(name) != _EXPECTED_NETWORK_DEFINITIONS[name]
            for name in owned_common_networks
        )
        or any(
            rendered_networks.get(name) != definition
            for name, definition in _COMMON_EXTERNAL_NETWORKS.items()
        )
    ):
        raise AdmissionError("rendered common Compose networks differ")
    protected_rendered = {
        "services": {
            service: services[service] for service in COMMON_PROTECTED_SERVICES
        },
        "networks": {
            # The common Compose model must declare paid-api as external.  The
            # generic protected-service verifier receives the exact owned
            # network policy so scheduler attachment semantics are checked
            # without pretending the common project owns that network.
            name: _EXPECTED_NETWORK_DEFINITIONS[name]
            for name in common_network_names
        },
        "volumes": rendered.get("volumes"),
    }
    projections = verify_rendered_compose(
        protected_rendered,
        bindings,
        selected_services=COMMON_PROTECTED_SERVICES,
    )
    scheduler_environment = projections["airflow-scheduler"]["environment"]
    if (
        scheduler_environment.get("WHOSCORED_SOURCE_POOL_SLOTS") != "4"
        or scheduler_environment.get("WHOSCORED_BACKFILL_ASSUMED_REQUEST_UNITS_PER_DAY")
        != "144000"
    ):
        raise AdmissionError(
            "common capacity profile must use four source slots and 144000 units/day"
        )
    config_hashes: dict[str, str] = {}
    for service in COMMON_PROTECTED_SERVICES:
        raw_hash = runner((*prefix, "config", "--hash", service))
        try:
            fields = raw_hash.decode("ascii").strip().split()
        except UnicodeDecodeError as exc:
            raise AdmissionError(
                f"common Compose config hash is non-ASCII: {service}"
            ) from exc
        if (
            len(fields) != 2
            or fields[0] != service
            or _CONFIG_HASH.fullmatch(fields[1]) is None
        ):
            raise AdmissionError(f"common Compose config hash is invalid: {service}")
        config_hashes[service] = fields[1]
    for path, (expected_raw, expected_identity) in snapshots.items():
        actual_raw, actual_identity = _read_regular_file(
            path, label="Compose admission input"
        )
        if actual_identity != expected_identity or not hmac.compare_digest(
            actual_raw, expected_raw
        ):
            raise AdmissionError(f"common Compose admission input changed: {path}")
    if protected_inputs is not None:
        _validate_bind_source_policy(
            projections,
            root=root,
            selected_services=COMMON_PROTECTED_SERVICES,
        )
    return projections, config_hashes, config_files, rendered


def render_attested_projects(
    bindings: Mapping[str, str],
    *,
    root: Path,
    common_override_path: Path,
    gateway_override_path: Path,
    env_files: Sequence[Path],
    provider_authority: Mapping[str, object],
    runner: DockerRunner = _run_docker,
    protected_inputs: Mapping[Path, bytes] | None = None,
) -> tuple[
    dict[str, dict[str, Any]],
    dict[str, str],
    dict[str, tuple[Path, ...]],
    dict[str, dict[str, Any]],
]:
    """Render and cross-check the fixed common and paid-gateway projects."""

    verify_override(common_override_path, bindings, COMMON_PROTECTED_SERVICES)
    verify_override(gateway_override_path, bindings, GATEWAY_PROTECTED_SERVICES)
    project_specs = {
        COMMON_PROJECT: (common_override_path, COMMON_PROTECTED_SERVICES),
        GATEWAY_PROJECT: (gateway_override_path, GATEWAY_PROTECTED_SERVICES),
    }
    rendered_projects: dict[str, dict[str, Any]] = {}
    config_hashes: dict[str, str] = {}
    config_files_by_project: dict[str, tuple[Path, ...]] = {}
    snapshots: dict[Path, tuple[bytes, tuple[int, ...]]] = {}
    for project, (override_path, services) in project_specs.items():
        prefix, config_files = _fixed_project_arguments(
            root=root,
            project=project,
            override_path=override_path,
            env_files=env_files,
        )
        config_files_by_project[project] = config_files
        for path in (*config_files, *env_files):
            snapshots.setdefault(
                path, _read_regular_file(path, label="Compose admission input")
            )
        raw = runner((*prefix, "config", "--format", "json"))
        try:
            rendered = json.loads(raw.decode("utf-8"), object_pairs_hook=_unique_object)
        except (_DuplicateKey, UnicodeDecodeError, json.JSONDecodeError) as exc:
            raise AdmissionError(
                f"Docker Compose returned ambiguous rendered JSON: {project}"
            ) from exc
        if not isinstance(rendered, dict) or rendered.get("name") != project:
            raise AdmissionError(
                f"rendered Compose project identity differs: {project}"
            )
        rendered_projects[project] = rendered
        for service in services:
            raw_hash = runner((*prefix, "config", "--hash", service))
            try:
                line = raw_hash.decode("ascii").strip()
            except UnicodeDecodeError as exc:
                raise AdmissionError(
                    f"Compose config hash is non-ASCII: {service}"
                ) from exc
            fields = line.split()
            if (
                len(fields) != 2
                or fields[0] != service
                or _CONFIG_HASH.fullmatch(fields[1]) is None
            ):
                raise AdmissionError(f"Compose config hash is invalid: {service}")
            config_hashes[service] = fields[1]
    if protected_inputs is not None and (
        set(protected_inputs) != set(snapshots)
        or any(
            not hmac.compare_digest(protected_inputs[path], snapshot[0])
            for path, snapshot in snapshots.items()
        )
    ):
        raise AdmissionError("protected Compose inputs differ from render snapshots")
    for path, (expected_raw, expected_identity) in snapshots.items():
        actual_raw, actual_identity = _read_regular_file(
            path, label="Compose admission input"
        )
        if actual_identity != expected_identity or not hmac.compare_digest(
            actual_raw, expected_raw
        ):
            raise AdmissionError(f"Compose admission input changed: {path}")

    common = rendered_projects[COMMON_PROJECT]
    gateway = rendered_projects[GATEWAY_PROJECT]
    common_services = common.get("services")
    gateway_services = gateway.get("services")
    if not isinstance(common_services, dict) or not isinstance(gateway_services, dict):
        raise AdmissionError("rendered split Compose service models differ")
    if any(service in common_services for service in GATEWAY_PROTECTED_SERVICES):
        raise AdmissionError("common project owns a paid-gateway service")
    if set(gateway_services) != set(GATEWAY_PROTECTED_SERVICES):
        raise AdmissionError("paid-gateway project service boundary differs")
    if any(service not in common_services for service in COMMON_PROTECTED_SERVICES):
        raise AdmissionError("common project omits a protected common service")
    if any(
        gateway_services[service].get("profiles") is not None
        for service in GATEWAY_PROTECTED_SERVICES
    ):
        raise AdmissionError("paid-gateway project retains an opt-in profile")
    if gateway_services["whoscored_proxy_filter"].get("depends_on") is not None:
        raise AdmissionError("paid filter depends on a common-project service")
    filter_environment = gateway_services["whoscored_proxy_filter"].get("environment")
    if not isinstance(filter_environment, dict) or any(
        filter_environment.get(name) != str(provider_authority.get(authority_name))
        for name, authority_name in (
            ("WHOSCORED_PROVIDER_ORDER_ID", "order_id"),
            ("WHOSCORED_PROVIDER_POLICY_SHA256", "provider_policy_sha256"),
            ("WHOSCORED_PROVIDER_ORDER_CAP_BYTES", "order_cap_bytes"),
            ("WHOSCORED_PROXY_FILTER_DAILY_BUDGET_BYTES", "daily_cap_bytes"),
        )
    ):
        raise AdmissionError("paid filter does not bind the admitted provider policy")
    scheduler_model = common_services["airflow-scheduler"]
    scheduler_environment = scheduler_model.get("environment")
    if (
        not isinstance(scheduler_environment, dict)
        or set(scheduler_environment)
        != _EXPECTED_ENVIRONMENT_NAMES["airflow-scheduler"]
        or scheduler_environment.get("WHOSCORED_SCHEDULED_PAID_MODE") != "required"
        or "WHOSCORED_PROXY_APPROVAL_PATH" in scheduler_environment
    ):
        raise AdmissionError("common scheduler paid authority differs")
    scheduler_volumes = scheduler_model.get("volumes")
    if not isinstance(scheduler_volumes, list) or not any(
        isinstance(volume, dict)
        and volume.get("target") == "/opt/airflow/secure/whoscored-scheduled-pointers"
        and volume.get("read_only") is True
        for volume in scheduler_volumes
    ):
        raise AdmissionError("common scheduler pointer authority mount differs")
    common_networks = common.get("networks")
    gateway_networks = gateway.get("networks")
    if not isinstance(common_networks, dict) or not isinstance(gateway_networks, dict):
        raise AdmissionError("rendered split Compose network models differ")
    if (
        common_networks.get("whoscored-paid-api")
        != _COMMON_EXTERNAL_NETWORKS["whoscored-paid-api"]
    ):
        raise AdmissionError("common project does not use external paid API network")
    if set(gateway_networks) != {
        "whoscored-paid-api",
        "whoscored-paid-browser",
        "whoscored-paid-direct-egress",
        "whoscored-paid-provider-egress",
    } or any(
        gateway_networks.get(name) != _EXPECTED_NETWORK_DEFINITIONS[name]
        for name in gateway_networks
    ):
        raise AdmissionError("paid-gateway project network ownership differs")
    combined = {
        "services": {
            **{
                service: common_services[service]
                for service in COMMON_PROTECTED_SERVICES
            },
            **{
                service: gateway_services[service]
                for service in GATEWAY_PROTECTED_SERVICES
            },
        },
        "networks": {
            **{
                name: common_networks[name]
                for name in ("backend", "frontend", "storage")
            },
            **{name: gateway_networks[name] for name in gateway_networks},
        },
        "volumes": common.get("volumes"),
    }
    projections = verify_rendered_compose(
        combined,
        bindings,
        provider_order_cap_bytes=provider_authority.get("order_cap_bytes"),
    )
    if protected_inputs is not None:
        _validate_bind_source_policy(projections, root=root)
    return projections, config_hashes, config_files_by_project, rendered_projects


def _docker_object(raw: bytes, *, label: str) -> dict[str, Any]:
    try:
        value = json.loads(raw.decode("utf-8"), object_pairs_hook=_unique_object)
    except (_DuplicateKey, UnicodeDecodeError, json.JSONDecodeError) as exc:
        raise AdmissionError(f"{label} returned ambiguous Docker JSON") from exc
    if not isinstance(value, list) or len(value) != 1 or not isinstance(value[0], dict):
        raise AdmissionError(f"{label} did not resolve to exactly one Docker object")
    return value[0]


def _verify_docker_security_options(*, runner: DockerRunner) -> tuple[str, ...]:
    """Prove the daemon applies its built-in seccomp and AppArmor defaults."""

    raw = runner(("info", "--format", "{{json .SecurityOptions}}"))
    try:
        value = json.loads(raw.decode("utf-8"))
    except (UnicodeDecodeError, json.JSONDecodeError) as exc:
        raise AdmissionError("Docker daemon security options are invalid") from exc
    if (
        not isinstance(value, list)
        or not all(isinstance(option, str) and option for option in value)
        or len(value) != len(set(value))
    ):
        raise AdmissionError("Docker daemon security options are invalid")
    options = frozenset(value)
    security_families = {
        option
        for option in options
        if option.startswith(("name=apparmor", "name=seccomp"))
    }
    if (
        not _REQUIRED_DOCKER_SECURITY_OPTIONS.issubset(options)
        or security_families != _REQUIRED_DOCKER_SECURITY_OPTIONS
    ):
        raise AdmissionError(
            "Docker daemon must enable AppArmor and the built-in seccomp profile"
        )
    return tuple(sorted(options))


def _apparmor_probe_arguments(image: str) -> tuple[str, ...]:
    if _PINNED_IMAGE.fullmatch(image) is None:
        raise AdmissionError("AppArmor probe image is not digest-pinned")
    return (
        "run",
        "--rm",
        "--pull=never",
        "--network=none",
        "--read-only",
        "--user=50000:0",
        "--cap-drop=ALL",
        "--security-opt=no-new-privileges=true",
        "--security-opt=apparmor=docker-default",
        "--security-opt=seccomp=builtin",
        "--entrypoint=/bin/cat",
        image,
        "/proc/self/attr/current",
    )


def _verify_apparmor_enforcement(*, runner: DockerRunner, image: str) -> str:
    """Prove enforcement inside a constrained digest-attested container."""

    try:
        lines = runner(_apparmor_probe_arguments(image)).decode("ascii").splitlines()
    except UnicodeDecodeError as exc:
        raise AdmissionError(
            "AppArmor enforcement probe returned invalid output"
        ) from exc
    if lines != ["docker-default (enforce)"]:
        raise AdmissionError(
            "docker-default AppArmor probe must report enforce mode exactly once"
        )
    return lines[0]


def _environment_mapping(value: object, *, label: str) -> dict[str, str]:
    sequence = _string_sequence(value, label=label) or ()
    result: dict[str, str] = {}
    for item in sequence:
        name, separator, content = item.partition("=")
        if not separator or not name or name in result:
            raise AdmissionError(f"{label} contains duplicate or invalid entries")
        result[name] = content
    return result


def _verify_container_labels(
    labels: Mapping[str, Any],
    image_labels_value: object,
    *,
    service: str,
    project: str,
    config_hash: str,
    image_id: str,
    config_files: str,
    env_files: str,
    working_dir: str,
) -> None:
    if image_labels_value is None:
        image_labels: dict[str, str] = {}
    elif isinstance(image_labels_value, dict) and all(
        isinstance(name, str) and isinstance(value, str)
        for name, value in image_labels_value.items()
    ):
        image_labels = dict(image_labels_value)
    else:
        raise AdmissionError(f"image labels are invalid: {service}")
    if any(not name.startswith(_SAFE_IMAGE_LABEL_PREFIXES) for name in image_labels):
        raise AdmissionError(f"image has an unsafe integration label: {service}")
    compose_version = labels.get("com.docker.compose.version")
    version_match = (
        _COMPOSE_VERSION.fullmatch(compose_version)
        if isinstance(compose_version, str)
        else None
    )
    if version_match is None or tuple(
        int(version_match.group(index)) for index in (1, 2, 3)
    ) < (2, 24, 4):
        raise AdmissionError(f"container Compose version label differs: {service}")
    compose_labels = {
        "com.docker.compose.config-hash": config_hash,
        "com.docker.compose.container-number": "1",
        "com.docker.compose.depends_on": "",
        "com.docker.compose.image": image_id,
        "com.docker.compose.oneoff": "False",
        "com.docker.compose.project": project,
        "com.docker.compose.project.config_files": config_files,
        "com.docker.compose.project.environment_file": env_files,
        "com.docker.compose.project.working_dir": working_dir,
        "com.docker.compose.service": service,
        "com.docker.compose.version": compose_version,
    }
    replacement = labels.get("com.docker.compose.replace")
    if replacement is not None:
        if replacement != service:
            raise AdmissionError(f"container replacement label differs: {service}")
        compose_labels["com.docker.compose.replace"] = service
    expected = {**image_labels, **compose_labels}
    if labels != expected:
        raise AdmissionError(f"container has unexpected integration labels: {service}")


def _port_bindings(
    value: object, *, label: str
) -> dict[str, tuple[dict[str, str], ...]]:
    if value is None:
        return {}
    if not isinstance(value, dict):
        raise AdmissionError(f"{label} is invalid")
    result: dict[str, tuple[dict[str, str], ...]] = {}
    for port, bindings in value.items():
        if not isinstance(port, str) or not isinstance(bindings, list):
            raise AdmissionError(f"{label} is invalid")
        normalized: list[dict[str, str]] = []
        for binding in bindings:
            if not isinstance(binding, dict) or set(binding) != {"HostIp", "HostPort"}:
                raise AdmissionError(f"{label} is invalid")
            host_ip = binding.get("HostIp")
            host_port = binding.get("HostPort")
            if not isinstance(host_ip, str) or not isinstance(host_port, str):
                raise AdmissionError(f"{label} is invalid")
            normalized.append({"HostIp": host_ip, "HostPort": host_port})
        result[port] = tuple(normalized)
    return result


def _verify_docker_network(
    *,
    logical_name: str,
    project: str,
    runner: DockerRunner,
    expected_containers: Mapping[str, str] | None = None,
    required_containers: Mapping[str, str] | None = None,
    optional_containers: Mapping[str, str] | None = None,
    forbidden_container_ids: frozenset[str] = frozenset(),
) -> dict[str, str]:
    definition = _EXPECTED_NETWORK_DEFINITIONS[logical_name]
    expected_name = definition["name"]
    expected_internal = bool(definition.get("internal", False))
    network = _docker_object(
        runner(("network", "inspect", expected_name)),
        label=f"network inspect for {expected_name}",
    )
    network_id = network.get("Id")
    if (
        network_id is None
        or not isinstance(network_id, str)
        or _CONTAINER_ID.fullmatch(network_id) is None
        or network.get("Name") != expected_name
        or network.get("Driver") != "bridge"
        or network.get("Scope") != "local"
        or network.get("Internal") is not expected_internal
        or network.get("Attachable") is not False
        or network.get("Ingress") is not False
        or network.get("EnableIPv4") is not True
        or network.get("EnableIPv6") is not False
        or network.get("Options") != {}
    ):
        raise AdmissionError(f"Docker network policy differs: {expected_name}")
    labels = network.get("Labels")
    if not isinstance(labels, dict) or set(labels) != {
        "com.docker.compose.config-hash",
        "com.docker.compose.network",
        "com.docker.compose.project",
        "com.docker.compose.version",
    }:
        raise AdmissionError(f"Docker network labels differ: {expected_name}")
    version = labels.get("com.docker.compose.version")
    version_match = (
        _COMPOSE_VERSION.fullmatch(version) if isinstance(version, str) else None
    )
    if (
        labels.get("com.docker.compose.network") != logical_name
        or labels.get("com.docker.compose.project") != project
        or not isinstance(labels.get("com.docker.compose.config-hash"), str)
        or _CONFIG_HASH.fullmatch(labels["com.docker.compose.config-hash"]) is None
        or version_match is None
        or tuple(int(version_match.group(index)) for index in (1, 2, 3)) < (2, 24, 4)
    ):
        raise AdmissionError(f"Docker network identity differs: {expected_name}")
    ipam = network.get("IPAM")
    if (
        not isinstance(ipam, dict)
        or set(ipam) != {"Config", "Driver", "Options"}
        or ipam.get("Driver") != "default"
        or ipam.get("Options") is not None
    ):
        raise AdmissionError(f"Docker network IPAM differs: {expected_name}")
    configurations = ipam.get("Config")
    if not isinstance(configurations, list) or len(configurations) != 1:
        raise AdmissionError(f"Docker network subnet differs: {expected_name}")
    configuration = configurations[0]
    if not isinstance(configuration, dict) or set(configuration) != {
        "Gateway",
        "IPRange",
        "Subnet",
    }:
        raise AdmissionError(f"Docker network subnet differs: {expected_name}")
    try:
        subnet = ipaddress.ip_network(configuration["Subnet"], strict=True)
        gateway = ipaddress.ip_address(configuration["Gateway"])
        ip_range = configuration["IPRange"]
        selected_range = (
            ipaddress.ip_network(ip_range, strict=True) if ip_range else subnet
        )
    except (KeyError, TypeError, ValueError) as exc:
        raise AdmissionError(f"Docker network subnet differs: {expected_name}") from exc
    if (
        subnet.version != 4
        or not subnet.is_private
        or subnet.is_loopback
        or subnet.prefixlen < 16
        or gateway not in subnet
        or selected_range.version != 4
        or not selected_range.subnet_of(subnet)
    ):
        raise AdmissionError(f"Docker network subnet differs: {expected_name}")
    containers = network.get("Containers")
    inspect_containers = (
        expected_containers is not None
        or required_containers is not None
        or optional_containers is not None
        or bool(forbidden_container_ids)
    )
    if inspect_containers:
        if not isinstance(containers, dict):
            raise AdmissionError(
                f"Docker network container inventory differs: {expected_name}"
            )
        endpoint_names: set[str] = set()
        for container_id, endpoint in containers.items():
            endpoint_name = endpoint.get("Name") if isinstance(endpoint, dict) else None
            if (
                not isinstance(container_id, str)
                or _CONTAINER_ID.fullmatch(container_id) is None
                or not isinstance(endpoint_name, str)
                or not endpoint_name
                or endpoint_name in endpoint_names
            ):
                raise AdmissionError(
                    f"Docker network container identity differs: {expected_name}"
                )
            endpoint_names.add(endpoint_name)
    if expected_containers is not None:
        assert isinstance(containers, dict)
        if set(containers) != set(expected_containers):
            raise AdmissionError(
                f"Docker network container inventory differs: {expected_name}"
            )
        for container_id, service in expected_containers.items():
            endpoint = containers.get(container_id)
            if (
                _CONTAINER_ID.fullmatch(container_id) is None
                or not isinstance(endpoint, dict)
                or endpoint.get("Name") != service
            ):
                raise AdmissionError(
                    f"Docker network container identity differs: {expected_name}"
                )
    if required_containers is not None:
        assert isinstance(containers, dict)
        for container_id, service in required_containers.items():
            endpoint = containers.get(container_id)
            if not isinstance(endpoint, dict) or endpoint.get("Name") != service:
                raise AdmissionError(
                    f"Docker network required container differs: {expected_name}"
                )
    if optional_containers is not None:
        assert isinstance(containers, dict)
        for container_id, service in optional_containers.items():
            admitted_endpoint = containers.get(container_id)
            observed_ids = {
                observed_id
                for observed_id, endpoint in containers.items()
                if endpoint.get("Name") == service
            }
            if (
                admitted_endpoint is not None
                and admitted_endpoint.get("Name") != service
                or observed_ids not in (set(), {container_id})
            ):
                raise AdmissionError(
                    f"Docker network optional container differs: {expected_name}"
                )
    if forbidden_container_ids and (
        not isinstance(containers, dict)
        or set(containers) & set(forbidden_container_ids)
    ):
        raise AdmissionError(
            f"Docker network contains stopped protected container: {expected_name}"
        )
    return {
        "id": network_id,
        "logical_name": logical_name,
        "name": expected_name,
        "subnet": str(subnet),
    }


def _docker_id_lines(raw: bytes, *, label: str) -> tuple[str, ...]:
    try:
        ids = tuple(raw.decode("ascii").splitlines())
    except UnicodeDecodeError as exc:
        raise AdmissionError(f"{label} returned non-ASCII Docker IDs") from exc
    if len(ids) != len(set(ids)) or any(
        _CONTAINER_ID.fullmatch(container_id) is None for container_id in ids
    ):
        raise AdmissionError(f"{label} returned duplicate or invalid Docker IDs")
    return ids


def _listed_cutover_networks(
    raw: bytes,
) -> dict[str, str]:
    try:
        lines = raw.decode("ascii").splitlines()
    except UnicodeDecodeError as exc:
        raise AdmissionError("cutover network inventory is not ASCII") from exc
    expected_names = {
        _EXPECTED_NETWORK_DEFINITIONS[logical_name]["name"]: logical_name
        for logical_name in _LEGACY_CUTOVER_ALL_NETWORKS
    }
    result: dict[str, str] = {}
    seen_ids: set[str] = set()
    seen_names: set[str] = set()
    for line in lines:
        network_id, separator, name = line.partition("\t")
        if (
            not separator
            or not network_id
            or not name
            or "\t" in name
            or _CONTAINER_ID.fullmatch(network_id) is None
            or network_id in seen_ids
            or name in seen_names
        ):
            raise AdmissionError("cutover network inventory is malformed")
        seen_ids.add(network_id)
        seen_names.add(name)
        logical_name = expected_names.get(name)
        if logical_name is None:
            continue
        if logical_name in result:
            raise AdmissionError("cutover network inventory is duplicated or invalid")
        result[logical_name] = network_id
    return result


def _cutover_input_evidence(path: Path) -> dict[str, Any]:
    """Capture one root-protected rollback input without exposing its bytes."""

    try:
        raw, identity = provenance.read_protected_regular_file_snapshot(
            path, label="legacy rollback Compose input"
        )
    except provenance.ProvenanceError as exc:
        raise AdmissionError(str(exc)) from exc
    (
        device,
        inode,
        mode,
        uid,
        gid,
        link_count,
        size,
        modified_ns,
        changed_ns,
    ) = identity
    return {
        "changed_ns": changed_ns,
        "device": device,
        "gid": gid,
        "inode": inode,
        "link_count": link_count,
        "mode": stat.S_IMODE(mode),
        "modified_ns": modified_ns,
        "path": str(path),
        "sha256": hashlib.sha256(raw).hexdigest(),
        "size": size,
        "uid": uid,
    }


CutoverInputReader = Callable[[Path], dict[str, Any]]


def _has_control_characters(value: str) -> bool:
    return any(ord(character) < 32 or ord(character) == 127 for character in value)


def _cutover_path_list(value: object, *, label: str) -> tuple[Path, ...]:
    if not isinstance(value, str) or not value:
        raise AdmissionError(f"legacy rollback {label} is missing")
    if _has_control_characters(value):
        raise AdmissionError(f"legacy rollback {label} is invalid")
    paths = tuple(Path(item) for item in value.split(","))
    if (
        not paths
        or len(paths) != len(set(paths))
        or any(not path.is_absolute() or "," in str(path) for path in paths)
    ):
        raise AdmissionError(f"legacy rollback {label} is invalid")
    return paths


def _cutover_directory_evidence(path: Path) -> dict[str, Any]:
    descriptor = -1
    parent = -1
    try:
        parent, name = provenance.open_protected_parent(
            path, label="legacy rollback working directory"
        )
        descriptor = os.open(
            name,
            os.O_RDONLY | os.O_DIRECTORY | os.O_NOFOLLOW | os.O_CLOEXEC,
            dir_fd=parent,
        )
        before = os.fstat(descriptor)
        entry = os.stat(name, dir_fd=parent, follow_symlinks=False)
    except (OSError, provenance.ProvenanceError) as exc:
        raise AdmissionError(
            f"legacy rollback working directory is not protected: {path}"
        ) from exc
    finally:
        if descriptor >= 0:
            os.close(descriptor)
        if parent >= 0:
            os.close(parent)
    fields = (
        "st_dev",
        "st_ino",
        "st_mode",
        "st_uid",
        "st_gid",
        "st_mtime_ns",
        "st_ctime_ns",
    )
    if (
        before.st_uid != 0
        or before.st_mode & 0o022
        or any(getattr(before, field) != getattr(entry, field) for field in fields)
    ):
        raise AdmissionError(
            f"legacy rollback working directory is not protected: {path}"
        )
    return {
        "changed_ns": before.st_ctime_ns,
        "device": before.st_dev,
        "gid": before.st_gid,
        "inode": before.st_ino,
        "mode": stat.S_IMODE(before.st_mode),
        "modified_ns": before.st_mtime_ns,
        "path": str(path),
        "uid": before.st_uid,
    }


CutoverDirectoryReader = Callable[[Path], dict[str, Any]]


def _cutover_compose_prefix(
    *,
    config_files: Sequence[Path],
    environment_files: Sequence[Path],
    profiles: Sequence[str],
    working_directory: Path,
) -> tuple[str, ...]:
    arguments = [
        "compose",
        "--project-name",
        COMMON_PROJECT,
        "--project-directory",
        str(working_directory),
    ]
    for path in environment_files:
        arguments.extend(("--env-file", str(path)))
    for profile in profiles:
        arguments.extend(("--profile", profile))
    for path in config_files:
        arguments.extend(("--file", str(path)))
    return tuple(arguments)


def _parse_cutover_timestamp(value: object, *, label: str) -> datetime:
    if not isinstance(value, str) or not value.endswith("Z"):
        raise AdmissionError(f"{label} is not a UTC timestamp")
    try:
        parsed = datetime.fromisoformat(value[:-1] + "+00:00")
    except ValueError as exc:
        raise AdmissionError(f"{label} is not a UTC timestamp") from exc
    if parsed.utcoffset() != timedelta(0):
        raise AdmissionError(f"{label} is not a UTC timestamp")
    return parsed.astimezone(timezone.utc)


def _cutover_now() -> datetime:
    return datetime.now(timezone.utc)


def _verified_cutover_postgres(*, runner: DockerRunner) -> str:
    name_ids = _docker_id_lines(
        runner(
            (
                "container",
                "ls",
                "--all",
                "--no-trunc",
                "--filter",
                "name=^/postgres$",
                "--format",
                "{{.ID}}",
            )
        ),
        label="exact-name PostgreSQL inventory",
    )
    service_ids = _docker_id_lines(
        runner(
            (
                "container",
                "ls",
                "--all",
                "--no-trunc",
                "--filter",
                f"label=com.docker.compose.project={COMMON_PROJECT}",
                "--filter",
                "label=com.docker.compose.service=postgres",
                "--format",
                "{{.ID}}",
            )
        ),
        label="Compose PostgreSQL inventory",
    )
    if name_ids != service_ids or len(name_ids) != 1:
        raise AdmissionError("shared PostgreSQL container identity is ambiguous")
    container_id = name_ids[0]
    container = _docker_object(
        runner(("container", "inspect", container_id)),
        label="shared PostgreSQL inspect",
    )
    config = container.get("Config")
    labels = config.get("Labels") if isinstance(config, dict) else None
    state = container.get("State")
    health = state.get("Health") if isinstance(state, dict) else None
    if (
        container.get("Id") != container_id
        or container.get("Name") != "/postgres"
        or not isinstance(labels, dict)
        or labels.get("com.docker.compose.project") != COMMON_PROJECT
        or labels.get("com.docker.compose.service") != "postgres"
        or labels.get("com.docker.compose.container-number") != "1"
        or labels.get("com.docker.compose.oneoff") != "False"
        or not isinstance(state, dict)
        or state.get("Status") != "running"
        or state.get("Running") is not True
        or any(
            state.get(field) is not False for field in ("Dead", "Paused", "Restarting")
        )
        or not isinstance(health, dict)
        or health.get("Status") != "healthy"
    ):
        raise AdmissionError("shared PostgreSQL container is not healthy and exact")
    return container_id


def _cutover_psql_json(
    sql: str,
    *,
    container_id: str,
    label: str,
    runner: DockerRunner,
) -> Any:
    raw = runner(
        (
            "container",
            "exec",
            container_id,
            "/bin/sh",
            "-ceu",
            "exec /usr/local/bin/psql -X --quiet --tuples-only --no-align "
            '--set=ON_ERROR_STOP=1 --username "$POSTGRES_USER" '
            '--dbname airflow --command "$1"',
            "whoscored-cutover-read-only",
            sql,
        )
    )
    try:
        lines = [line for line in raw.decode("utf-8").splitlines() if line]
        result = json.loads(lines[0], object_pairs_hook=_unique_object)
    except (
        _DuplicateKey,
        IndexError,
        UnicodeDecodeError,
        json.JSONDecodeError,
    ) as exc:
        raise AdmissionError(f"{label} returned invalid JSON") from exc
    if len(lines) != 1:
        raise AdmissionError(f"{label} returned multiple values")
    return result


def snapshot_cutover_dag_pauses(
    *, runner: DockerRunner = _run_docker
) -> dict[str, Any]:
    """Capture every DagModel pause flag before all trigger paths are frozen."""

    container_id = _verified_cutover_postgres(runner=runner)
    snapshot = _cutover_psql_json(
        _CUTOVER_DAG_PAUSE_SQL,
        container_id=container_id,
        label="DagModel pause snapshot",
        runner=runner,
    )
    states = snapshot.get("dag_pause_states") if isinstance(snapshot, dict) else None
    if (
        not isinstance(snapshot, dict)
        or set(snapshot) != {"dag_pause_states", "observed_at"}
        or not isinstance(states, list)
        or any(
            not isinstance(item, dict)
            or set(item) != {"dag_id", "is_paused"}
            or not isinstance(item.get("dag_id"), str)
            or not item["dag_id"]
            or type(item.get("is_paused")) is not bool
            for item in states
        )
        or len({item["dag_id"] for item in states}) != len(states)
    ):
        raise AdmissionError("DagModel pause snapshot schema differs")
    _parse_cutover_timestamp(snapshot.get("observed_at"), label="DB observation time")
    return {
        **snapshot,
        "postgres_container_id": container_id,
        "schema_version": 1,
        "status": "dag-pauses-snapshotted-v1",
    }


def verify_cutover_quiescence(*, runner: DockerRunner = _run_docker) -> dict[str, Any]:
    """Prove one cross-source, read-only metadata snapshot has no active work."""

    container_id = _verified_cutover_postgres(runner=runner)
    snapshot = _cutover_psql_json(
        _CUTOVER_QUIESCENCE_SQL,
        container_id=container_id,
        label="cutover quiescence query",
        runner=runner,
    )
    active_fields = (
        "active_dag_runs",
        "active_task_instances",
        "active_non_scheduler_jobs",
        "active_fbref_crawl_runs",
        "active_fbref_publication_locks",
    )
    if (
        not isinstance(snapshot, dict)
        or set(snapshot) != {*active_fields, "observed_at"}
        or any(not isinstance(snapshot.get(field), list) for field in active_fields)
    ):
        raise AdmissionError("cutover quiescence snapshot schema differs")
    _parse_cutover_timestamp(snapshot.get("observed_at"), label="DB observation time")
    blockers = {field: snapshot[field] for field in active_fields if snapshot[field]}
    if blockers:
        raise AdmissionError(
            "shared scheduler cutover is not quiescent: "
            + ", ".join(
                f"{field}={len(records)}" for field, records in blockers.items()
            )
        )
    return {
        **snapshot,
        "postgres_container_id": container_id,
        "schema_version": 1,
        "status": "cutover-quiescent-v1",
    }


def verify_create_vacancy(
    service: str,
    *,
    runner: DockerRunner = _run_docker,
    now: Callable[[], datetime] = _cutover_now,
) -> dict[str, Any]:
    """Publish the empty identity boundary required before one Compose create."""

    if service not in _PROTECTED_SERVICE_SET:
        raise AdmissionError("create-vacancy service is not protected")
    project = _SERVICE_PROJECT[service]
    name_ids = _docker_id_lines(
        runner(
            (
                "container",
                "ls",
                "--all",
                "--no-trunc",
                "--filter",
                f"name=^/{service}$",
                "--format",
                "{{.ID}}",
            )
        ),
        label=f"create-vacancy exact-name inventory for {service}",
    )
    service_ids = _docker_id_lines(
        runner(
            (
                "container",
                "ls",
                "--all",
                "--no-trunc",
                "--filter",
                f"label=com.docker.compose.project={project}",
                "--filter",
                f"label=com.docker.compose.service={service}",
                "--format",
                "{{.ID}}",
            )
        ),
        label=f"create-vacancy Compose inventory for {service}",
    )
    if name_ids or service_ids:
        raise AdmissionError(f"protected create target is not vacant: {service}")
    observed_at = now().astimezone(timezone.utc)
    return {
        "observed_at": observed_at.isoformat(timespec="microseconds").replace(
            "+00:00", "Z"
        ),
        "project": project,
        "schema_version": 1,
        "service": service,
        "status": "create-vacancy-admitted-v1",
    }


def capture_created_object(
    vacancy: Mapping[str, Any],
    *,
    runner: DockerRunner = _run_docker,
    allow_partial: bool = False,
) -> dict[str, Any]:
    """Capture exact IDs immediately after one Compose create attempt."""

    if set(vacancy) != {
        "observed_at",
        "project",
        "schema_version",
        "service",
        "status",
    }:
        raise AdmissionError("create-vacancy receipt schema differs")
    service = vacancy.get("service")
    if (
        vacancy.get("schema_version") != 1
        or vacancy.get("status") != "create-vacancy-admitted-v1"
        or not isinstance(service, str)
        or service not in _PROTECTED_SERVICE_SET
        or vacancy.get("project") != _SERVICE_PROJECT[service]
    ):
        raise AdmissionError("create-vacancy receipt identity differs")
    project = _SERVICE_PROJECT[service]
    not_before = _parse_cutover_timestamp(
        vacancy.get("observed_at"), label="create-vacancy observation time"
    )
    name_ids = _docker_id_lines(
        runner(
            (
                "container",
                "ls",
                "--all",
                "--no-trunc",
                "--filter",
                f"name=^/{service}$",
                "--format",
                "{{.ID}}",
            )
        ),
        label=f"created exact-name inventory for {service}",
    )
    service_ids = _docker_id_lines(
        runner(
            (
                "container",
                "ls",
                "--all",
                "--no-trunc",
                "--filter",
                f"label=com.docker.compose.project={project}",
                "--filter",
                f"label=com.docker.compose.service={service}",
                "--format",
                "{{.ID}}",
            )
        ),
        label=f"created Compose inventory for {service}",
    )
    if name_ids != service_ids or len(name_ids) > 1:
        raise AdmissionError(f"created container identity is ambiguous: {service}")
    if not name_ids and not allow_partial:
        raise AdmissionError(f"created container identity is ambiguous: {service}")
    container_id: str | None = None
    created_value: str | None = None
    if name_ids:
        container_id = name_ids[0]
        container = _docker_object(
            runner(("container", "inspect", container_id)),
            label=f"created container inspect for {service}",
        )
        config = container.get("Config")
        labels = config.get("Labels") if isinstance(config, dict) else None
        state = container.get("State")
        created_at = _parse_cutover_timestamp(
            container.get("Created"), label=f"container Created for {service}"
        )
        if (
            created_at < not_before
            or container.get("Id") != container_id
            or container.get("Name") != f"/{service}"
            or not isinstance(labels, dict)
            or labels.get("com.docker.compose.project") != project
            or labels.get("com.docker.compose.service") != service
            or labels.get("com.docker.compose.container-number") != "1"
            or labels.get("com.docker.compose.oneoff") != "False"
            or not isinstance(state, dict)
            or type(state.get("Running")) is not bool
            or not isinstance(state.get("Status"), str)
            or not state["Status"]
            or not allow_partial
            and (state.get("Status") != "created" or state.get("Running") is not False)
        ):
            raise AdmissionError(
                f"new container is not an exact created object: {service}"
            )
        created_value = str(container["Created"])
    networks: list[dict[str, str]] = []
    if project == GATEWAY_PROJECT:
        network_ids = _docker_id_lines(
            runner(
                (
                    "network",
                    "ls",
                    "--no-trunc",
                    "--filter",
                    f"label=com.docker.compose.project={project}",
                    "--format",
                    "{{.ID}}",
                )
            ),
            label="created gateway-project network inventory",
        )
        expected_logical_networks = _CREATE_CAPTURE_GATEWAY_NETWORKS[service]
        expected_names = {
            _EXPECTED_NETWORK_DEFINITIONS[name]["name"]
            for name in expected_logical_networks
        }
        expected_logical_names = {
            _EXPECTED_NETWORK_DEFINITIONS[name]["name"]: name
            for name in expected_logical_networks
        }
        for network_id in network_ids:
            network = _docker_object(
                runner(("network", "inspect", network_id)),
                label="created gateway-project network inspect",
            )
            labels = network.get("Labels")
            if (
                network.get("Id") != network_id
                or network.get("Name") not in expected_names
                or not isinstance(labels, dict)
                or labels.get("com.docker.compose.project") != project
                or labels.get("com.docker.compose.network")
                != expected_logical_names.get(str(network.get("Name")))
            ):
                raise AdmissionError("created gateway network identity differs")
            networks.append({"id": network_id, "name": str(network["Name"])})
        observed_names = {record["name"] for record in networks}
        if len(observed_names) != len(networks) or (
            observed_names != expected_names
            if not allow_partial
            else not observed_names.issubset(expected_names)
        ):
            raise AdmissionError("gateway create did not publish exact owned networks")
    return {
        "container_id": container_id,
        "created_at": created_value,
        "networks": sorted(networks, key=lambda item: item["name"]),
        "project": project,
        "schema_version": 1,
        "service": service,
        "status": "created-object-captured-v1",
    }


def _validated_cutover_input_evidence(
    path: Path, *, reader: CutoverInputReader
) -> dict[str, Any]:
    evidence = reader(path)
    integer_fields = {
        "changed_ns",
        "device",
        "gid",
        "inode",
        "link_count",
        "mode",
        "modified_ns",
        "size",
        "uid",
    }
    if (
        not isinstance(evidence, dict)
        or set(evidence) != {*integer_fields, "path", "sha256"}
        or evidence.get("path") != str(path)
        or not isinstance(evidence.get("sha256"), str)
        or _DIGEST.fullmatch(evidence["sha256"]) is None
        or any(
            isinstance(evidence.get(field), bool)
            or not isinstance(evidence.get(field), int)
            or evidence[field] < 0
            for field in integer_fields
        )
        or evidence["uid"] != 0
        or evidence["link_count"] != 1
        or evidence["mode"] & 0o022
    ):
        raise AdmissionError(f"legacy rollback input evidence is invalid: {path}")
    return evidence


def _validated_cutover_directory_evidence(
    path: Path, *, reader: CutoverDirectoryReader
) -> dict[str, Any]:
    evidence = reader(path)
    integer_fields = {
        "changed_ns",
        "device",
        "gid",
        "inode",
        "mode",
        "modified_ns",
        "uid",
    }
    if (
        not isinstance(evidence, dict)
        or set(evidence) != {*integer_fields, "path"}
        or evidence.get("path") != str(path)
        or any(
            isinstance(evidence.get(field), bool)
            or not isinstance(evidence.get(field), int)
            or evidence[field] < 0
            for field in integer_fields
        )
        or evidence["uid"] != 0
        or evidence["mode"] & 0o022
    ):
        raise AdmissionError("legacy rollback working-directory evidence is invalid")
    return evidence


def _verify_legacy_rollback_model(
    containers: Mapping[str, Mapping[str, Any]],
    *,
    runner: DockerRunner,
    input_reader: CutoverInputReader,
    directory_reader: CutoverDirectoryReader,
) -> dict[str, Any]:
    """Bind rollback to protected old Compose bytes and locally present images."""

    rollback_services: list[dict[str, Any]] = []
    for service in _LEGACY_CUTOVER_SERVICES:
        if service not in containers:
            continue
        record = containers[service]
        config_files = tuple(record["config_files"])
        environment_files = tuple(record["environment_files"])
        working_directory = record["working_directory"]
        profiles = ("whoscored-paid",) if service in GATEWAY_PROTECTED_SERVICES else ()
        input_paths = (*config_files, *environment_files)
        before = {
            path: _validated_cutover_input_evidence(path, reader=input_reader)
            for path in input_paths
        }
        working_directory_before = _validated_cutover_directory_evidence(
            working_directory, reader=directory_reader
        )
        prefix = _cutover_compose_prefix(
            config_files=config_files,
            environment_files=environment_files,
            profiles=profiles,
            working_directory=working_directory,
        )
        try:
            rendered = json.loads(
                runner((*prefix, "config", "--format", "json")).decode("utf-8"),
                object_pairs_hook=_unique_object,
            )
        except (
            _DuplicateKey,
            UnicodeDecodeError,
            json.JSONDecodeError,
        ) as exc:
            raise AdmissionError(
                f"legacy rollback Compose model is invalid JSON: {service}"
            ) from exc
        services = rendered.get("services") if isinstance(rendered, dict) else None
        networks = rendered.get("networks") if isinstance(rendered, dict) else None
        if (
            not isinstance(rendered, dict)
            or rendered.get("name") != COMMON_PROJECT
            or not isinstance(services, dict)
            or service not in services
            or not isinstance(networks, dict)
        ):
            raise AdmissionError(f"legacy rollback Compose model differs: {service}")
        required_networks = (
            (*_LEGACY_CUTOVER_RETAINED_NETWORKS, "whoscored-paid-api")
            if service == "airflow-scheduler"
            else _LEGACY_CUTOVER_NETWORKS
            if service in GATEWAY_PROTECTED_SERVICES
            else ("backend",)
            if service == "flaresolverr"
            else ()
        )
        for logical_name in required_networks:
            network = networks.get(logical_name)
            definition = _EXPECTED_NETWORK_DEFINITIONS[logical_name]
            if network != definition:
                raise AdmissionError(
                    f"legacy rollback network model differs: {service} {logical_name}"
                )
        try:
            raw_hash = runner((*prefix, "config", "--hash", service)).decode("ascii")
        except UnicodeDecodeError as exc:
            raise AdmissionError(
                f"legacy rollback Compose hash is not ASCII: {service}"
            ) from exc
        fields = raw_hash.strip().split()
        if fields != [service, record["config_hash"]]:
            raise AdmissionError(f"legacy rollback Compose hash differs: {service}")
        image = _docker_object(
            runner(("image", "inspect", str(record["image"]))),
            label=f"legacy rollback image inspect for {service}",
        )
        repo_digests = image.get("RepoDigests")
        if (
            image.get("Id") != record["image_id"]
            or not isinstance(repo_digests, list)
            or record["image"] not in repo_digests
        ):
            raise AdmissionError(
                f"legacy rollback image is not locally digest-bound: {service}"
            )
        after = {
            path: _validated_cutover_input_evidence(path, reader=input_reader)
            for path in input_paths
        }
        working_directory_after = _validated_cutover_directory_evidence(
            working_directory, reader=directory_reader
        )
        if before != after or working_directory_before != working_directory_after:
            raise AdmissionError(
                f"legacy rollback Compose inputs changed during preflight: {service}"
            )
        rollback_services.append(
            {
                "compose": {
                    "config_files": [str(path) for path in config_files],
                    "environment_files": [str(path) for path in environment_files],
                    "profiles": list(profiles),
                    "project": COMMON_PROJECT,
                    "working_directory": str(working_directory),
                    "working_directory_evidence": working_directory_before,
                },
                "config_hash": str(record["config_hash"]),
                "image": str(record["image"]),
                "image_id": str(record["image_id"]),
                "inputs": [before[path] for path in input_paths],
                "running": bool(record["running"]),
                "service": service,
            }
        )
    return {"services": rollback_services}


def verify_rollback_bundle(
    inventory: Mapping[str, Any],
    *,
    runner: DockerRunner = _run_docker,
    input_reader: CutoverInputReader = _cutover_input_evidence,
    directory_reader: CutoverDirectoryReader = _cutover_directory_evidence,
    post_cleanup: bool = False,
) -> dict[str, Any]:
    """Repeat every immutable rollback check immediately before recreation."""

    if (
        not isinstance(inventory, dict)
        or set(inventory)
        != {
            "containers",
            "mode",
            "networks",
            "retained_networks",
            "rollback",
            "schema_version",
            "status",
        }
        or inventory.get("schema_version") != 1
        or inventory.get("status") != "cutover-inventory-admitted-v1"
        or inventory.get("mode") not in {"scheduler-only-v1", "full-legacy-v1"}
    ):
        raise AdmissionError("legacy rollback inventory schema differs")
    rollback = inventory.get("rollback")
    services = rollback.get("services") if isinstance(rollback, dict) else None
    if (
        not isinstance(rollback, dict)
        or set(rollback) != {"services"}
        or not isinstance(services, list)
    ):
        raise AdmissionError("legacy rollback bundle schema differs")
    inventory_containers = inventory.get("containers")
    if not isinstance(inventory_containers, list) or any(
        not isinstance(record, dict)
        or set(record) != {"container_id", "running", "service"}
        or not isinstance(record.get("container_id"), str)
        or _CONTAINER_ID.fullmatch(record["container_id"]) is None
        or type(record.get("running")) is not bool
        or record.get("service") not in _LEGACY_CUTOVER_SERVICES
        for record in inventory_containers
    ):
        raise AdmissionError("legacy rollback container inventory differs")
    inventory_states = {
        str(record["service"]): bool(record["running"])
        for record in inventory_containers
    }
    inventory_ids = {
        str(record["service"]): str(record["container_id"])
        for record in inventory_containers
    }
    expected_services = (
        {"airflow-scheduler", "flaresolverr"}
        if inventory["mode"] == "scheduler-only-v1"
        else set(_LEGACY_CUTOVER_SERVICES)
    )
    if set(inventory_states) != expected_services or len(inventory_states) != len(
        inventory_containers
    ):
        raise AdmissionError("legacy rollback service set differs from cutover mode")
    expected_paid_networks = (
        ("whoscored-paid-api",)
        if inventory["mode"] == "scheduler-only-v1"
        else _LEGACY_CUTOVER_NETWORKS
    )
    network_ids: set[str] = set()
    for field, expected_logical_names in (
        ("retained_networks", _LEGACY_CUTOVER_RETAINED_NETWORKS),
        ("networks", expected_paid_networks),
    ):
        network_records = inventory.get(field)
        if (
            not isinstance(network_records, list)
            or len(network_records) != len(expected_logical_names)
            or [
                record.get("logical_name")
                for record in network_records
                if isinstance(record, dict)
            ]
            != list(expected_logical_names)
        ):
            raise AdmissionError("legacy rollback network inventory differs")
        for record in network_records:
            logical_name = record.get("logical_name")
            try:
                subnet = ipaddress.ip_network(record.get("subnet"), strict=True)
            except (TypeError, ValueError) as exc:
                raise AdmissionError(
                    "legacy rollback network inventory differs"
                ) from exc
            if (
                set(record) != {"id", "logical_name", "name", "subnet"}
                or logical_name not in expected_logical_names
                or record.get("name")
                != _EXPECTED_NETWORK_DEFINITIONS[str(logical_name)]["name"]
                or not isinstance(record.get("id"), str)
                or _CONTAINER_ID.fullmatch(record["id"]) is None
                or record["id"] in network_ids
                or subnet.version != 4
                or not subnet.is_private
            ):
                raise AdmissionError("legacy rollback network inventory differs")
            network_ids.add(record["id"])
            if field == "retained_networks":
                member_services = _LEGACY_CUTOVER_RETAINED_MEMBERS[str(logical_name)]
                current_network = _verify_docker_network(
                    logical_name=str(logical_name),
                    project=COMMON_PROJECT,
                    runner=runner,
                    required_containers={
                        inventory_ids[service]: service
                        for service in member_services
                        if not post_cleanup and inventory_states[service]
                    },
                    optional_containers={
                        inventory_ids[service]: service
                        for service in member_services
                        if post_cleanup and inventory_states[service]
                    },
                    forbidden_container_ids=frozenset(
                        inventory_ids[service]
                        for service in member_services
                        if not inventory_states[service]
                    ),
                )
                if current_network != record:
                    raise AdmissionError(
                        "retained Docker network changed after cutover admission"
                    )
    pseudo_containers: dict[str, dict[str, Any]] = {}
    for record in services:
        compose = record.get("compose") if isinstance(record, dict) else None
        inputs = record.get("inputs") if isinstance(record, dict) else None
        service = record.get("service") if isinstance(record, dict) else None
        expected_profiles = (
            ["whoscored-paid"] if service in GATEWAY_PROTECTED_SERVICES else []
        )
        if (
            not isinstance(record, dict)
            or set(record)
            != {
                "compose",
                "config_hash",
                "image",
                "image_id",
                "inputs",
                "running",
                "service",
            }
            or service not in expected_services
            or not isinstance(compose, dict)
            or set(compose)
            != {
                "config_files",
                "environment_files",
                "profiles",
                "project",
                "working_directory",
                "working_directory_evidence",
            }
            or compose.get("project") != COMMON_PROJECT
            or compose.get("profiles") != expected_profiles
            or not isinstance(compose.get("config_files"), list)
            or not isinstance(compose.get("environment_files"), list)
            or any(
                not isinstance(path, str)
                for path in (
                    *compose.get("config_files", []),
                    *compose.get("environment_files", []),
                )
            )
            or not isinstance(compose.get("working_directory"), str)
            or not isinstance(inputs, list)
            or not isinstance(record.get("config_hash"), str)
            or _CONFIG_HASH.fullmatch(record["config_hash"]) is None
            or not isinstance(record.get("image"), str)
            or _PINNED_IMAGE.fullmatch(record["image"]) is None
            or not isinstance(record.get("image_id"), str)
            or _IMAGE_ID.fullmatch(record["image_id"]) is None
            or type(record.get("running")) is not bool
            or record["running"] is not inventory_states[str(record["service"])]
        ):
            raise AdmissionError("legacy rollback service evidence differs")
        config_files = _cutover_path_list(
            ",".join(compose["config_files"]), label="config-file list"
        )
        environment_files = _cutover_path_list(
            ",".join(compose["environment_files"]), label="environment-file list"
        )
        working_directory_raw = compose["working_directory"]
        working_directory = Path(working_directory_raw)
        if (
            not working_directory.is_absolute()
            or "," in working_directory_raw
            or _has_control_characters(working_directory_raw)
            or set(config_files) & set(environment_files)
        ):
            raise AdmissionError("legacy rollback Compose paths differ")
        service = str(service)
        if service in pseudo_containers:
            raise AdmissionError("legacy rollback service evidence is duplicated")
        pseudo_containers[service] = {
            **record,
            "config_files": config_files,
            "environment_files": environment_files,
            "working_directory": working_directory,
        }
    if set(pseudo_containers) != expected_services:
        raise AdmissionError("legacy rollback service evidence is incomplete")
    current = _verify_legacy_rollback_model(
        pseudo_containers,
        runner=runner,
        input_reader=input_reader,
        directory_reader=directory_reader,
    )
    if current != rollback:
        raise AdmissionError("legacy rollback bundle changed after cutover admission")
    return {
        "inventory_sha256": hashlib.sha256(_canonical_bytes(inventory)).hexdigest(),
        "schema_version": 1,
        "services": [record["service"] for record in services],
        "status": "rollback-bundle-admitted-v1",
    }


def _legacy_cutover_container_running_state(state: Any, *, service: str) -> bool:
    """Return the admitted legacy running flag only for a sane Docker state."""

    unhealthy_flags = ("Paused", "Restarting", "Dead", "OOMKilled")
    if (
        not isinstance(state, dict)
        or type(state.get("Running")) is not bool
        or any(type(state.get(field)) is not bool for field in unhealthy_flags)
    ):
        raise AdmissionError(f"legacy cutover container state differs: {service}")
    if state["Running"]:
        health = state.get("Health")
        if (
            state.get("Status") != "running"
            or any(state[field] for field in unhealthy_flags)
            or not isinstance(health, dict)
            or health.get("Status") != "healthy"
        ):
            raise AdmissionError(
                f"legacy cutover container is not healthy and running: {service}"
            )
        return True
    if state.get("Status") not in {"created", "exited"} or any(
        state[field] for field in unhealthy_flags
    ):
        raise AdmissionError(f"legacy cutover stopped container is not sane: {service}")
    return False


def verify_cutover_inventory(
    *,
    runner: DockerRunner = _run_docker,
    input_reader: CutoverInputReader = _cutover_input_evidence,
    directory_reader: CutoverDirectoryReader = _cutover_directory_evidence,
) -> dict[str, Any]:
    """Admit only an exact legacy split-project migration inventory."""

    gateway_project_ids = _docker_id_lines(
        runner(
            (
                "container",
                "ls",
                "--all",
                "--no-trunc",
                "--filter",
                f"label=com.docker.compose.project={GATEWAY_PROJECT}",
                "--format",
                "{{.ID}}",
            )
        ),
        label="gateway-project container inventory",
    )
    if gateway_project_ids:
        raise AdmissionError("whoscored-gw already has containers before cutover")

    containers: dict[str, dict[str, Any]] = {}
    for service in _LEGACY_CUTOVER_SERVICES:
        name_ids = _docker_id_lines(
            runner(
                (
                    "container",
                    "ls",
                    "--all",
                    "--no-trunc",
                    "--filter",
                    f"name=^/{service}$",
                    "--format",
                    "{{.ID}}",
                )
            ),
            label=f"exact-name container inventory for {service}",
        )
        service_ids = _docker_id_lines(
            runner(
                (
                    "container",
                    "ls",
                    "--all",
                    "--no-trunc",
                    "--filter",
                    f"label=com.docker.compose.project={COMMON_PROJECT}",
                    "--filter",
                    f"label=com.docker.compose.service={service}",
                    "--format",
                    "{{.ID}}",
                )
            ),
            label=f"service-label container inventory for {service}",
        )
        if name_ids != service_ids or len(name_ids) > 1:
            raise AdmissionError(
                f"legacy cutover container identity is ambiguous: {service}"
            )
        if not name_ids:
            continue
        container_id = name_ids[0]
        container = _docker_object(
            runner(("container", "inspect", container_id)),
            label=f"legacy container inspect for {service}",
        )
        config = container.get("Config")
        state = container.get("State")
        network_settings = container.get("NetworkSettings")
        labels = config.get("Labels") if isinstance(config, dict) else None
        attached_networks = (
            network_settings.get("Networks")
            if isinstance(network_settings, dict)
            else None
        )
        config_files = _cutover_path_list(
            labels.get("com.docker.compose.project.config_files")
            if isinstance(labels, dict)
            else None,
            label="config-file list",
        )
        environment_files = _cutover_path_list(
            labels.get("com.docker.compose.project.environment_file")
            if isinstance(labels, dict)
            else None,
            label="environment-file list",
        )
        working_directory_raw = (
            labels.get("com.docker.compose.project.working_dir")
            if isinstance(labels, dict)
            else None
        )
        config_hash = (
            labels.get("com.docker.compose.config-hash")
            if isinstance(labels, dict)
            else None
        )
        compose_version = (
            labels.get("com.docker.compose.version")
            if isinstance(labels, dict)
            else None
        )
        image = config.get("Image") if isinstance(config, dict) else None
        image_id = container.get("Image")
        version_match = (
            _COMPOSE_VERSION.fullmatch(compose_version)
            if isinstance(compose_version, str)
            else None
        )
        working_directory = (
            Path(working_directory_raw)
            if isinstance(working_directory_raw, str)
            else Path()
        )
        running = _legacy_cutover_container_running_state(state, service=service)
        if (
            container.get("Id") != container_id
            or container.get("Name") != f"/{service}"
            or not isinstance(labels, dict)
            or labels.get("com.docker.compose.project") != COMMON_PROJECT
            or labels.get("com.docker.compose.service") != service
            or labels.get("com.docker.compose.container-number") != "1"
            or labels.get("com.docker.compose.oneoff") != "False"
            or not isinstance(attached_networks, dict)
            or not isinstance(config_hash, str)
            or _CONFIG_HASH.fullmatch(config_hash) is None
            or version_match is None
            or tuple(int(version_match.group(index)) for index in (1, 2, 3))
            < (2, 24, 4)
            or not isinstance(working_directory_raw, str)
            or not working_directory.is_absolute()
            or "," in working_directory_raw
            or _has_control_characters(working_directory_raw)
            or set(config_files) & set(environment_files)
            or not isinstance(image, str)
            or _PINNED_IMAGE.fullmatch(image) is None
            or not isinstance(image_id, str)
            or _IMAGE_ID.fullmatch(image_id) is None
        ):
            raise AdmissionError(
                f"legacy cutover container metadata differs: {service}"
            )
        containers[service] = {
            "attached_networks": dict(attached_networks),
            "container_id": container_id,
            "config_files": config_files,
            "config_hash": config_hash,
            "environment_files": environment_files,
            "image": image,
            "image_id": image_id,
            "running": running,
            "working_directory": working_directory,
        }

    if not {"airflow-scheduler", "flaresolverr"}.issubset(containers):
        raise AdmissionError(
            "legacy cutover requires exactly one shared scheduler and FlareSolverr"
        )
    trio = tuple(
        service for service in GATEWAY_PROTECTED_SERVICES if service in containers
    )
    if not trio:
        mode = "scheduler-only-v1"
        expected_network_members = {
            "whoscored-paid-api": ("airflow-scheduler",),
        }
    elif trio == GATEWAY_PROTECTED_SERVICES:
        mode = "full-legacy-v1"
        expected_network_members = _LEGACY_CUTOVER_FULL_MEMBERS
    else:
        raise AdmissionError("legacy paid trio inventory is partial")

    paid_network_names = {
        _EXPECTED_NETWORK_DEFINITIONS[name]["name"] for name in _LEGACY_CUTOVER_NETWORKS
    }
    for service, record in containers.items():
        attached = record["attached_networks"]
        expected = {
            _EXPECTED_NETWORK_DEFINITIONS[logical_name]["name"]
            for logical_name, members in expected_network_members.items()
            if service in members
        }
        observed = set(attached) & paid_network_names
        if observed != expected:
            raise AdmissionError(f"legacy paid-network attachment differs: {service}")
        expected_all = (
            expected | {"dp-backend", "dp-frontend", "dp-storage"}
            if service == "airflow-scheduler"
            else {"dp-backend"}
            if service == "flaresolverr"
            else expected
        )
        if set(attached) != expected_all:
            raise AdmissionError(
                f"legacy cutover service has an unexpected network: {service}"
            )

    gateway_network_ids = _docker_id_lines(
        runner(
            (
                "network",
                "ls",
                "--no-trunc",
                "--filter",
                f"label=com.docker.compose.project={GATEWAY_PROJECT}",
                "--format",
                "{{.ID}}",
            )
        ),
        label="gateway-project network inventory",
    )
    if gateway_network_ids:
        raise AdmissionError("whoscored-gw already has networks before cutover")
    listed_networks = _listed_cutover_networks(
        runner(
            (
                "network",
                "ls",
                "--no-trunc",
                "--format",
                "{{.ID}}\t{{.Name}}",
            )
        )
    )
    expected_network_inventory = {
        *_LEGACY_CUTOVER_RETAINED_NETWORKS,
        *expected_network_members,
    }
    if set(listed_networks) != expected_network_inventory:
        raise AdmissionError("legacy paid network inventory differs from cutover mode")
    for logical_name in _LEGACY_CUTOVER_ALL_NETWORKS:
        labeled_ids = _docker_id_lines(
            runner(
                (
                    "network",
                    "ls",
                    "--no-trunc",
                    "--filter",
                    f"label=com.docker.compose.project={COMMON_PROJECT}",
                    "--filter",
                    f"label=com.docker.compose.network={logical_name}",
                    "--format",
                    "{{.ID}}",
                )
            ),
            label=f"Compose-label network inventory for {logical_name}",
        )
        expected_id = listed_networks.get(logical_name)
        if labeled_ids != ((expected_id,) if expected_id is not None else ()):
            raise AdmissionError(
                f"legacy cutover network identity is ambiguous: {logical_name}"
            )

    retained_networks: list[dict[str, str]] = []
    for logical_name in _LEGACY_CUTOVER_RETAINED_NETWORKS:
        member_services = _LEGACY_CUTOVER_RETAINED_MEMBERS[logical_name]
        network = _verify_docker_network(
            logical_name=logical_name,
            project=COMMON_PROJECT,
            runner=runner,
            required_containers={
                str(containers[service]["container_id"]): service
                for service in member_services
                if bool(containers[service]["running"])
            },
            forbidden_container_ids=frozenset(
                str(containers[service]["container_id"])
                for service in member_services
                if not bool(containers[service]["running"])
            ),
        )
        if network["id"] != listed_networks[logical_name]:
            raise AdmissionError(
                f"cutover network changed during inspection: {logical_name}"
            )
        retained_networks.append(network)

    verified_networks: list[dict[str, str]] = []
    for logical_name, services in expected_network_members.items():
        expected_member_ids = {
            str(containers[service]["container_id"]): service for service in services
        }
        configured_member_ids = _docker_id_lines(
            runner(
                (
                    "container",
                    "ls",
                    "--all",
                    "--no-trunc",
                    "--filter",
                    f"network={_EXPECTED_NETWORK_DEFINITIONS[logical_name]['name']}",
                    "--format",
                    "{{.ID}}",
                )
            ),
            label=f"configured network membership for {logical_name}",
        )
        if set(configured_member_ids) != set(expected_member_ids):
            raise AdmissionError(
                f"configured Docker network membership differs: {logical_name}"
            )
        running_members = {
            container_id: service
            for container_id, service in expected_member_ids.items()
            if bool(containers[service]["running"])
        }
        network = _verify_docker_network(
            logical_name=logical_name,
            project=COMMON_PROJECT,
            runner=runner,
            expected_containers=running_members,
        )
        if network["id"] != listed_networks[logical_name]:
            raise AdmissionError(
                f"cutover network changed during inspection: {logical_name}"
            )
        verified_networks.append(network)

    network_ids_by_name = {
        record["name"]: record["id"]
        for record in (*retained_networks, *verified_networks)
    }
    for service, record in containers.items():
        for network_name, endpoint in record["attached_networks"].items():
            if not isinstance(endpoint, dict) or endpoint.get(
                "NetworkID"
            ) != network_ids_by_name.get(network_name):
                raise AdmissionError(
                    f"legacy cutover network endpoint identity differs: {service}"
                )

    rollback = _verify_legacy_rollback_model(
        containers,
        runner=runner,
        input_reader=input_reader,
        directory_reader=directory_reader,
    )
    return {
        "containers": [
            {
                "container_id": str(containers[service]["container_id"]),
                "running": bool(containers[service]["running"]),
                "service": service,
            }
            for service in _LEGACY_CUTOVER_SERVICES
            if service in containers
        ],
        "mode": mode,
        "networks": verified_networks,
        "retained_networks": retained_networks,
        "rollback": rollback,
        "schema_version": 1,
        "status": "cutover-inventory-admitted-v1",
    }


def _verify_soccerdata_volume(*, project: str, runner: DockerRunner) -> dict[str, str]:
    name = "soccerdata_cache"
    volume = _docker_object(
        runner(("volume", "inspect", name)),
        label="volume inspect for soccerdata_cache",
    )
    mountpoint = "/var/lib/docker/volumes/soccerdata_cache/_data"
    if (
        volume.get("Name") != name
        or volume.get("Driver") != "local"
        or volume.get("Scope") != "local"
        or volume.get("Options") is not None
        or volume.get("Mountpoint") != mountpoint
    ):
        raise AdmissionError("Docker soccerdata volume policy differs")
    labels = volume.get("Labels")
    if not isinstance(labels, dict) or set(labels) != {
        "com.docker.compose.config-hash",
        "com.docker.compose.project",
        "com.docker.compose.version",
        "com.docker.compose.volume",
    }:
        raise AdmissionError("Docker soccerdata volume labels differ")
    version = labels.get("com.docker.compose.version")
    version_match = (
        _COMPOSE_VERSION.fullmatch(version) if isinstance(version, str) else None
    )
    config_hash = labels.get("com.docker.compose.config-hash")
    if (
        labels.get("com.docker.compose.project") != project
        or labels.get("com.docker.compose.volume") != name
        or not isinstance(config_hash, str)
        or _CONFIG_HASH.fullmatch(config_hash) is None
        or version_match is None
        or tuple(int(version_match.group(index)) for index in (1, 2, 3)) < (2, 24, 4)
    ):
        raise AdmissionError("Docker soccerdata volume identity differs")
    return {"driver": "local", "mountpoint": mountpoint, "name": name}


def verify_created_containers(
    bindings: Mapping[str, str],
    *,
    project: str | Mapping[str, str],
    selected_services: Sequence[str],
    projections: Mapping[str, Mapping[str, Any]],
    config_hashes: Mapping[str, str],
    config_files: Sequence[Path] | Mapping[str, Sequence[Path]],
    env_files: Sequence[Path],
    runner: DockerRunner = _run_docker,
    expected_state: str = "created",
) -> dict[str, Any]:
    if set(bindings) != _PROTECTED_SERVICE_SET:
        raise AdmissionError("post-create bindings omit a protected service")
    split_projects = isinstance(project, Mapping)
    if split_projects:
        service_projects = dict(project)
        if service_projects != _SERVICE_PROJECT:
            raise AdmissionError("split Compose project ownership differs")
        if not isinstance(config_files, Mapping) or set(config_files) != {
            COMMON_PROJECT,
            GATEWAY_PROJECT,
        }:
            raise AdmissionError("split Compose file ownership differs")
        project_config_files = {
            name: tuple(paths) for name, paths in config_files.items()
        }
        project_directory = project_config_files[COMMON_PROJECT][0].parent
    else:
        if not isinstance(project, str) or _PROJECT_NAME.fullmatch(project) is None:
            raise AdmissionError("Compose project name is invalid")
        if isinstance(config_files, Mapping):
            raise AdmissionError("single-project Compose files are invalid")
        service_projects = {service: project for service in PROTECTED_SERVICES}
        project_config_files = {project: tuple(config_files)}
        project_directory = tuple(config_files)[0].parent
    if expected_state not in {"created", "running"}:
        raise AdmissionError("container admission state must be created or running")
    selected = tuple(selected_services)
    if (
        not selected
        or len(selected) != len(set(selected))
        or any(service not in _PROTECTED_SERVICE_SET for service in selected)
    ):
        raise AdmissionError(
            "post-create services must be a non-empty unique protected subset"
        )
    model_services = set(projections)
    common_only_model = not split_projects and frozenset(model_services) == frozenset(
        COMMON_PROTECTED_SERVICES
    )
    allowed_model_services = (
        {frozenset(_PROTECTED_SERVICE_SET)}
        if split_projects
        else {
            frozenset(_PROTECTED_SERVICE_SET),
            frozenset(COMMON_PROTECTED_SERVICES),
        }
    )
    if (
        model_services != set(config_hashes)
        or frozenset(model_services) not in allowed_model_services
        or not set(selected).issubset(model_services)
    ):
        raise AdmissionError("post-create model does not bind selected services")
    docker_security_options = _verify_docker_security_options(runner=runner)
    apparmor_profile = _verify_apparmor_enforcement(
        runner=runner,
        image=bindings["airflow-scheduler"],
    )
    expected_env_files = ",".join(str(path) for path in env_files)
    logical_networks = sorted(
        {
            logical_name
            for service in selected
            for logical_name in _EXPECTED_NETWORKS[service]
        }
    )
    verified_networks = {
        record["name"]: record
        for logical_name in logical_networks
        for record in (
            _verify_docker_network(
                logical_name=logical_name,
                project=(
                    _NETWORK_PROJECT[logical_name]
                    if split_projects or common_only_model
                    else service_projects[selected[0]]
                ),
                runner=runner,
            ),
        )
    }
    verified_volumes = (
        [_verify_soccerdata_volume(project=COMMON_PROJECT, runner=runner)]
        if "airflow-scheduler" in selected
        else []
    )
    verified: list[dict[str, str]] = []
    for service in selected:
        service_project = service_projects[service]
        service_config_files = project_config_files[service_project]
        expected_config_files = ",".join(str(path) for path in service_config_files)
        raw_ids = runner(
            (
                "container",
                "ls",
                "--all",
                "--no-trunc",
                "--filter",
                f"label=com.docker.compose.project={service_project}",
                "--filter",
                f"label=com.docker.compose.service={service}",
                "--format",
                "{{.ID}}",
            )
        )
        try:
            ids = raw_ids.decode("ascii").splitlines()
        except UnicodeDecodeError as exc:
            raise AdmissionError(
                f"Docker returned a non-ASCII container ID: {service}"
            ) from exc
        if len(ids) != 1 or _CONTAINER_ID.fullmatch(ids[0]) is None:
            raise AdmissionError(
                f"protected service must have exactly one full container ID: {service}"
            )
        container_id = ids[0]
        container = _docker_object(
            runner(("container", "inspect", container_id)),
            label=f"container inspect for {service}",
        )
        config = container.get("Config")
        state = container.get("State")
        image_id = container.get("Image")
        if not isinstance(config, dict) or not isinstance(state, dict):
            raise AdmissionError(f"container metadata is incomplete: {service}")
        if container.get("AppArmorProfile") != "docker-default":
            raise AdmissionError(f"container AppArmor profile differs: {service}")
        labels = config.get("Labels")
        if (
            not isinstance(labels, dict)
            or labels.get("com.docker.compose.project") != service_project
            or labels.get("com.docker.compose.service") != service
        ):
            raise AdmissionError(f"container Compose identity differs: {service}")
        if labels.get("com.docker.compose.oneoff") != "False":
            raise AdmissionError(f"container is a one-off Compose bypass: {service}")
        if labels.get("com.docker.compose.container-number") != "1":
            raise AdmissionError(f"container ordinal differs from policy: {service}")
        if (
            labels.get("com.docker.compose.project.config_files")
            != expected_config_files
        ):
            raise AdmissionError(f"container Compose file set differs: {service}")
        if (
            labels.get("com.docker.compose.project.environment_file")
            != expected_env_files
        ):
            raise AdmissionError(f"container Compose env-file set differs: {service}")
        if labels.get("com.docker.compose.project.working_dir") != str(
            project_directory
        ):
            raise AdmissionError(
                f"container Compose working directory differs: {service}"
            )
        if labels.get("com.docker.compose.config-hash") != config_hashes[service]:
            raise AdmissionError(f"container Compose config hash differs: {service}")
        if config.get("Image") != bindings[service]:
            raise AdmissionError(f"container Config.Image is not attested: {service}")
        if tuple(config.get("Entrypoint") or ()) != _EXPECTED_ENTRYPOINTS[service]:
            raise AdmissionError(
                f"container Config.Entrypoint bypasses gate: {service}"
            )
        if tuple(config.get("Cmd") or ()) != projections[service]["command"]:
            raise AdmissionError(f"container Config.Cmd differs from policy: {service}")
        healthcheck = config.get("Healthcheck")
        if not isinstance(healthcheck, dict):
            raise AdmissionError(f"container healthcheck is missing: {service}")
        actual_healthcheck = {
            "Interval": healthcheck.get("Interval"),
            "Retries": healthcheck.get("Retries"),
            "StartPeriod": healthcheck.get("StartPeriod"),
            "Test": tuple(healthcheck.get("Test") or ()),
            "Timeout": healthcheck.get("Timeout"),
        }
        unexpected_healthcheck = set(healthcheck) - {
            *actual_healthcheck,
            "StartInterval",
        }
        if (
            unexpected_healthcheck
            or healthcheck.get("StartInterval", 0) != 0
            or actual_healthcheck != projections[service]["healthcheck"]
        ):
            raise AdmissionError(f"container healthcheck policy differs: {service}")
        if not isinstance(image_id, str) or _IMAGE_ID.fullmatch(image_id) is None:
            raise AdmissionError(
                f"container .Image is not an immutable image ID: {service}"
            )
        if expected_state == "created":
            if state.get("Status") != "created" or state.get("Running") is not False:
                raise AdmissionError(
                    "protected container was started before post-create admission: "
                    f"{service}"
                )
        else:
            unhealthy_flags = ("Paused", "Restarting", "Dead", "OOMKilled")
            health = state.get("Health")
            if (
                state.get("Status") != "running"
                or state.get("Running") is not True
                or any(state.get(field) is not False for field in unhealthy_flags)
                or not isinstance(health, dict)
                or health.get("Status") != "healthy"
            ):
                raise AdmissionError(
                    f"protected container is not healthy and running: {service}"
                )
        image = _docker_object(
            runner(("image", "inspect", bindings[service])),
            label=f"image inspect for {service}",
        )
        if image.get("Id") != image_id:
            raise AdmissionError(
                f"container .Image differs from digest-selected image .Id: {service}"
            )
        repo_digests = image.get("RepoDigests")
        if not isinstance(repo_digests, list) or bindings[service] not in repo_digests:
            raise AdmissionError(
                f"digest-selected image lacks its attested RepoDigest: {service}"
            )
        image_config = image.get("Config")
        if (
            not isinstance(image_config, dict)
            or tuple(image_config.get("Entrypoint") or ())
            != _EXPECTED_ENTRYPOINTS[service]
        ):
            raise AdmissionError(f"digest-selected image entrypoint differs: {service}")
        image_user = image_config.get("User")
        if (
            image_user != _EXPECTED_IMAGE_USER[service]
            or config.get("User") != image_user
        ):
            raise AdmissionError(
                f"container user differs from non-root image policy: {service}"
            )
        image_working_dir = image_config.get("WorkingDir")
        if (
            image_working_dir != _EXPECTED_WORKING_DIR[service]
            or config.get("WorkingDir") != image_working_dir
        ):
            raise AdmissionError(f"container working directory differs: {service}")
        _verify_container_labels(
            labels,
            image_config.get("Labels"),
            service=service,
            project=service_project,
            config_hash=config_hashes[service],
            image_id=image_id,
            config_files=expected_config_files,
            env_files=expected_env_files,
            working_dir=str(project_directory),
        )
        expected_environment = _environment_mapping(
            image_config.get("Env"), label=f"image environment for {service}"
        )
        if any(
            expected_environment.get(name) != value
            for name, value in _EXPECTED_IMAGE_ENVIRONMENT[service].items()
        ):
            raise AdmissionError(f"image hardening environment differs: {service}")
        rendered_environment = projections[service]["environment"]
        if any(
            name in expected_environment and expected_environment[name] != value
            for name, value in rendered_environment.items()
        ):
            raise AdmissionError(
                f"rendered environment overrides image policy: {service}"
            )
        expected_environment.update(rendered_environment)
        if _forbidden_environment_names(expected_environment, include_empty=False):
            raise AdmissionError(f"image environment has loader controls: {service}")
        actual_environment = _environment_mapping(
            config.get("Env"), label=f"container environment for {service}"
        )
        if actual_environment != expected_environment:
            raise AdmissionError(f"container environment projection differs: {service}")
        if config.get("OpenStdin") is not False or config.get("Tty") is not False:
            raise AdmissionError(f"container interactive mode differs: {service}")
        host_config = container.get("HostConfig")
        if not isinstance(host_config, dict):
            raise AdmissionError(f"container HostConfig is incomplete: {service}")
        projection = projections[service]
        if host_config.get("Privileged") is not False:
            raise AdmissionError(f"container is privileged: {service}")
        if host_config.get("ReadonlyRootfs") is not projection["read_only"]:
            raise AdmissionError(f"container root filesystem policy differs: {service}")
        cap_add = _normal_capabilities(
            host_config.get("CapAdd"), label=f"container CapAdd for {service}"
        )
        cap_drop = _normal_capabilities(
            host_config.get("CapDrop"), label=f"container CapDrop for {service}"
        )
        security_opt = (
            _string_sequence(
                host_config.get("SecurityOpt"),
                label=f"container SecurityOpt for {service}",
            )
            or ()
        )
        if cap_add != projection["cap_add"] or cap_drop != projection["cap_drop"]:
            raise AdmissionError(f"container capability policy differs: {service}")
        if security_opt != projection["security_opt"]:
            raise AdmissionError(f"container security options differ: {service}")
        for field in (
            "DeviceCgroupRules",
            "Devices",
            "DeviceRequests",
            "GroupAdd",
            "StorageOpt",
            "Sysctls",
            "Ulimits",
            "VolumesFrom",
        ):
            if host_config.get(field):
                raise AdmissionError(f"container adds forbidden {field}: {service}")
        for field in ("PidMode", "UTSMode", "UsernsMode", "CgroupParent"):
            if host_config.get(field) not in (None, ""):
                raise AdmissionError(f"container adds forbidden {field}: {service}")
        if host_config.get("IpcMode") not in (None, "", "private"):
            raise AdmissionError(f"container IPC mode differs: {service}")
        if host_config.get("CgroupnsMode") not in (None, "", "private"):
            raise AdmissionError(f"container cgroup namespace differs: {service}")
        network_mode = host_config.get("NetworkMode")
        if network_mode != _EXPECTED_NETWORK_MODE[service]:
            raise AdmissionError(f"container network mode differs: {service}")
        network_settings = container.get("NetworkSettings")
        attached_networks = (
            network_settings.get("Networks")
            if isinstance(network_settings, dict)
            else None
        )
        if not isinstance(attached_networks, dict) or set(attached_networks) != set(
            projection["network_names"]
        ):
            raise AdmissionError(f"container network attachments differ: {service}")
        for endpoint in attached_networks.values():
            aliases = endpoint.get("Aliases") if isinstance(endpoint, dict) else None
            if (
                not isinstance(aliases, list)
                or not aliases
                or any(alias != service for alias in aliases)
            ):
                raise AdmissionError(f"container network aliases differ: {service}")
        for network_name, endpoint in attached_networks.items():
            assert isinstance(endpoint, dict)
            expected_network = verified_networks[network_name]
            address = endpoint.get("IPAddress")
            try:
                address_in_subnet = (
                    address == ""
                    or isinstance(address, str)
                    and ipaddress.ip_address(address)
                    in ipaddress.ip_network(expected_network["subnet"])
                )
            except ValueError:
                address_in_subnet = False
            if (
                endpoint.get("NetworkID") not in ("", expected_network["id"])
                or bool(address)
                and endpoint.get("NetworkID") != expected_network["id"]
                or endpoint.get("IPAMConfig") is not None
                or endpoint.get("Links") is not None
                or endpoint.get("DriverOpts") is not None
                or endpoint.get("GwPriority") != 0
                or not isinstance(endpoint.get("MacAddress"), str)
                or endpoint["MacAddress"]
                and _MAC_ADDRESS.fullmatch(endpoint["MacAddress"]) is None
                or endpoint.get("GlobalIPv6Address") not in (None, "")
                or not address_in_subnet
            ):
                raise AdmissionError(
                    f"container network endpoint policy differs: {service}"
                )
        if host_config.get("Runtime") not in (None, "", "runc"):
            raise AdmissionError(f"container runtime differs: {service}")
        if host_config.get("AutoRemove") is not False:
            raise AdmissionError(f"container auto-remove policy differs: {service}")
        if host_config.get("PublishAllPorts") is not False:
            raise AdmissionError(f"container publishes unmodeled ports: {service}")
        if (
            any(
                host_config.get(field) is not None
                for field in ("Dns", "DnsOptions", "DnsSearch", "Links")
            )
            or host_config.get("ExtraHosts") != []
        ):
            raise AdmissionError(f"container DNS/host-link policy differs: {service}")
        if host_config.get("LogConfig") != {"Config": {}, "Type": "json-file"}:
            raise AdmissionError(f"container logging policy differs: {service}")
        if tuple(host_config.get("MaskedPaths") or ()) != _EXPECTED_MASKED_PATHS:
            raise AdmissionError(f"container masked-path policy differs: {service}")
        if tuple(host_config.get("ReadonlyPaths") or ()) != _EXPECTED_READONLY_PATHS:
            raise AdmissionError(f"container readonly-path policy differs: {service}")
        if host_config.get("Init") is not None:
            raise AdmissionError(f"container init policy differs: {service}")
        oom_kill_disable = host_config.get("OomKillDisable")
        if (
            oom_kill_disable is not None and oom_kill_disable is not False
        ) or host_config.get("OomScoreAdj", 0) != 0:
            raise AdmissionError(f"container OOM policy differs: {service}")
        if host_config.get("ShmSize") != projection["shm_size"]:
            raise AdmissionError(f"container shm_size differs: {service}")
        if any(
            host_config.get(field) != expected
            for field, expected in _EXPECTED_CONTAINER_RESOURCES[service].items()
        ):
            raise AdmissionError(f"container resource policy differs: {service}")
        if host_config.get("RestartPolicy") != {
            "MaximumRetryCount": 0,
            "Name": "unless-stopped",
        }:
            raise AdmissionError(f"container restart policy differs: {service}")
        actual_ports = _port_bindings(
            host_config.get("PortBindings"), label=f"container ports for {service}"
        )
        if actual_ports != projection["port_bindings"]:
            raise AdmissionError(f"container port bindings differ: {service}")
        actual_mounts = container.get("Mounts")
        if not isinstance(actual_mounts, list):
            raise AdmissionError(f"container mount metadata is invalid: {service}")
        requested_mounts = host_config.get("Mounts")
        if requested_mounts is None:
            requested_mounts = []
        if not isinstance(requested_mounts, list) or any(
            not isinstance(item, dict) for item in requested_mounts
        ):
            raise AdmissionError(
                f"container requested-mount metadata is invalid: {service}"
            )
        requested_by_target: dict[str, dict[str, Any]] = {}
        for requested in requested_mounts:
            requested_target = requested.get("Target")
            if (
                not isinstance(requested_target, str)
                or not requested_target.startswith("/")
                or requested_target in requested_by_target
            ):
                raise AdmissionError(
                    f"container requested-mount identity is invalid: {service}"
                )
            requested_by_target[requested_target] = requested
        effective: list[tuple[Any, ...]] = []
        actual_tmpfs_targets: set[str] = set()
        seen_targets: set[str] = set()
        for mount in actual_mounts:
            if not isinstance(mount, dict):
                raise AdmissionError(f"container mount record is invalid: {service}")
            mount_type = mount.get("Type")
            target = mount.get("Destination")
            if (
                mount_type not in ("bind", "volume", "tmpfs")
                or not isinstance(target, str)
                or not target.startswith("/")
                or target in seen_targets
            ):
                raise AdmissionError(f"container mount identity is invalid: {service}")
            seen_targets.add(target)
            if _mount_shadows_image_path(service, target):
                raise AdmissionError(
                    f"container mount shadows image trust path: {service}"
                )
            if mount_type == "tmpfs":
                actual_tmpfs_targets.add(target)
                continue
            source_field = "Source" if mount_type == "bind" else "Name"
            source = mount.get(source_field)
            read_only = mount.get("RW") is False
            if (
                not isinstance(source, str)
                or not source
                or not isinstance(mount.get("RW"), bool)
            ):
                raise AdmissionError(f"container mount identity is invalid: {service}")
            expected_mode = "ro" if read_only else "rw"
            mount_mode = mount.get("Mode")
            if mount_mode != expected_mode:
                requested = requested_by_target.get(target)
                requested_read_only = (
                    requested.get("ReadOnly", False)
                    if isinstance(requested, dict)
                    else None
                )
                if (
                    mount_mode != ""
                    or mount_type != "bind"
                    or not isinstance(requested, dict)
                    or requested.get("Type") != "bind"
                    or requested.get("Source") != source
                    or requested.get("Target") != target
                    or not isinstance(requested_read_only, bool)
                    or requested_read_only is not read_only
                    or requested.get("BindOptions") != {}
                ):
                    raise AdmissionError(f"container mount mode differs: {service}")
            if mount_type == "bind":
                if mount.get("Propagation") != "rprivate":
                    raise AdmissionError(
                        f"container bind propagation differs: {service}"
                    )
            elif (
                mount.get("Driver") != "local"
                or mount.get("Propagation") != ""
                or not verified_volumes
                or mount.get("Source") != verified_volumes[0]["mountpoint"]
            ):
                raise AdmissionError(f"container volume driver differs: {service}")
            effective.append((mount_type, source, target, read_only))
        if tuple(sorted(effective)) != projection["volumes"]:
            raise AdmissionError(f"container mount projection differs: {service}")
        expected_tmpfs = projection["tmpfs"]
        actual_tmpfs = host_config.get("Tmpfs")
        if expected_tmpfs:
            if not isinstance(actual_tmpfs, dict) or set(actual_tmpfs) != set(
                expected_tmpfs
            ):
                raise AdmissionError(f"container tmpfs projection differs: {service}")
            for target, raw_options in actual_tmpfs.items():
                if (
                    not isinstance(raw_options, str)
                    or frozenset(raw_options.split(",")) != expected_tmpfs[target]
                ):
                    raise AdmissionError(f"container tmpfs options differ: {service}")
            if actual_tmpfs_targets and actual_tmpfs_targets != set(expected_tmpfs):
                raise AdmissionError(f"container tmpfs mounts differ: {service}")
        elif (actual_tmpfs is not None and actual_tmpfs != {}) or actual_tmpfs_targets:
            raise AdmissionError(f"container adds an unexpected tmpfs: {service}")
        verified.append(
            {
                "container_id": container_id,
                "final_image": bindings[service],
                "image_id": image_id,
                "service": service,
            }
        )
    return {
        "apparmor_profile": apparmor_profile,
        "docker_security_options": list(docker_security_options),
        "images": verified,
        "networks": list(verified_networks.values()),
        "projects": (
            {
                COMMON_PROJECT: list(COMMON_PROTECTED_SERVICES),
                GATEWAY_PROJECT: list(GATEWAY_PROTECTED_SERVICES),
            }
            if split_projects
            else {str(project): list(selected)}
        ),
        "schema_version": 2 if split_projects else 1,
        "status": (
            "admitted-running-v1" if expected_state == "running" else "admitted-v1"
        ),
        "volumes": verified_volumes,
    }


def _absolute(path: Path) -> Path:
    return Path(os.path.abspath(path))


def _assert_canonical_release(root: Path) -> None:
    if os.geteuid() != 0:
        raise AdmissionError("production admission requires effective UID 0")
    if Path(sys.executable) != _SYSTEM_PYTHON:
        raise AdmissionError("production admission requires exact /usr/bin/python3")
    if root != _REPOSITORY_ROOT:
        raise AdmissionError(
            "--root must be the canonical protected release containing this helper"
        )
    validator_path = root / "scripts/validate_whoscored_build_provenance.py"
    try:
        helper_source = provenance.read_protected_regular_file(
            _SCRIPT_PATH, label="production admission helper"
        )
        validator_source = provenance.read_protected_regular_file(
            validator_path, label="production provenance validator"
        )
    except provenance.ProvenanceError as exc:
        raise AdmissionError(str(exc)) from exc
    if not helper_source:
        raise AdmissionError("production admission helper is empty")
    loaded_validator_digest = getattr(
        provenance, "_whoscored_loaded_source_sha256", None
    )
    if not isinstance(loaded_validator_digest, str) or not hmac.compare_digest(
        hashlib.sha256(validator_source).hexdigest(), loaded_validator_digest
    ):
        raise AdmissionError("production provenance validator changed after loading")


def _assert_protected_compose_inputs(
    paths: Sequence[Path],
) -> dict[Path, ProtectedComposeInputEvidence]:
    captured: dict[Path, ProtectedComposeInputEvidence] = {}
    for path in paths:
        try:
            raw, identity = provenance.read_protected_regular_file_snapshot(
                path,
                label="production Compose admission input",
            )
        except provenance.ProvenanceError as exc:
            raise AdmissionError(str(exc)) from exc
        captured[path] = ProtectedComposeInputEvidence(raw=raw, identity=identity)
    return captured


def _assert_compose_input_evidence_unchanged(
    evidence: Mapping[Path, ProtectedComposeInputEvidence],
) -> None:
    for path, expected in evidence.items():
        try:
            raw, identity = provenance.read_protected_regular_file_snapshot(
                path,
                label="production Compose admission input",
            )
        except provenance.ProvenanceError as exc:
            raise AdmissionError(str(exc)) from exc
        if identity != expected.identity or not hmac.compare_digest(raw, expected.raw):
            raise AdmissionError(f"protected Compose input changed: {path}")


def _assert_protected_directory(path: Path, *, label: str) -> os.stat_result:
    if not path.is_absolute() or not path.name:
        raise AdmissionError(f"{label} must be an absolute directory")
    parent = -1
    descriptor = -1
    try:
        parent, name = provenance.open_protected_parent(path, label=label)
        descriptor = os.open(
            name,
            os.O_RDONLY | os.O_DIRECTORY | os.O_NOFOLLOW | os.O_CLOEXEC,
            dir_fd=parent,
        )
        before = os.fstat(descriptor)
        entry = os.stat(name, dir_fd=parent, follow_symlinks=False)
    except (OSError, provenance.ProvenanceError) as exc:
        raise AdmissionError(
            f"{label} is missing, unsafe, or symlinked: {path}"
        ) from exc
    finally:
        if descriptor >= 0:
            os.close(descriptor)
        if parent >= 0:
            os.close(parent)
    identity = ("st_dev", "st_ino", "st_mode", "st_uid", "st_mtime_ns", "st_ctime_ns")
    if (
        before.st_uid != 0
        or before.st_mode & 0o022
        or any(getattr(before, field) != getattr(entry, field) for field in identity)
    ):
        raise AdmissionError(f"{label} is not a protected directory: {path}")
    return before


def _assert_protected_regular_file(path: Path, *, label: str) -> os.stat_result:
    try:
        provenance.read_protected_regular_file(path, label=label)
        metadata = path.lstat()
    except (OSError, provenance.ProvenanceError) as exc:
        raise AdmissionError(
            f"{label} is missing, unsafe, or symlinked: {path}"
        ) from exc
    if not stat.S_ISREG(metadata.st_mode):
        raise AdmissionError(f"{label} is not a protected regular file: {path}")
    return metadata


def _assert_airflow_authority_directory(path: Path, *, label: str) -> os.stat_result:
    """Require one UID-50000 directory writable only by its container owner."""

    if not path.is_absolute() or not path.name:
        raise AdmissionError(f"{label} must be an absolute directory")
    parent = -1
    descriptor = -1
    try:
        parent, name = provenance.open_protected_parent(path, label=label)
        descriptor = os.open(
            name,
            os.O_RDONLY | os.O_DIRECTORY | os.O_NOFOLLOW | os.O_CLOEXEC,
            dir_fd=parent,
        )
        before = os.fstat(descriptor)
        entry = os.stat(name, dir_fd=parent, follow_symlinks=False)
    except (OSError, provenance.ProvenanceError) as exc:
        raise AdmissionError(
            f"{label} is missing, unsafe, or symlinked: {path}"
        ) from exc
    finally:
        if descriptor >= 0:
            os.close(descriptor)
        if parent >= 0:
            os.close(parent)
    identity = (
        "st_dev",
        "st_ino",
        "st_mode",
        "st_uid",
        "st_gid",
        "st_mtime_ns",
        "st_ctime_ns",
    )
    if (
        before.st_uid != _AIRFLOW_RUNTIME_UID
        or before.st_gid != 0
        or stat.S_IMODE(before.st_mode) not in {0o700, 0o750}
        or any(getattr(before, field) != getattr(entry, field) for field in identity)
    ):
        raise AdmissionError(
            f"{label} must be owned by {_AIRFLOW_RUNTIME_UID}:0 with mode "
            f"0700 or 0750: {path}"
        )
    return before


def _assert_scheduler_readable_regular_file(
    path: Path, *, label: str
) -> os.stat_result:
    """Require a protected file whose mode is readable by UID 50000/GID 0."""

    metadata = _assert_protected_regular_file(path, label=label)
    if metadata.st_uid == 50_000:
        readable = bool(metadata.st_mode & stat.S_IRUSR)
    elif metadata.st_gid == 0:
        readable = bool(metadata.st_mode & stat.S_IRGRP)
    else:
        readable = bool(metadata.st_mode & stat.S_IROTH)
    if not readable:
        raise AdmissionError(
            f"{label} is not readable by scheduler UID 50000/GID 0: {path}"
        )
    return metadata


def _assert_fbref_geoip_database(path: Path) -> None:
    """Require the one reviewed external GeoLite byte identity."""

    try:
        raw, identity = provenance.read_protected_regular_file_snapshot(
            path, label="FBref Camoufox GeoLite database"
        )
    except provenance.ProvenanceError as exc:
        raise AdmissionError(
            "FBref Camoufox GeoLite database is missing or unprotected"
        ) from exc
    (
        _device,
        _inode,
        mode,
        uid,
        gid,
        link_count,
        size,
        _modified_ns,
        _changed_ns,
    ) = identity
    if (
        not stat.S_ISREG(mode)
        or uid != 0
        or gid != 0
        or link_count != 1
        or stat.S_IMODE(mode) != 0o444
        or size != FBREF_CAMOUFOX_GEOIP_DATABASE_SIZE
        or len(raw) != FBREF_CAMOUFOX_GEOIP_DATABASE_SIZE
        or not hmac.compare_digest(
            hashlib.sha256(raw).hexdigest(),
            FBREF_CAMOUFOX_GEOIP_DATABASE_SHA256,
        )
    ):
        raise AdmissionError(
            "FBref Camoufox GeoLite database differs from the reviewed bytes"
        )


def _provider_receipt_now() -> datetime:
    return datetime.now(timezone.utc)


def _provider_policy_utc(value: object, *, field: str) -> datetime:
    if not isinstance(value, str):
        raise AdmissionError(f"provider policy {field} is invalid")
    try:
        parsed = datetime.fromisoformat(value.replace("Z", "+00:00"))
    except ValueError as exc:
        raise AdmissionError(f"provider policy {field} is invalid") from exc
    if parsed.tzinfo is None or parsed.utcoffset() != timedelta(0):
        raise AdmissionError(f"provider policy {field} is not UTC")
    return parsed.astimezone(timezone.utc)


def validate_provider_policy(
    path: Path, *, owner_secret_path: Path
) -> dict[str, str | int]:
    """Verify one owner-signed provider-policy-v1 without projecting its key."""

    _canonical_existing_path(path, label="provider policy")
    _canonical_existing_path(owner_secret_path, label="provider-policy owner key")
    try:
        raw = provenance.read_protected_regular_file(path, label="provider policy")
        secret_raw = provenance.read_protected_regular_file(
            owner_secret_path, label="provider-policy owner key"
        )
        secret = secret_raw.decode("utf-8").strip()
        value = json.loads(raw.decode("utf-8"), object_pairs_hook=_unique_object)
    except (
        _DuplicateKey,
        UnicodeDecodeError,
        json.JSONDecodeError,
        provenance.ProvenanceError,
    ) as exc:
        raise AdmissionError(
            "provider policy/key is not protected strict data"
        ) from exc
    if len(secret.encode("utf-8")) < 32:
        raise AdmissionError("provider-policy owner key is too short")
    if (
        not isinstance(value, dict)
        or frozenset(value) != _PROVIDER_POLICY_FIELDS
        or raw != _canonical_bytes(value)
    ):
        raise AdmissionError("provider policy is not canonical provider-policy-v1")
    unsigned = {field: value[field] for field in _PROVIDER_POLICY_UNSIGNED_FIELDS}
    digest = hashlib.sha256(_authority_canonical_bytes(unsigned)).hexdigest()
    signed_body = {**unsigned, "document_sha256": digest}
    signature = hmac.new(
        secret.encode("utf-8"),
        _authority_canonical_bytes(signed_body),
        hashlib.sha256,
    ).hexdigest()
    if (
        value.get("schema_version") != 1
        or value.get("source") != "whoscored"
        or value.get("signature_algorithm") != "hmac-sha256"
        or not isinstance(value.get("document_sha256"), str)
        or not hmac.compare_digest(str(value["document_sha256"]), digest)
        or not isinstance(value.get("signature"), str)
        or not hmac.compare_digest(str(value["signature"]), signature)
    ):
        raise AdmissionError("provider policy digest/signature is invalid")
    token = re.compile(r"\A[A-Za-z0-9][A-Za-z0-9._:-]{0,127}\Z")
    for field in ("provider_id", "order_id", "plan_id"):
        if (
            not isinstance(value.get(field), str)
            or token.fullmatch(str(value[field])) is None
        ):
            raise AdmissionError(f"provider policy {field} is invalid")
    valid_from = _provider_policy_utc(value.get("valid_from"), field="valid_from")
    valid_until = _provider_policy_utc(value.get("valid_until"), field="valid_until")
    now = _provider_receipt_now()
    if not valid_from <= now < valid_until:
        raise AdmissionError("provider policy is not active")
    caps: list[int] = []
    for field in (
        "daily_cap_bytes",
        "monthly_cap_bytes",
        "order_cap_bytes",
        "safety_cap_bytes",
        "provider_quota_bytes",
    ):
        item = value.get(field)
        if isinstance(item, bool) or not isinstance(item, int) or item <= 0:
            raise AdmissionError(f"provider policy {field} is invalid")
        caps.append(item)
    if caps != sorted(caps):
        raise AdmissionError("provider policy quota/safety caps are inconsistent")
    receipt_sha256 = value.get("receipt_sha256")
    if not isinstance(receipt_sha256, str) or _DIGEST.fullmatch(receipt_sha256) is None:
        raise AdmissionError("provider policy receipt digest is invalid")
    return {
        "daily_cap_bytes": int(value["daily_cap_bytes"]),
        "document_sha256": digest,
        "monthly_cap_bytes": int(value["monthly_cap_bytes"]),
        "order_cap_bytes": int(value["order_cap_bytes"]),
        "order_id": str(value["order_id"]),
        "plan_id": str(value["plan_id"]),
        "policy_path": str(path),
        "provider_id": str(value["provider_id"]),
        "provider_quota_bytes": int(value["provider_quota_bytes"]),
        "receipt_sha256": receipt_sha256,
        "safety_cap_bytes": int(value["safety_cap_bytes"]),
        "valid_from": str(value["valid_from"]),
        "valid_until": str(value["valid_until"]),
    }


def _read_protected_authority(path: Path, *, label: str) -> dict[str, Any]:
    _canonical_existing_path(path, label=label)
    try:
        raw = provenance.read_protected_regular_file(path, label=label)
        value = json.loads(raw.decode("utf-8"), object_pairs_hook=_unique_object)
    except (
        _DuplicateKey,
        UnicodeDecodeError,
        json.JSONDecodeError,
        provenance.ProvenanceError,
    ) as exc:
        raise AdmissionError(f"{label} is not protected strict data") from exc
    if (
        not 0 < len(raw) <= 4 * 1024 * 1024
        or not isinstance(value, dict)
        or raw != _canonical_bytes(value)
    ):
        raise AdmissionError(f"{label} is not canonical JSON")
    return value


def _read_owner_secret(path: Path) -> str:
    _canonical_existing_path(path, label="owner HMAC key")
    try:
        secret = (
            provenance.read_protected_regular_file(path, label="owner HMAC key")
            .decode("utf-8")
            .strip()
        )
    except (UnicodeDecodeError, provenance.ProvenanceError) as exc:
        raise AdmissionError("owner HMAC key is not protected UTF-8") from exc
    if len(secret.encode("utf-8")) < 32:
        raise AdmissionError("owner HMAC key is too short")
    return secret


def _authority_token(value: object, *, field: str) -> str:
    if not isinstance(value, str) or _ROLLOUT_ID.fullmatch(value) is None:
        raise AdmissionError(f"current rollout {field} is invalid")
    return value


def _authority_digest(value: object, *, field: str) -> str:
    if not isinstance(value, str) or _DIGEST.fullmatch(value) is None:
        raise AdmissionError(f"current rollout {field} is invalid")
    return value


def _scope_specs_sha256(scopes: Sequence[str]) -> str:
    return hashlib.sha256(
        ("\n".join(scopes) + ("\n" if scopes else "")).encode("utf-8")
    ).hexdigest()


def _validate_rollout_workloads(
    value: object, *, ranked_scope_ids: Sequence[str]
) -> list[dict[str, Any]]:
    fields = {
        "scope",
        "work_item_id",
        "schedule_target_limit",
        "schedule_targets_sha256",
        "player_pagination_target_limit",
        "match_target_count",
        "match_targets_sha256",
        "preview_target_count",
        "preview_targets_sha256",
        "paid_target_count",
    }
    if not isinstance(value, list) or len(value) != len(ranked_scope_ids):
        raise AdmissionError("current rollout ranking basis is incomplete")
    normalized: list[dict[str, Any]] = []
    for item in value:
        if not isinstance(item, dict) or set(item) != fields:
            raise AdmissionError("current rollout ranking-basis fields are invalid")
        scope = item.get("scope")
        paid = item.get("paid_target_count")
        schedule = item.get("schedule_target_limit")
        pagination = item.get("player_pagination_target_limit")
        matches = item.get("match_target_count")
        previews = item.get("preview_target_count")
        if (
            not isinstance(scope, str)
            or not scope
            or scope != scope.strip()
            or len(scope) > 512
            or item.get("work_item_id")
            != "scope-" + hashlib.sha256(scope.encode("utf-8")).hexdigest()
            or isinstance(paid, bool)
            or not isinstance(paid, int)
            or not 1 <= paid <= 5_000
            or isinstance(schedule, bool)
            or not isinstance(schedule, int)
            or schedule <= 0
            or isinstance(pagination, bool)
            or not isinstance(pagination, int)
            or pagination < 0
            or isinstance(matches, bool)
            or not isinstance(matches, int)
            or not 0 <= matches <= 100
            or isinstance(previews, bool)
            or not isinstance(previews, int)
            or not 0 <= previews <= 256
            or paid != schedule + pagination + matches + previews
            or any(
                not isinstance(item.get(field), str)
                or _DIGEST.fullmatch(item[field]) is None
                for field in (
                    "schedule_targets_sha256",
                    "match_targets_sha256",
                    "preview_targets_sha256",
                )
            )
        ):
            raise AdmissionError("current rollout ranking-basis item is invalid")
        normalized.append(dict(item))
    if [item["scope"] for item in normalized] != list(
        ranked_scope_ids
    ) or normalized != sorted(
        normalized,
        key=lambda workload: (
            -int(workload["paid_target_count"]),
            str(workload["scope"]),
        ),
    ):
        raise AdmissionError("current rollout ranking basis is not heavy-first")
    return normalized


def _normalized_bootstrap_authority(value: Mapping[str, Any]) -> dict[str, Any]:
    """Use the release's one shared bootstrap wire validator, loaded lazily."""

    try:
        from dags.scripts.whoscored_bootstrap import normalize_bootstrap_authority
    except ImportError as exc:
        raise AdmissionError(
            "current rollout bootstrap validator is unavailable"
        ) from exc
    try:
        return normalize_bootstrap_authority(value)
    except ValueError as exc:
        raise AdmissionError(
            f"current rollout bootstrap authority is invalid: {exc}"
        ) from exc


def _verify_owner_authority_signature(
    value: Mapping[str, Any],
    *,
    unsigned_fields: frozenset[str],
    secret: str,
    label: str,
) -> str:
    if frozenset(value) != unsigned_fields | {"document_sha256", "signature"}:
        raise AdmissionError(f"{label} fields are invalid")
    unsigned = {field: value[field] for field in unsigned_fields}
    digest = hashlib.sha256(_authority_canonical_bytes(unsigned)).hexdigest()
    signed_body = {**unsigned, "document_sha256": digest}
    signature = hmac.new(
        secret.encode("utf-8"),
        _authority_canonical_bytes(signed_body),
        hashlib.sha256,
    ).hexdigest()
    if (
        not isinstance(value.get("document_sha256"), str)
        or not hmac.compare_digest(str(value["document_sha256"]), digest)
        or not isinstance(value.get("signature"), str)
        or not hmac.compare_digest(str(value["signature"]), signature)
    ):
        raise AdmissionError(f"{label} digest/signature is invalid")
    return digest


def _validate_current_active_rollout_authority(
    rollout_id: str,
    *,
    owner_secret_path: Path,
    provider_policy: Mapping[str, str | int],
    required_wave_id: str | None,
) -> dict[str, Any]:
    """Bind one exact active wave to its root-owned manifest and charter."""

    if _ROLLOUT_ID.fullmatch(rollout_id) is None:
        raise AdmissionError("WhoScored rollout id is invalid")
    rollout = _read_protected_authority(
        _CURRENT_ROLLOUT_PATH, label="current rollout manifest"
    )
    wave_id = rollout.get("wave_id")
    wave_contract = _ROLLOUT_WAVE_CONTRACTS.get(str(wave_id))
    if (
        frozenset(rollout) != _ROLLOUT_MANIFEST_FIELDS
        or rollout.get("schema_version") != _ROLLOUT_MANIFEST_SCHEMA_VERSION
        or rollout.get("rollout_id") != rollout_id
        or wave_contract is None
        or (rollout.get("max_scopes"), rollout.get("require_full_active"))
        != wave_contract
        or (required_wave_id is not None and wave_id != required_wave_id)
    ):
        raise AdmissionError(
            "current rollout is not the requested final wave-all authority"
            if required_wave_id == "wave-all"
            else "current rollout is not the requested active wave authority"
        )
    _authority_token(rollout.get("cohort_id"), field="cohort_id")
    _authority_token(rollout.get("rollout_id"), field="rollout_id")
    for field in (
        "ranked_scope_ids_sha256",
        "ranking_basis_workload_sha256",
        "runtime_sha256",
        "classifier_sha256",
        "promotion_acceptance_sha256",
        "promotion_terminal_receipt_sha256",
    ):
        _authority_digest(rollout.get(field), field=field)
    promotion_proofs = (
        rollout["promotion_acceptance_sha256"],
        rollout["promotion_terminal_receipt_sha256"],
    )
    if (
        wave_id == "wave-20"
        and promotion_proofs
        != (
            _ROLLOUT_GENESIS_PROOF_SHA256,
            _ROLLOUT_GENESIS_PROOF_SHA256,
        )
    ) or (
        wave_id != "wave-20"
        and any(proof == _ROLLOUT_GENESIS_PROOF_SHA256 for proof in promotion_proofs)
    ):
        raise AdmissionError("current rollout promotion proof differs from its wave")
    ranked_scope_ids = rollout.get("ranked_scope_ids")
    if (
        not isinstance(ranked_scope_ids, list)
        or not ranked_scope_ids
        or len(ranked_scope_ids) > 2_000
        or any(
            not isinstance(scope, str)
            or not scope
            or scope != scope.strip()
            or len(scope) > 512
            for scope in ranked_scope_ids
        )
        or len(ranked_scope_ids) != len(set(ranked_scope_ids))
        or rollout["ranked_scope_ids_sha256"] != _scope_specs_sha256(ranked_scope_ids)
    ):
        raise AdmissionError("current rollout ranked scope identity is invalid")
    workloads = _validate_rollout_workloads(
        rollout.get("ranking_basis_scope_workloads"),
        ranked_scope_ids=ranked_scope_ids,
    )
    if (
        rollout["ranking_basis_workload_sha256"]
        != hashlib.sha256(_authority_canonical_bytes(workloads)).hexdigest()
    ):
        raise AdmissionError("current rollout ranking-basis digest is invalid")
    bootstrap_authority = _normalized_bootstrap_authority(rollout)
    if bootstrap_authority["provider_order_cap_bytes"] != provider_policy.get(
        "order_cap_bytes"
    ):
        raise AdmissionError(
            "current rollout provider order cap differs from signed policy"
        )
    cohort_sha256 = hashlib.sha256(_authority_canonical_bytes(rollout)).hexdigest()

    charter = _read_protected_authority(
        _CURRENT_CHARTER_PATH, label="current rollout charter"
    )
    secret = _read_owner_secret(owner_secret_path)
    charter_sha256 = _verify_owner_authority_signature(
        charter,
        unsigned_fields=_CHARTER_UNSIGNED_FIELDS,
        secret=secret,
        label="current rollout charter",
    )
    rollout_charter_fields = (
        "rollout_id",
        "wave_id",
        "max_scopes",
        "require_full_active",
        "ranked_scope_ids_sha256",
        "runtime_sha256",
        "classifier_sha256",
        "promotion_acceptance_sha256",
        "promotion_terminal_receipt_sha256",
        "acceptance_mode",
        "bootstrap_slots",
        "capacity_receipt_sha256",
        "provider_order_cap_bytes",
    )
    if (
        frozenset(charter) != _CHARTER_FIELDS
        or charter.get("schema_version") != _CHARTER_SCHEMA_VERSION
        or charter.get("source") != "whoscored"
        or charter.get("signature_algorithm") != "hmac-sha256"
        or charter.get("provider_policy_sha256")
        != provider_policy.get("document_sha256")
        or charter.get("order_id") != provider_policy.get("order_id")
        or charter.get("cohort_id") != rollout.get("cohort_id")
        or charter.get("cohort_sha256") != cohort_sha256
        or any(
            charter.get(field) != rollout.get(field) for field in rollout_charter_fields
        )
        or _normalized_bootstrap_authority(charter) != bootstrap_authority
    ):
        raise AdmissionError(
            "current charter does not bind the active policy and rollout"
        )
    now = _provider_receipt_now()
    valid_from = _provider_policy_utc(
        charter.get("valid_from"), field="charter.valid_from"
    )
    valid_until = _provider_policy_utc(
        charter.get("valid_until"), field="charter.valid_until"
    )
    policy_valid_from = _provider_policy_utc(
        provider_policy.get("valid_from"), field="valid_from"
    )
    policy_valid_until = _provider_policy_utc(
        provider_policy.get("valid_until"), field="valid_until"
    )
    if (
        not valid_from <= now < valid_until
        or valid_until - now > MAX_CHARTER_HORIZON
        or valid_from < policy_valid_from
        or valid_until > policy_valid_until
        or charter.get("billing_month") != now.strftime("%Y-%m")
    ):
        raise AdmissionError("current rollout charter is not active")
    charter_caps: list[int] = []
    for field in ("daily_cap_bytes", "monthly_cap_bytes", "order_cap_bytes"):
        item = charter.get(field)
        if isinstance(item, bool) or not isinstance(item, int) or item <= 0:
            raise AdmissionError(f"current rollout charter {field} is invalid")
        charter_caps.append(item)
    max_issuances = charter.get("max_issuances")
    if (
        charter_caps != sorted(charter_caps)
        or any(
            charter_caps[index] > int(provider_policy[field])
            for index, field in enumerate(
                ("daily_cap_bytes", "monthly_cap_bytes", "order_cap_bytes")
            )
        )
        or isinstance(max_issuances, bool)
        or not isinstance(max_issuances, int)
        or max_issuances <= 0
        or charter["order_cap_bytes"]
        > bootstrap_authority["provider_order_cap_bytes"] * 95 // 100
    ):
        raise AdmissionError("current rollout charter caps are invalid")
    authority = {
        field: (cohort_sha256 if field == "cohort_sha256" else rollout[field])
        for field in _ROLLOUT_ACCEPTANCE_AUTHORITY_FIELDS
    }
    return {
        "authority": authority,
        "authority_binding": "current-signed-rollout",
        "catalog_active_scope_count": len(ranked_scope_ids),
        "catalog_active_scopes_sha256": _scope_specs_sha256(sorted(ranked_scope_ids)),
        "charter_sha256": charter_sha256,
        "cohort_id": rollout["cohort_id"],
        "rollout_manifest_sha256": cohort_sha256,
    }


def validate_current_rollout_authority(
    rollout_id: str,
    *,
    owner_secret_path: Path,
    provider_policy: Mapping[str, str | int],
) -> dict[str, Any]:
    """Bind final GO to the one active root-owned wave-all authority."""

    return _validate_current_active_rollout_authority(
        rollout_id,
        owner_secret_path=owner_secret_path,
        provider_policy=provider_policy,
        required_wave_id="wave-all",
    )


def validate_current_issuance_authority(
    rollout_id: str,
    *,
    owner_secret_path: Path,
    provider_policy: Mapping[str, str | int],
) -> dict[str, Any]:
    """Bind daily issuance to whichever exact rollout wave is active now."""

    return _validate_current_active_rollout_authority(
        rollout_id,
        owner_secret_path=owner_secret_path,
        provider_policy=provider_policy,
        required_wave_id=None,
    )


def _decimal_gigabytes_to_bytes(value: object, *, field: str) -> int:
    if not isinstance(value, str) or re.fullmatch(r"[0-9]+\.[0-9]{2}", value) is None:
        raise AdmissionError(f"provider quota receipt {field} is invalid")
    try:
        result = Decimal(value) * Decimal(1_000_000_000)
    except InvalidOperation as exc:
        raise AdmissionError(f"provider quota receipt {field} is invalid") from exc
    if result != result.to_integral_value() or result < 0:
        raise AdmissionError(f"provider quota receipt {field} is invalid")
    return int(result)


def validate_provider_quota_receipt(
    path: Path,
    *,
    provider_policy_path: Path | None = None,
    owner_secret_path: Path | None = None,
) -> dict[str, str | int]:
    """Bind admission to fresh, credential-free provider quota evidence."""

    _canonical_existing_path(path, label="provider quota receipt")
    try:
        raw = provenance.read_protected_regular_file(
            path, label="provider quota receipt"
        )
    except provenance.ProvenanceError as exc:
        raise AdmissionError(str(exc)) from exc
    if not 0 < len(raw) <= MAX_PROVIDER_QUOTA_RECEIPT_BYTES:
        raise AdmissionError("provider quota receipt size is invalid")
    try:
        document = json.loads(raw.decode("utf-8"), object_pairs_hook=_unique_object)
    except (_DuplicateKey, UnicodeDecodeError, json.JSONDecodeError) as exc:
        raise AdmissionError("provider quota receipt is not strict JSON") from exc
    if (
        not isinstance(document, dict)
        or frozenset(document) != _PROVIDER_QUOTA_RECEIPT_FIELDS
        or raw != _canonical_bytes(document)
    ):
        raise AdmissionError("provider quota receipt is not canonical JSON")
    if (provider_policy_path is None) != (owner_secret_path is None):
        raise AdmissionError("provider policy and owner key must be supplied together")
    policy = (
        validate_provider_policy(
            provider_policy_path, owner_secret_path=owner_secret_path
        )
        if provider_policy_path is not None and owner_secret_path is not None
        else None
    )
    if policy is None:
        expected = {
            "schema_version": 1,
            "status": "active",
            "provider": "PROXYS.IO",
            "order_id": "38950",
            "plan": "Bronze",
            "quota_decimal_gb": "1.00",
            "remaining_decimal_gb": "1.00",
        }
    else:
        expected = {
            "schema_version": 1,
            "status": "active",
            "provider": policy["provider_id"],
            "order_id": policy["order_id"],
            "plan": policy["plan_id"],
        }
    if any(document.get(name) != value for name, value in expected.items()):
        raise AdmissionError("provider quota receipt does not prove the exact order")
    receipt_digest = hashlib.sha256(raw).hexdigest()
    if policy is not None:
        quota_bytes = _decimal_gigabytes_to_bytes(
            document.get("quota_decimal_gb"), field="quota_decimal_gb"
        )
        remaining_bytes = _decimal_gigabytes_to_bytes(
            document.get("remaining_decimal_gb"), field="remaining_decimal_gb"
        )
        if (
            not hmac.compare_digest(str(policy["receipt_sha256"]), receipt_digest)
            or quota_bytes != policy["provider_quota_bytes"]
            or remaining_bytes > quota_bytes
            or remaining_bytes < policy["safety_cap_bytes"]
        ):
            raise AdmissionError("provider quota receipt differs from signed policy")
    observed_raw = document.get("observed_at")
    if (
        type(observed_raw) is not str
        or re.fullmatch(
            r"[0-9]{4}-[0-9]{2}-[0-9]{2}T[0-9]{2}:[0-9]{2}:[0-9]{2}Z", observed_raw
        )
        is None
    ):
        raise AdmissionError("provider quota receipt observed_at is invalid")
    observed_at = datetime.fromisoformat(observed_raw.replace("Z", "+00:00"))
    age = _provider_receipt_now() - observed_at
    if age < -timedelta(minutes=5) or age > MAX_PROVIDER_QUOTA_RECEIPT_AGE:
        raise AdmissionError("provider quota receipt is stale or future-dated")
    screenshot_raw = document.get("screenshot_path")
    screenshot_sha256 = document.get("screenshot_sha256")
    if (
        type(screenshot_raw) is not str
        or not screenshot_raw
        or type(screenshot_sha256) is not str
        or _DIGEST.fullmatch(screenshot_sha256) is None
    ):
        raise AdmissionError("provider quota screenshot identity is invalid")
    screenshot = Path(screenshot_raw)
    _canonical_existing_path(screenshot, label="provider quota screenshot")
    try:
        screenshot_bytes = provenance.read_protected_regular_file(
            screenshot, label="provider quota screenshot"
        )
        screenshot_stat = screenshot.lstat()
    except (OSError, provenance.ProvenanceError) as exc:
        raise AdmissionError("provider quota screenshot is not protected") from exc
    if not hmac.compare_digest(
        hashlib.sha256(screenshot_bytes).hexdigest(), screenshot_sha256
    ):
        raise AdmissionError("provider quota screenshot digest differs")
    screenshot_time = datetime.fromtimestamp(
        screenshot_stat.st_mtime, tz=timezone.utc
    ).replace(microsecond=0)
    if screenshot_time != observed_at:
        raise AdmissionError("provider quota screenshot time differs from receipt")
    projection: dict[str, str | int] = {
        **expected,
        "observed_at": observed_raw,
        "receipt_path": str(path),
        "receipt_sha256": receipt_digest,
        "screenshot_path": str(screenshot),
        "screenshot_sha256": screenshot_sha256,
    }
    if policy is not None:
        projection.update(
            {
                "provider_policy_path": str(policy["policy_path"]),
                "provider_policy_sha256": str(policy["document_sha256"]),
                "daily_cap_bytes": int(policy["daily_cap_bytes"]),
                "monthly_cap_bytes": int(policy["monthly_cap_bytes"]),
                "order_cap_bytes": int(policy["order_cap_bytes"]),
                "provider_quota_bytes": int(policy["provider_quota_bytes"]),
                "safety_cap_bytes": int(policy["safety_cap_bytes"]),
            }
        )
    return projection


def validate_deployment_admission_receipt(
    path: Path,
    *,
    deployment_attestation_path: Path,
    provider_policy: Mapping[str, str | int],
) -> dict[str, str]:
    """Bind unattended running checks to the deploy-time fresh-receipt gate."""

    _canonical_existing_path(path, label="deployment admission receipt")
    try:
        raw = provenance.read_protected_regular_file(
            path, label="deployment admission receipt"
        )
        value = json.loads(raw.decode("utf-8"), object_pairs_hook=_unique_object)
        deployment_raw = provenance.read_protected_regular_file(
            deployment_attestation_path, label="deployment attestation"
        )
    except (
        _DuplicateKey,
        UnicodeDecodeError,
        json.JSONDecodeError,
        provenance.ProvenanceError,
    ) as exc:
        raise AdmissionError(
            "deployment admission receipt is not protected strict data"
        ) from exc
    if not isinstance(value, dict) or raw != _canonical_bytes(value):
        raise AdmissionError("deployment admission receipt is not canonical JSON")
    attestation = value.get("deployment_attestation")
    quota = value.get("provider_quota_receipt")
    if (
        value.get("schema_version") != 2
        or value.get("status") != "rendered-admitted-v2"
        or value.get("projects")
        != {
            COMMON_PROJECT: list(COMMON_PROTECTED_SERVICES),
            GATEWAY_PROJECT: list(GATEWAY_PROTECTED_SERVICES),
        }
        or not isinstance(attestation, dict)
        or attestation
        != {
            "path": str(deployment_attestation_path),
            "sha256": hashlib.sha256(deployment_raw).hexdigest(),
        }
        or not isinstance(quota, dict)
        or quota.get("provider_policy_sha256") != provider_policy.get("document_sha256")
        or quota.get("receipt_sha256") != provider_policy.get("receipt_sha256")
        or quota.get("order_id") != provider_policy.get("order_id")
        or quota.get("daily_cap_bytes") != provider_policy.get("daily_cap_bytes")
    ):
        raise AdmissionError(
            "deployment admission receipt differs from attestation/provider policy"
        )
    return {
        "path": str(path),
        "sha256": hashlib.sha256(raw).hexdigest(),
    }


def _assert_writable_runtime_directory(path: Path, *, label: str) -> os.stat_result:
    if not path.is_absolute() or not path.name:
        raise AdmissionError(f"{label} must be an absolute directory")
    parent = -1
    descriptor = -1
    try:
        parent, name = provenance.open_protected_parent(path, label=label)
        descriptor = os.open(
            name,
            os.O_RDONLY | os.O_DIRECTORY | os.O_NOFOLLOW | os.O_CLOEXEC,
            dir_fd=parent,
        )
        before = os.fstat(descriptor)
        entry = os.stat(name, dir_fd=parent, follow_symlinks=False)
    except (OSError, provenance.ProvenanceError) as exc:
        raise AdmissionError(
            f"{label} is missing, unsafe, or symlinked: {path}"
        ) from exc
    finally:
        if descriptor >= 0:
            os.close(descriptor)
        if parent >= 0:
            os.close(parent)
    identity = (
        "st_dev",
        "st_ino",
        "st_mode",
        "st_uid",
        "st_gid",
        "st_mtime_ns",
        "st_ctime_ns",
    )
    mode = stat.S_IMODE(before.st_mode)
    if (
        before.st_uid not in {0, 50_000, 65_534}
        or before.st_gid != 0
        or mode & 0o070 != 0o070
        or mode & 0o002
        or any(getattr(before, field) != getattr(entry, field) for field in identity)
    ):
        raise AdmissionError(f"{label} is not a protected writable directory: {path}")
    return before


def _canonical_existing_path(path: Path, *, label: str) -> Path:
    try:
        resolved = path.resolve(strict=True)
    except OSError as exc:
        raise AdmissionError(f"{label} is missing: {path}") from exc
    if path != resolved:
        raise AdmissionError(f"{label} is not a canonical non-symlink path: {path}")
    return resolved


def _assert_separate_mounts(paths: Mapping[str, Path], *, label: str) -> None:
    resolved = {
        name: _canonical_existing_path(path, label=f"{label} {name}")
        for name, path in paths.items()
    }
    items = tuple(resolved.items())
    for index, (left_name, left) in enumerate(items):
        left_stat = left.stat()
        for right_name, right in items[index + 1 :]:
            right_stat = right.stat()
            if (
                (left_stat.st_dev, left_stat.st_ino)
                == (right_stat.st_dev, right_stat.st_ino)
                or left in right.parents
                or right in left.parents
            ):
                raise AdmissionError(
                    f"{label} mounts alias or nest: {left_name}, {right_name}"
                )


def _validate_bind_source_policy(
    projections: Mapping[str, Mapping[str, Any]],
    *,
    root: Path,
    selected_services: Sequence[str] = PROTECTED_SERVICES,
) -> None:
    selected = tuple(selected_services)
    if set(projections) != set(selected):
        raise AdmissionError("rendered bind-source service set differs")
    sources: dict[tuple[str, str], Path] = {}
    for service in selected:
        for kind, raw_source, target, _read_only in projections[service]["volumes"]:
            if kind == "bind":
                sources[(service, target)] = Path(raw_source)
    expected = {
        (service, target)
        for service, targets in _RELEASE_BIND_TARGETS.items()
        if service in selected
        for target in targets
    } | {identity for identity in _RUNTIME_HOST_BIND_TARGETS if identity[0] in selected}
    pointer_identity = (
        "airflow-scheduler",
        "/opt/airflow/secure/whoscored-scheduled-pointers",
    )
    if pointer_identity not in sources:
        expected.discard(pointer_identity)
    if set(sources) != expected:
        raise AdmissionError("rendered bind-source policy differs")
    release_root = _canonical_existing_path(root, label="release checkout")
    artifact_identity = (
        "airflow-scheduler",
        "/opt/airflow/runtime/sofascore/proxy_budget_canary.json",
    )
    artifact_source = _canonical_existing_path(
        sources[artifact_identity], label="SofaScore budget artifact"
    )
    if artifact_source == release_root or release_root in artifact_source.parents:
        raise AdmissionError(
            "SofaScore budget artifact must be outside the release checkout"
        )
    for service, targets in _RELEASE_BIND_TARGETS.items():
        if service not in selected:
            continue
        for target, relative in targets.items():
            source = sources[(service, target)]
            expected_source = root / relative
            if source != expected_source:
                raise AdmissionError(
                    f"rendered release bind source differs: {service} {target}"
                )
            if expected_source.is_dir():
                _assert_protected_directory(
                    source, label=f"release code directory for {service} {target}"
                )
            else:
                _assert_protected_regular_file(
                    source, label=f"release code file for {service} {target}"
                )
    for identity, policy in _RUNTIME_HOST_BIND_TARGETS.items():
        if identity[0] not in selected:
            continue
        if identity == pointer_identity and identity not in sources:
            continue
        source = sources[identity]
        _canonical_existing_path(
            source, label=f"runtime bind {identity[0]} {identity[1]}"
        )
        if policy == "writable-directory":
            _assert_writable_runtime_directory(
                source, label=f"runtime state directory for {identity[0]} {identity[1]}"
            )
        elif policy == "airflow-authority-directory":
            _assert_airflow_authority_directory(
                source,
                label=f"Airflow authority directory for {identity[0]} {identity[1]}",
            )
        elif policy == "protected-directory":
            _assert_protected_directory(
                source, label=f"authority directory for {identity[0]} {identity[1]}"
            )
        elif policy == "scheduler-readable-protected-file":
            _assert_scheduler_readable_regular_file(
                source,
                label=f"scheduler runtime input for {identity[0]} {identity[1]}",
            )
        elif policy == "fbref-geoip-database":
            _assert_fbref_geoip_database(source)
        else:
            _assert_protected_regular_file(
                source, label=f"protected input for {identity[0]} {identity[1]}"
            )
    scheduler_filter_identity = (
        "airflow-scheduler",
        "/opt/airflow/state/whoscored-proxy-filter",
    )
    filter_identity = (
        "whoscored_proxy_filter",
        "/opt/airflow/state/whoscored-proxy-filter",
    )
    if (
        scheduler_filter_identity in sources
        and filter_identity in sources
        and sources[scheduler_filter_identity] != sources[filter_identity]
    ):
        raise AdmissionError(
            "scheduler read-only provider evidence does not bind filter-owned state"
        )
    protected_mount_identities = {
        "fotmob-admission": ("airflow-scheduler", "/opt/airflow/fotmob-admission"),
        "fbref-geoip-database": (
            "airflow-scheduler",
            FBREF_CAMOUFOX_GEOIP_DATABASE_CONTAINER_PATH,
        ),
        "scheduler-logs": ("airflow-scheduler", "/opt/airflow/logs"),
        "sofascore-budget-artifact": (
            "airflow-scheduler",
            "/opt/airflow/runtime/sofascore/proxy_budget_canary.json",
        ),
        "gateway-state": (
            "whoscored_paid_gateway",
            "/opt/airflow/state/whoscored-paid-gateway",
        ),
        "filter-state": filter_identity,
        "scheduler-approvals": (
            "airflow-scheduler",
            "/opt/airflow/secure/whoscored-approvals",
        ),
        "gateway-alert-authority": (
            "whoscored_paid_gateway",
            "/opt/airflow/secure/whoscored-alert-authority",
        ),
    }
    protected_mounts = {
        name: sources[identity]
        for name, identity in protected_mount_identities.items()
        if identity in sources
    }
    if pointer_identity in sources:
        protected_mounts["scheduler-pointers"] = sources[pointer_identity]
    _assert_separate_mounts(protected_mounts, label="protected runtime")


def _read_protected_canonical_object(path: Path, *, label: str) -> dict[str, Any]:
    _canonical_existing_path(path, label=label)
    try:
        raw = provenance.read_protected_regular_file(path, label=label)
        value = json.loads(raw.decode("utf-8"), object_pairs_hook=_unique_object)
    except (
        _DuplicateKey,
        UnicodeDecodeError,
        json.JSONDecodeError,
        provenance.ProvenanceError,
    ) as exc:
        raise AdmissionError(f"{label} is not protected strict JSON") from exc
    if not isinstance(value, dict) or raw != _canonical_bytes(value):
        raise AdmissionError(f"{label} is not canonical JSON")
    return value


def _capacity_number(value: object, *, field: str) -> Decimal:
    if isinstance(value, bool) or not isinstance(value, (int, float)):
        raise AdmissionError(f"capacity receipt {field} is invalid")
    number = Decimal(str(value))
    if not number.is_finite() or number < 0:
        raise AdmissionError(f"capacity receipt {field} is invalid")
    return number


def _capacity_nonnegative_int(value: object, *, field: str) -> int:
    if type(value) is not int or value < 0:
        raise AdmissionError(f"capacity receipt {field} is invalid")
    return value


def _capacity_utc_timestamp(value: object, *, field: str) -> datetime:
    if not isinstance(value, str) or not value:
        raise AdmissionError(f"capacity receipt {field} is invalid")
    try:
        parsed = datetime.fromisoformat(value)
    except ValueError as exc:
        raise AdmissionError(f"capacity receipt {field} is invalid") from exc
    if (
        parsed.tzinfo is None
        or parsed.utcoffset() != timedelta(0)
        or parsed.isoformat() != value
    ):
        raise AdmissionError(f"capacity receipt {field} is not canonical UTC")
    return parsed


def _capacity_runtime_contract_evidence() -> tuple[dict[str, str], str, str]:
    """Return the current lock map, lock digest and exact sealed-tree digest."""

    path = _REPOSITORY_ROOT / _CAPACITY_RUNTIME_CONTRACT_RELATIVE
    try:
        raw = provenance.read_protected_regular_file(
            path, label="WhoScored runtime contract"
        )
        document = json.loads(raw.decode("utf-8"), object_pairs_hook=_unique_object)
    except (
        _DuplicateKey,
        UnicodeDecodeError,
        json.JSONDecodeError,
        provenance.ProvenanceError,
    ) as exc:
        raise AdmissionError("capacity runtime contract is invalid") from exc
    if (
        not isinstance(document, dict)
        or set(document)
        != {
            "business_dataset_count",
            "files",
            "parser_version",
            "report_schema_version",
            "schema_version",
        }
        or document.get("schema_version") != 1
    ):
        raise AdmissionError("capacity runtime contract schema is invalid")
    files = document.get("files")
    if not isinstance(files, dict) or not files:
        raise AdmissionError("capacity runtime contract file map is invalid")
    normalized: dict[str, str] = {}
    for relative, digest in files.items():
        parts = relative.split("/") if isinstance(relative, str) else []
        if (
            not isinstance(relative, str)
            or not relative
            or relative.startswith("/")
            or any(part in {"", ".", ".."} for part in parts)
            or not isinstance(digest, str)
            or _DIGEST.fullmatch(digest) is None
        ):
            raise AdmissionError("capacity runtime contract file map is invalid")
        normalized[relative] = digest
    expected_locked_paths = _CAPACITY_SEALED_RUNTIME_PATHS - {
        _CAPACITY_RUNTIME_CONTRACT_RELATIVE
    }
    if not expected_locked_paths.issubset(normalized):
        raise AdmissionError("capacity runtime contract omits canary runtime files")

    lock_sha256 = hashlib.sha256(raw).hexdigest()
    tree_files = dict(normalized)
    tree_files[_CAPACITY_RUNTIME_CONTRACT_RELATIVE] = lock_sha256
    aggregate = hashlib.sha256(b"whoscored-capacity-runtime-tree-v1\0")
    for relative, digest in sorted(tree_files.items()):
        aggregate.update(relative.encode("utf-8"))
        aggregate.update(b"\0")
        aggregate.update(bytes.fromhex(digest))
    return normalized, lock_sha256, aggregate.hexdigest()


def _capacity_runtime_file_evidence() -> dict[str, str]:
    """Hash the exact protected files included by the capacity identity."""

    result: dict[str, str] = {}
    for relative in sorted(_CAPACITY_RUNTIME_FILE_PATHS):
        path = _REPOSITORY_ROOT / relative
        try:
            raw = provenance.read_protected_regular_file(
                path, label=f"capacity runtime file {relative}"
            )
        except provenance.ProvenanceError as exc:
            raise AdmissionError("capacity runtime file is not protected") from exc
        result[relative] = hashlib.sha256(raw).hexdigest()
    return result


def _validate_capacity_container_record(
    record: object,
    *,
    service: str,
    running_images: Mapping[str, Mapping[str, Any]],
) -> Mapping[str, Any]:
    expected_image = running_images.get(service)
    flaresolverr = service == "flaresolverr"
    if (
        not isinstance(record, dict)
        or set(record) != _CAPACITY_CONTAINER_FIELDS
        or not isinstance(expected_image, Mapping)
        or record.get("name") != service
        or record.get("id") != expected_image.get("container_id")
        or record.get("image_id") != expected_image.get("image_id")
        or record.get("status") != "running"
        or record.get("running") is not True
        or record.get("healthy") is not True
        or record.get("oom_killed") is not False
        or record.get("restart_count") != 0
        or type(record.get("restart_count")) is not int
        or record.get("production_admission_contract_ok") is not True
        or any(
            record.get(field) is not flaresolverr
            for field in (
                "command_contract_ok",
                "image_identity_contract_ok",
                "immutable_payload_contract_ok",
                "security_contract_ok",
                "compose_identity_ok",
                "published_endpoint_contract_ok",
            )
        )
    ):
        raise AdmissionError("capacity receipt container evidence is invalid")
    pid = _capacity_nonnegative_int(record.get("pid"), field="container pid")
    memory = _capacity_nonnegative_int(
        record.get("memory_usage_bytes"), field="container memory"
    )
    memory_limit = _capacity_nonnegative_int(
        record.get("memory_limit_bytes"), field="container memory limit"
    )
    processes = _capacity_nonnegative_int(
        record.get("process_count"), field="container process count"
    )
    if pid == 0 or memory_limit == 0 or memory > memory_limit or processes == 0:
        raise AdmissionError("capacity receipt container resources are invalid")
    return record


def _validate_capacity_run_summaries(
    runs: object,
    *,
    scopes: frozenset[str],
    completed_by_worker: Mapping[str, int],
    completed_runs: int,
    summaries_total: object,
    summaries_retained: object,
    summaries_truncated: object,
    deadline_truncations: int,
    max_source_stage_count: int,
) -> None:
    if (
        not isinstance(runs, list)
        or type(summaries_total) is not int
        or type(summaries_retained) is not int
        or type(summaries_truncated) is not bool
        or summaries_total != completed_runs + deadline_truncations
        or summaries_retained != len(runs)
        or not 1 <= summaries_retained <= _CAPACITY_MAX_RUN_SUMMARIES
        or summaries_truncated is not (summaries_total > summaries_retained)
    ):
        raise AdmissionError("capacity receipt run-summary counters are invalid")

    identities: set[tuple[int, int]] = set()
    retained_by_worker = {worker_id: 0 for worker_id in range(_CAPACITY_WORKER_COUNT)}
    retained_deadlines = 0
    for run in runs:
        if not isinstance(run, dict) or set(run) != _CAPACITY_RUN_FIELDS:
            raise AdmissionError("capacity receipt run summary is invalid")
        worker_id = _capacity_nonnegative_int(
            run.get("worker_id"), field="run worker_id"
        )
        iteration = _capacity_nonnegative_int(
            run.get("iteration"), field="run iteration"
        )
        process_elapsed = _capacity_number(
            run.get("process_elapsed_seconds"), field="run process elapsed"
        )
        identity = (worker_id, iteration)
        if (
            worker_id >= _CAPACITY_WORKER_COUNT
            or identity in identities
            or run.get("scope") not in scopes
            or type(run.get("returncode")) is not int
            or _capacity_nonnegative_int(
                run.get("stderr_bytes"), field="run stderr bytes"
            )
            < 0
            or not isinstance(run.get("stderr_sha256"), str)
            or _DIGEST.fullmatch(run["stderr_sha256"]) is None
        ):
            raise AdmissionError("capacity receipt run summary is invalid")
        identities.add(identity)
        retained_by_worker[worker_id] += 1
        status = run.get("status")
        if status == "success":
            if (
                run.get("returncode") != 0
                or process_elapsed <= 0
                or run.get("termination_reason") is not None
                or run.get("source_request_attempts") != 0
                or type(run.get("source_request_attempts")) is not int
                or run.get("network_requests") != 0
                or type(run.get("network_requests")) is not int
                or run.get("page_units") != 5
                or type(run.get("page_units")) is not int
                or run.get("paid_bytes") != 0
                or type(run.get("paid_bytes")) is not int
                or run.get("paid_route_requests") != 0
                or type(run.get("paid_route_requests")) is not int
                or run.get("traffic_evidence_valid") is not True
                or _capacity_number(
                    run.get("workflow_elapsed_seconds"),
                    field="run workflow elapsed",
                )
                <= 0
                or run.get("publishes") is not False
                or run.get("writes_bronze") is not False
                or run.get("executes_ddl") is not False
                or run.get("entities")
                != ["matches", "multistage", "previews", "profiles"]
                or type(run.get("source_stage_count")) is not int
                or not 2 <= run["source_stage_count"] <= max_source_stage_count
                or run.get("mode") != _CAPACITY_MODE
                or run.get("seed_sha256") != _CAPACITY_SEED_SHA256
                or run.get("seed_evidence_valid") is not True
                or run.get("cleanup_evidence_valid") is not True
            ):
                raise AdmissionError("capacity receipt successful run is unsafe")
        elif status == "deadline_terminated":
            retained_deadlines += 1
            if (
                run.get("termination_reason") != "deadline_terminated"
                or any(
                    run.get(field) != 0
                    for field in (
                        "source_request_attempts",
                        "network_requests",
                        "page_units",
                        "paid_bytes",
                        "paid_route_requests",
                        "workflow_elapsed_seconds",
                        "source_stage_count",
                    )
                )
                or any(
                    run.get(field) is not None
                    for field in (
                        "traffic_evidence_valid",
                        "publishes",
                        "writes_bronze",
                        "executes_ddl",
                        "mode",
                        "seed_sha256",
                        "seed_evidence_valid",
                        "cleanup_evidence_valid",
                    )
                )
                or run.get("entities") != []
            ):
                raise AdmissionError("capacity receipt deadline run is invalid")
        else:
            raise AdmissionError("capacity receipt retained a failed run")
    if (
        set(worker_id for worker_id, _iteration in identities)
        != set(range(_CAPACITY_WORKER_COUNT))
        or retained_deadlines != deadline_truncations
        or any(
            count not in {1, 2}
            or completed_by_worker[str(worker_id)] > 1
            and count != 2
            for worker_id, count in retained_by_worker.items()
        )
    ):
        raise AdmissionError("capacity receipt run retention is inconsistent")


def _validate_capacity_samples(
    samples: object,
    *,
    observed: Decimal,
    total_elapsed: Decimal,
    completed_runs: int,
    total_page_units: int,
    baseline: Sequence[Mapping[str, Any]],
    running_images: Mapping[str, Mapping[str, Any]],
    report: Mapping[str, Any],
) -> None:
    if not isinstance(samples, list) or not 1 <= len(samples) <= _CAPACITY_MAX_SAMPLES:
        raise AdmissionError("capacity receipt sample retention is invalid")
    prior_elapsed = Decimal("-1")
    prior_completed = -1
    sample_points: list[tuple[Decimal, int, int]] = []
    baseline_by_service = {record["name"]: record for record in baseline}
    maxima = {
        "max_harness_rss_bytes": 0,
        "max_container_memory_bytes": 0,
        "max_worker_container_memory_bytes": 0,
        "max_worker_container_pids": 0,
        "max_aggregate_memory_bytes": 0,
    }
    for sample in samples:
        if not isinstance(sample, dict) or set(sample) != _CAPACITY_SAMPLE_FIELDS:
            raise AdmissionError("capacity receipt sample is invalid")
        elapsed = _capacity_number(
            sample.get("elapsed_seconds"), field="sample elapsed"
        )
        sample_completed = _capacity_nonnegative_int(
            sample.get("completed_runs"), field="sample completed_runs"
        )
        page_units = _capacity_nonnegative_int(
            sample.get("page_units"), field="sample page_units"
        )
        projected = _capacity_number(
            sample.get("projected_page_units_per_day"), field="sample throughput"
        )
        if (
            elapsed < prior_elapsed
            or elapsed > total_elapsed + Decimal("0.001")
            or sample_completed < prior_completed
            or sample_completed > completed_runs
            or page_units != sample_completed * 5
            or sample.get("source_request_attempts") != 0
            or type(sample.get("source_request_attempts")) is not int
            or sample.get("paid_bytes") != 0
            or type(sample.get("paid_bytes")) is not int
            or sample.get("paid_route_requests") != 0
            or type(sample.get("paid_route_requests")) is not int
            or abs(
                projected
                - (
                    Decimal(page_units) * Decimal(86_400) / elapsed
                    if elapsed > 0
                    else Decimal(0)
                )
            )
            > Decimal("0.001")
        ):
            raise AdmissionError("capacity receipt sample accounting is invalid")
        prior_elapsed = elapsed
        prior_completed = sample_completed
        sample_points.append((elapsed, sample_completed, page_units))

        containers = sample.get("containers")
        if not isinstance(containers, list) or [
            record.get("name") if isinstance(record, dict) else None
            for record in containers
        ] != list(COMMON_PROTECTED_SERVICES):
            raise AdmissionError("capacity receipt sample container set is invalid")
        container_memory = 0
        for service, record in zip(COMMON_PROTECTED_SERVICES, containers, strict=True):
            admitted = _validate_capacity_container_record(
                record, service=service, running_images=running_images
            )
            baseline_record = baseline_by_service[service]
            if any(
                admitted.get(field) != baseline_record.get(field)
                for field in ("id", "image_id", "restart_count")
            ):
                raise AdmissionError("capacity receipt container identity changed")
            container_memory += int(admitted["memory_usage_bytes"])

        workers = sample.get("worker_containers")
        if not isinstance(workers, list) or len(workers) not in {
            0,
            _CAPACITY_WORKER_COUNT,
        }:
            raise AdmissionError("capacity receipt worker sample is invalid")
        worker_memory = 0
        worker_pids = 0
        worker_ids: set[int] = set()
        for worker in workers:
            if (
                not isinstance(worker, dict)
                or set(worker) != _CAPACITY_WORKER_CONTAINER_FIELDS
            ):
                raise AdmissionError("capacity receipt worker sample is invalid")
            worker_id = _capacity_nonnegative_int(
                worker.get("worker_id"), field="sample worker_id"
            )
            iteration = _capacity_nonnegative_int(
                worker.get("iteration"), field="sample worker iteration"
            )
            del iteration
            worker_memory += _capacity_nonnegative_int(
                worker.get("memory_usage_bytes"), field="sample worker memory"
            )
            worker_pids += _capacity_nonnegative_int(
                worker.get("pids_current"), field="sample worker pids"
            )
            if (
                worker_id >= _CAPACITY_WORKER_COUNT
                or worker_id in worker_ids
                or not isinstance(worker.get("container_id"), str)
                or _CONTAINER_ID.fullmatch(worker["container_id"]) is None
                or worker.get("oom_killed") is not False
                or type(worker.get("running")) is not bool
                or (
                    worker.get("running") is True
                    and (
                        worker.get("status") != "running"
                        or worker.get("exit_code") != 0
                        or type(worker.get("exit_code")) is not int
                    )
                )
                or (
                    worker.get("running") is False
                    and (
                        worker.get("status") != "exited"
                        or worker.get("exit_code") != 0
                        or type(worker.get("exit_code")) is not int
                    )
                )
            ):
                raise AdmissionError("capacity receipt worker sample is unsafe")
            worker_ids.add(worker_id)
        if workers and worker_ids != set(range(_CAPACITY_WORKER_COUNT)):
            raise AdmissionError("capacity receipt worker sample is incomplete")

        harness_memory = _capacity_nonnegative_int(
            sample.get("harness_rss_bytes"), field="sample harness memory"
        )
        stated_worker_memory = _capacity_nonnegative_int(
            sample.get("worker_container_memory_bytes"),
            field="sample worker container memory",
        )
        stated_worker_pids = _capacity_nonnegative_int(
            sample.get("worker_container_pids"), field="sample worker pids"
        )
        stated_container_memory = _capacity_nonnegative_int(
            sample.get("container_memory_bytes"), field="sample container memory"
        )
        aggregate_memory = _capacity_nonnegative_int(
            sample.get("aggregate_memory_bytes"), field="sample aggregate memory"
        )
        rss_process_count = _capacity_nonnegative_int(
            sample.get("rss_process_count"), field="sample RSS process count"
        )
        container_memory += worker_memory
        if (
            stated_worker_memory != worker_memory
            or stated_worker_pids != worker_pids
            or stated_container_memory != container_memory
            or aggregate_memory != harness_memory + container_memory
            or rss_process_count == 0
            or aggregate_memory > _CAPACITY_MAX_COMBINED_RSS_BYTES
        ):
            raise AdmissionError("capacity receipt sample resources are invalid")
        maxima["max_harness_rss_bytes"] = max(
            maxima["max_harness_rss_bytes"], harness_memory
        )
        maxima["max_container_memory_bytes"] = max(
            maxima["max_container_memory_bytes"], container_memory
        )
        maxima["max_worker_container_memory_bytes"] = max(
            maxima["max_worker_container_memory_bytes"], worker_memory
        )
        maxima["max_worker_container_pids"] = max(
            maxima["max_worker_container_pids"], worker_pids
        )
        maxima["max_aggregate_memory_bytes"] = max(
            maxima["max_aggregate_memory_bytes"], aggregate_memory
        )
    work_indexes = [
        index
        for index, (elapsed, _completed, _page_units) in enumerate(sample_points[:-1])
        if elapsed <= observed + Decimal("0.001")
    ]
    if not work_indexes:
        raise AdmissionError("capacity receipt omitted its work-final sample")
    work_index = work_indexes[-1]
    work_elapsed, work_completed, work_page_units = sample_points[work_index]
    final_elapsed, final_completed, final_page_units = sample_points[-1]
    final_sample = samples[-1]
    assert isinstance(final_sample, dict)
    if final_sample.get("worker_containers") != []:
        raise AdmissionError(
            "capacity receipt cleanup-final sample retains worker containers"
        )
    maximum_cadence = (
        _CAPACITY_SAMPLE_INTERVAL_SECONDS + _CAPACITY_SAMPLE_CADENCE_TOLERANCE_SECONDS
    )
    if (
        sample_points[0][0] > maximum_cadence
        or any(
            current[0] - previous[0] > maximum_cadence
            for previous, current in zip(
                sample_points[:work_index],
                sample_points[1 : work_index + 1],
                strict=True,
            )
        )
        or abs(work_elapsed - observed) > _CAPACITY_FINAL_SAMPLE_TOLERANCE_SECONDS
        or work_completed != completed_runs
        or work_page_units != total_page_units
        or abs(final_elapsed - total_elapsed) > _CAPACITY_FINAL_SAMPLE_TOLERANCE_SECONDS
        or final_completed != completed_runs
        or final_page_units != total_page_units
    ):
        raise AdmissionError("capacity receipt monitoring timeline is incomplete")
    if any(
        type(report.get(field)) is not int or report[field] < observed_maximum
        for field, observed_maximum in maxima.items()
    ):
        raise AdmissionError("capacity receipt sample maxima are inconsistent")


def validate_capacity_receipt(
    path: Path,
    *,
    capacity_receipt_sha256: str,
    bindings_evidence: ValidatedBindingsEvidence,
    scheduler_image_id: str,
    compose_inputs_evidence: Mapping[str, ProtectedComposeInputEvidence],
    common_config_hashes: Mapping[str, str],
    running_images_evidence: Sequence[Mapping[str, Any]],
) -> dict[str, Any]:
    """Replay the signed six-hour capacity proof against the current release."""

    value = _read_protected_canonical_object(path, label="capacity receipt")
    if set(value) != _CAPACITY_REPORT_FIELDS:
        raise AdmissionError("capacity receipt schema is invalid")
    claimed = value.get("report_sha256")
    if (
        not isinstance(claimed, str)
        or _DIGEST.fullmatch(claimed) is None
        or not isinstance(capacity_receipt_sha256, str)
        or _DIGEST.fullmatch(capacity_receipt_sha256) is None
        or not secrets.compare_digest(claimed, capacity_receipt_sha256)
    ):
        raise AdmissionError("capacity receipt differs from signed rollout authority")
    unsigned = dict(value)
    unsigned.pop("report_sha256")
    actual = hashlib.sha256(_authority_canonical_bytes(unsigned)).hexdigest()
    if not secrets.compare_digest(claimed, actual):
        raise AdmissionError("capacity receipt content address is invalid")

    if (
        value.get("canary_version") != _CAPACITY_CANARY_VERSION
        or value.get("schema_version") != 1
        or value.get("mode") != _CAPACITY_MODE
        or value.get("status") != "success"
        or value.get("seed_sha256") != _CAPACITY_SEED_SHA256
        or value.get("worker_count") != _CAPACITY_WORKER_COUNT
        or value.get("workers") != _CAPACITY_WORKER_COUNT
        or value.get("publishes") is not False
        or value.get("writes_bronze") is not False
        or value.get("executes_ddl") is not False
        or value.get("network_requests") != 0
        or value.get("paid_bytes") != 0
        or value.get("paid_proxy_bytes") != 0
        or value.get("paid_route_requests") != 0
        or value.get("restart_count") != 0
        or value.get("oom_killed") is not False
        or value.get("cleanup_status") != "success"
        or value.get("page_unit_definition") != _CAPACITY_PAGE_UNIT_DEFINITION
        or value.get("stop_reasons") != []
    ):
        raise AdmissionError("capacity receipt core contract is not green")
    for zero_field in (
        "network_requests",
        "paid_bytes",
        "paid_proxy_bytes",
        "paid_route_requests",
        "restart_count",
    ):
        if type(value[zero_field]) is not int:
            raise AdmissionError("capacity receipt zero counters are invalid")

    requested = _capacity_number(
        value.get("duration_seconds_requested"),
        field="duration_seconds_requested",
    )
    observed = _capacity_number(
        value.get("duration_seconds_observed"),
        field="duration_seconds_observed",
    )
    elapsed = _capacity_number(value.get("elapsed_seconds"), field="elapsed_seconds")
    total_elapsed = _capacity_number(
        value.get("total_elapsed_seconds"), field="total_elapsed_seconds"
    )
    if (
        requested != _CAPACITY_DURATION_SECONDS
        or observed < _CAPACITY_DURATION_SECONDS
        or elapsed != observed
        or total_elapsed < observed
    ):
        raise AdmissionError("capacity receipt did not sustain six hours")
    started_at = _capacity_utc_timestamp(value.get("started_at"), field="started_at")
    ended_at = _capacity_utc_timestamp(value.get("ended_at"), field="ended_at")
    wall_elapsed = Decimal(str((ended_at - started_at).total_seconds()))
    if (
        ended_at <= started_at
        or abs(wall_elapsed - total_elapsed) > _CAPACITY_WALL_CLOCK_TOLERANCE_SECONDS
    ):
        raise AdmissionError("capacity receipt wall-clock timeline is invalid")
    throughput = _capacity_number(
        value.get("projected_page_units_per_day"),
        field="projected_page_units_per_day",
    )
    peak_rss = _capacity_number(
        value.get("peak_combined_rss_bytes"), field="peak_combined_rss_bytes"
    )
    aggregate_rss = _capacity_number(
        value.get("max_aggregate_memory_bytes"),
        field="max_aggregate_memory_bytes",
    )
    if (
        throughput < _CAPACITY_MIN_PAGE_UNITS_PER_DAY
        or peak_rss != aggregate_rss
        or peak_rss > _CAPACITY_MAX_COMBINED_RSS_BYTES
    ):
        raise AdmissionError("capacity receipt throughput or memory gate failed")
    source_attempts = _capacity_nonnegative_int(
        value.get("source_request_attempts"), field="source_request_attempts"
    )
    deadline_truncations = _capacity_nonnegative_int(
        value.get("deadline_truncations"), field="deadline_truncations"
    )
    max_source_stage_count = _capacity_nonnegative_int(
        value.get("max_source_stage_count"), field="max_source_stage_count"
    )
    if (
        source_attempts != 0
        or deadline_truncations > _CAPACITY_WORKER_COUNT
        or max_source_stage_count < 2
        or value.get("raw_store_policy") != "exact content-addressed temporary cache"
        or value.get("repository_policy") != "per-process in-memory repository"
        or type(value.get("match_limit")) is not int
        or not 1 <= value["match_limit"] <= 10
        or type(value.get("profile_limit")) is not int
        or not 1 <= value["profile_limit"] <= 20
    ):
        raise AdmissionError("capacity receipt workload contract is invalid")
    cleanup_elapsed = _capacity_number(
        value.get("cleanup_elapsed_seconds"), field="cleanup_elapsed_seconds"
    )
    completed_worker_seconds = _capacity_number(
        value.get("completed_worker_seconds"), field="completed_worker_seconds"
    )
    if (
        abs(total_elapsed - observed - cleanup_elapsed) > Decimal("0.002")
        or completed_worker_seconds <= 0
    ):
        raise AdmissionError("capacity receipt elapsed accounting is invalid")
    for memory_field in (
        "max_harness_rss_bytes",
        "max_container_memory_bytes",
        "max_worker_container_memory_bytes",
        "max_worker_container_pids",
        "max_aggregate_memory_bytes",
    ):
        _capacity_nonnegative_int(value.get(memory_field), field=memory_field)

    scopes_value = value.get("scopes")
    if (
        not isinstance(scopes_value, list)
        or not scopes_value
        or len(scopes_value) != len(set(scopes_value))
        or any(
            not isinstance(scope, str)
            or scope.count("=") != 1
            or any(not part.strip() for part in scope.split("=", 1))
            for scope in scopes_value
        )
    ):
        raise AdmissionError("capacity receipt scope set is invalid")
    scopes = frozenset(scopes_value)

    completed = value.get("completed_by_worker")
    if (
        not isinstance(completed, dict)
        or set(completed) != {"0", "1", "2", "3"}
        or any(type(count) is not int or count <= 0 for count in completed.values())
    ):
        raise AdmissionError("capacity receipt lacks all four completed workers")
    completed_runs_value = value.get("completed_runs")
    page_units = value.get("page_units")
    if (
        type(completed_runs_value) is not int
        or completed_runs_value != sum(completed.values())
        or type(page_units) is not int
        or page_units != completed_runs_value * 5
        or abs(throughput - (Decimal(page_units) * Decimal(86_400) / observed))
        > Decimal("0.001")
    ):
        raise AdmissionError("capacity receipt page-unit accounting is invalid")
    runs = value.get("runs")
    _validate_capacity_run_summaries(
        runs,
        scopes=scopes,
        completed_by_worker=completed,
        completed_runs=completed_runs_value,
        summaries_total=value.get("run_summaries_total"),
        summaries_retained=value.get("run_summaries_retained"),
        summaries_truncated=value.get("run_summaries_truncated"),
        deadline_truncations=deadline_truncations,
        max_source_stage_count=max_source_stage_count,
    )
    samples = value.get("samples")
    if not isinstance(samples, list) or not 1 <= len(samples) <= _CAPACITY_MAX_SAMPLES:
        raise AdmissionError("capacity receipt sample retention is invalid")
    cleanup = value.get("cleanup")
    if (
        not isinstance(cleanup, dict)
        or set(cleanup)
        != {
            "status",
            "cache_workspaces_removed",
            "worker_runtime_removed",
            "browser_sessions_removed",
        }
        or cleanup.get("status") != "success"
        or any(
            cleanup.get(field) is not True
            for field in (
                "cache_workspaces_removed",
                "worker_runtime_removed",
                "browser_sessions_removed",
            )
        )
    ):
        raise AdmissionError("capacity receipt cleanup proof is not green")
    sealed_runtime = value.get("sealed_worker_runtime")
    if (
        not isinstance(sealed_runtime, dict)
        or set(sealed_runtime)
        != {
            "bundle_sha256",
            "execution_mode",
            "file_count",
            "runtime_cleanup_complete",
            "runtime_tree_sha256",
            "worker_image_id",
        }
        or sealed_runtime.get("bundle_sha256") is not None
        or sealed_runtime.get("execution_mode") != "exact-scheduler-image-v1"
        or type(sealed_runtime.get("file_count")) is not int
        or sealed_runtime["file_count"] < 1
        or sealed_runtime.get("runtime_cleanup_complete") is not True
        or not isinstance(sealed_runtime.get("runtime_tree_sha256"), str)
        or _DIGEST.fullmatch(sealed_runtime["runtime_tree_sha256"]) is None
        or sealed_runtime.get("worker_image_id") != scheduler_image_id
    ):
        raise AdmissionError("capacity receipt sealed worker runtime is invalid")
    session_cleanup = value.get("session_cleanup")
    session_fields = {
        "lock_acquired",
        "preflight_required",
        "preflight_verified_zero",
        "final_verified_zero",
        "state_file_removed",
        "poll_attempts",
        "successful_polls",
        "final_zero_scans",
        "active_max",
        "pending_create_max",
        "pending_destroy_max",
        "failed_create_max",
        "failed_destroy_max",
        "failure_generation_changed",
        "quiet_window_observed",
        "error_count",
        "error_sha256",
        "stale_worker_cleanup_required",
        "stale_worker_cleanup_verified",
        "stale_worker_containers_removed",
        "worker_artifact_cleanup_required",
        "worker_artifact_cleanup_verified",
    }
    if (
        not isinstance(session_cleanup, dict)
        or set(session_cleanup) != session_fields
        or any(
            type(session_cleanup.get(field)) is not bool
            for field in (
                "lock_acquired",
                "preflight_required",
                "preflight_verified_zero",
                "final_verified_zero",
                "state_file_removed",
                "failure_generation_changed",
                "quiet_window_observed",
                "stale_worker_cleanup_required",
                "stale_worker_cleanup_verified",
                "worker_artifact_cleanup_required",
                "worker_artifact_cleanup_verified",
            )
        )
        or any(
            session_cleanup.get(field) is not True
            for field in (
                "lock_acquired",
                "preflight_verified_zero",
                "final_verified_zero",
                "state_file_removed",
                "quiet_window_observed",
                "stale_worker_cleanup_verified",
                "worker_artifact_cleanup_required",
                "worker_artifact_cleanup_verified",
            )
        )
        or session_cleanup.get("failure_generation_changed") is not False
        or any(
            type(session_cleanup.get(field)) is not int
            or session_cleanup.get(field) != 0
            for field in (
                "poll_attempts",
                "successful_polls",
                "final_zero_scans",
                "active_max",
                "pending_create_max",
                "pending_destroy_max",
                "failed_create_max",
                "failed_destroy_max",
                "error_count",
            )
        )
        or session_cleanup.get("error_sha256") != []
        or type(session_cleanup.get("stale_worker_containers_removed")) is not int
        or session_cleanup["stale_worker_containers_removed"] < 0
        or session_cleanup.get("stale_worker_cleanup_required")
        is not session_cleanup.get("preflight_required")
        or (
            session_cleanup.get("stale_worker_cleanup_required") is False
            and session_cleanup.get("stale_worker_containers_removed") != 0
        )
    ):
        raise AdmissionError("capacity receipt cache lifecycle cleanup is invalid")

    gates = value.get("gates")
    if not isinstance(gates, list) or len(gates) != len(_CAPACITY_GATE_NAMES):
        raise AdmissionError("capacity receipt gate set is invalid")
    by_name: dict[str, Mapping[str, Any]] = {}
    for gate in gates:
        name = gate.get("name") if isinstance(gate, Mapping) else None
        if (
            not isinstance(name, str)
            or name in by_name
            or name not in _CAPACITY_GATE_NAMES
            or gate.get("passed") is not True
        ):
            raise AdmissionError("capacity receipt gate set is invalid")
        by_name[name] = gate
    if set(by_name) != _CAPACITY_GATE_NAMES:
        raise AdmissionError("capacity receipt gate set is incomplete")
    if (
        by_name["throughput"].get("minimum_page_units_per_day")
        != _CAPACITY_MIN_PAGE_UNITS_PER_DAY
        or _capacity_number(
            by_name["throughput"].get("observed_page_units_per_day"),
            field="throughput gate",
        )
        != throughput
        or by_name["memory"].get("maximum_aggregate_memory_bytes")
        != _CAPACITY_MAX_COMBINED_RSS_BYTES
        or _capacity_number(
            by_name["memory"].get("observed_max_aggregate_memory_bytes"),
            field="memory gate",
        )
        != peak_rss
        or by_name["sustained_duration"].get("required_seconds")
        != _CAPACITY_DURATION_SECONDS
        or _capacity_number(
            by_name["sustained_duration"].get("observed_seconds"),
            field="sustained duration gate",
        )
        != observed
        or by_name["network_isolation"].get("observed_network_requests") != 0
        or by_name["paid_traffic"].get("observed_paid_bytes") != 0
        or by_name["paid_traffic"].get("observed_paid_route_requests") != 0
        or by_name["cache_seed"].get("observed_seed_sha256") != _CAPACITY_SEED_SHA256
        or by_name["monitoring_evidence"].get("sample_count") != len(samples)
    ):
        raise AdmissionError("capacity receipt gate evidence is inconsistent")

    release = value.get("runtime_release_identity")
    runtime = value.get("runtime_identity")
    deployment = value.get("production_deployment")
    common_hashes = (
        deployment.get("protected_config_hashes")
        if isinstance(deployment, dict)
        else None
    )
    protected_inputs = (
        deployment.get("protected_inputs") if isinstance(deployment, dict) else None
    )
    running = (
        deployment.get("running_admission") if isinstance(deployment, dict) else None
    )
    running_images = running.get("images") if isinstance(running, dict) else None
    expected_input_labels = {
        "build-attestation",
        "build-manifest",
        "deployment-attestation",
        "compose:compose.yaml",
        "compose:compose.seaweedfs-supervised.yaml",
        "common-digest-override",
        "compose-env:0",
        "compose-env:1",
        "compose-env:2",
    }
    expected_compose_input_labels = expected_input_labels - {
        "build-attestation",
        "build-manifest",
        "deployment-attestation",
    }
    common_network_names = {
        _EXPECTED_NETWORK_DEFINITIONS[name]["name"]
        for service in COMMON_PROTECTED_SERVICES
        for name in _EXPECTED_NETWORKS[service]
    }
    if (
        not isinstance(release, dict)
        or set(release)
        != {"release_revision", "manifest_sha256", "worker_image_id", "git_clean"}
        or not isinstance(runtime, dict)
        or not isinstance(deployment, dict)
        or set(deployment)
        != {
            "admission_mode",
            "common_digest_override_sha256",
            "deployment_attestation_sha256",
            "gateway_digest_override_sha256",
            "payload_revision",
            "protected_bindings",
            "protected_config_hashes",
            "protected_inputs",
            "protected_payload_image_ids",
            "provenance_manifest_sha256",
            "release_revision",
            "running_admission",
            "source_tree_sha256",
        }
        or deployment.get("admission_mode") != _CAPACITY_MODE
        or deployment.get("gateway_digest_override_sha256") is not None
        or not isinstance(deployment.get("common_digest_override_sha256"), str)
        or _DIGEST.fullmatch(deployment["common_digest_override_sha256"]) is None
        or not isinstance(common_hashes, dict)
        or set(common_hashes) != set(COMMON_PROTECTED_SERVICES)
        or any(
            not isinstance(item, str) or _DIGEST.fullmatch(item) is None
            for item in common_hashes.values()
        )
        or not isinstance(common_config_hashes, Mapping)
        or set(common_config_hashes) != set(COMMON_PROTECTED_SERVICES)
        or any(
            not isinstance(item, str) or _DIGEST.fullmatch(item) is None
            for item in common_config_hashes.values()
        )
        or common_hashes != dict(common_config_hashes)
        or not isinstance(running_images_evidence, (list, tuple))
        or any(not isinstance(item, Mapping) for item in running_images_evidence)
        or not isinstance(protected_inputs, dict)
        or set(protected_inputs) != expected_input_labels
        or any(
            not isinstance(item, dict)
            or set(item) != {"identity", "sha256"}
            or not isinstance(item.get("identity"), list)
            or len(item["identity"]) != 9
            or any(type(field) is not int or field < 0 for field in item["identity"])
            or not isinstance(item.get("sha256"), str)
            or _DIGEST.fullmatch(item["sha256"]) is None
            for item in protected_inputs.values()
        )
        or not isinstance(compose_inputs_evidence, Mapping)
        or set(compose_inputs_evidence) != expected_compose_input_labels
        or any(
            not isinstance(item, ProtectedComposeInputEvidence)
            or not isinstance(item.raw, bytes)
            or not item.raw
            or len(item.identity) != 9
            or any(type(field) is not int or field < 0 for field in item.identity)
            for item in compose_inputs_evidence.values()
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
        or running.get("projects") != {COMMON_PROJECT: list(COMMON_PROTECTED_SERVICES)}
        or not isinstance(running.get("apparmor_profile"), str)
        or not running["apparmor_profile"]
        or not isinstance(running.get("docker_security_options"), list)
        or not isinstance(running_images, list)
        or len(running_images) != len(COMMON_PROTECTED_SERVICES)
        or any(
            not isinstance(record, dict)
            or set(record) != {"container_id", "final_image", "image_id", "service"}
            or record.get("service") != service
            or record.get("final_image") != bindings_evidence.bindings[service]
            or record.get("image_id")
            != bindings_evidence.validated_payload_image_ids[service]
            or not isinstance(record.get("container_id"), str)
            or _CONTAINER_ID.fullmatch(record["container_id"]) is None
            for service, record in zip(
                COMMON_PROTECTED_SERVICES, running_images, strict=True
            )
        )
        or not isinstance(running.get("networks"), list)
        or {
            record.get("name")
            for record in running["networks"]
            if isinstance(record, dict)
        }
        != common_network_names
        or not isinstance(running.get("volumes"), list)
        or len(running["volumes"]) != 1
        or not isinstance(running["volumes"][0], dict)
        or running["volumes"][0].get("name") != "soccerdata_cache"
        or release.get("release_revision")
        != bindings_evidence.validated_release_revision
        or release.get("worker_image_id") != scheduler_image_id
        or release.get("git_clean") is not True
        or not isinstance(release.get("manifest_sha256"), str)
        or _DIGEST.fullmatch(release["manifest_sha256"]) is None
        or runtime.get("git_revision") != release.get("release_revision")
        or runtime.get("git_clean") is not True
        or runtime.get("manifest_sha256") != release.get("manifest_sha256")
        or runtime.get("worker_image_id") != scheduler_image_id
        or runtime.get("production_deployment") != deployment
        or deployment.get("release_revision")
        != bindings_evidence.validated_release_revision
        or deployment.get("payload_revision")
        != bindings_evidence.validated_payload_revision
        or deployment.get("provenance_manifest_sha256")
        != bindings_evidence.validated_manifest_sha256
        or deployment.get("source_tree_sha256")
        != bindings_evidence.validated_source_tree_sha256
        or deployment.get("protected_bindings") != dict(bindings_evidence.bindings)
        or deployment.get("protected_payload_image_ids")
        != dict(bindings_evidence.validated_payload_image_ids)
        or bindings_evidence.validated_payload_image_ids.get("airflow-scheduler")
        != scheduler_image_id
    ):
        raise AdmissionError("capacity receipt differs from the current release")

    assert isinstance(release, dict)
    assert isinstance(runtime, dict)
    assert isinstance(deployment, dict)
    assert isinstance(protected_inputs, dict)
    assert isinstance(running, dict)
    assert isinstance(running_images, list)
    running_images_by_service = {
        str(record["service"]): record
        for record in running_images
        if isinstance(record, dict) and isinstance(record.get("service"), str)
    }
    current_common_images = [
        dict(record)
        for record in running_images_evidence
        if isinstance(record, Mapping)
        and record.get("service") in COMMON_PROTECTED_SERVICES
    ]
    if current_common_images != running_images:
        raise AdmissionError(
            "capacity receipt differs from current running container evidence"
        )

    if (
        running.get("apparmor_profile") != "docker-default (enforce)"
        or running.get("docker_security_options")
        != [
            "name=apparmor",
            "name=cgroupns",
            "name=seccomp,profile=builtin",
        ]
        or running.get("volumes")
        != [
            {
                "driver": "local",
                "mountpoint": "/var/lib/docker/volumes/soccerdata_cache/_data",
                "name": "soccerdata_cache",
            }
        ]
    ):
        raise AdmissionError("capacity receipt running security summary is invalid")

    expected_network_logicals = tuple(
        sorted(
            {
                logical_name
                for service in COMMON_PROTECTED_SERVICES
                for logical_name in _EXPECTED_NETWORKS[service]
            }
        )
    )
    networks = running.get("networks")
    if (
        not isinstance(networks, list)
        or len(networks) != len(expected_network_logicals)
        or [
            record.get("logical_name") if isinstance(record, dict) else None
            for record in networks
        ]
        != list(expected_network_logicals)
    ):
        raise AdmissionError("capacity receipt running network summary is invalid")
    network_ids: set[str] = set()
    network_subnets: set[str] = set()
    for logical_name, record in zip(expected_network_logicals, networks, strict=True):
        if (
            not isinstance(record, dict)
            or set(record) != {"id", "logical_name", "name", "subnet"}
            or record.get("logical_name") != logical_name
            or record.get("name") != _EXPECTED_NETWORK_DEFINITIONS[logical_name]["name"]
            or not isinstance(record.get("id"), str)
            or _CONTAINER_ID.fullmatch(record["id"]) is None
            or record["id"] in network_ids
            or not isinstance(record.get("subnet"), str)
            or record["subnet"] in network_subnets
        ):
            raise AdmissionError("capacity receipt running network summary is invalid")
        try:
            subnet = ipaddress.ip_network(record["subnet"], strict=True)
        except ValueError as exc:
            raise AdmissionError(
                "capacity receipt running network summary is invalid"
            ) from exc
        if (
            subnet.version != 4
            or not subnet.is_private
            or subnet.is_loopback
            or subnet.prefixlen < 16
        ):
            raise AdmissionError("capacity receipt running network summary is invalid")
        network_ids.add(record["id"])
        network_subnets.add(record["subnet"])

    build_attestation_sha256 = hashlib.sha256(
        bindings_evidence.build_attestation_raw
    ).hexdigest()
    build_manifest_sha256 = hashlib.sha256(
        bindings_evidence.build_manifest_raw
    ).hexdigest()
    deployment_attestation_sha256 = hashlib.sha256(
        bindings_evidence.deployment_attestation_raw
    ).hexdigest()
    bound_inputs = {
        "build-attestation": (
            list(bindings_evidence.build_attestation_identity),
            build_attestation_sha256,
        ),
        "build-manifest": (
            list(bindings_evidence.build_manifest_identity),
            build_manifest_sha256,
        ),
        "deployment-attestation": (
            list(bindings_evidence.deployment_attestation_identity),
            deployment_attestation_sha256,
        ),
    }
    if (
        build_manifest_sha256 != bindings_evidence.validated_manifest_sha256
        or deployment.get("deployment_attestation_sha256")
        != deployment_attestation_sha256
        or deployment.get("common_digest_override_sha256")
        != protected_inputs["common-digest-override"]["sha256"]
        or any(
            protected_inputs[label] != {"identity": identity, "sha256": sha256}
            for label, (identity, sha256) in bound_inputs.items()
        )
    ):
        raise AdmissionError("capacity receipt protected input binding is invalid")
    if any(
        protected_inputs[label]
        != {
            "identity": list(evidence.identity),
            "sha256": evidence.sha256,
        }
        for label, evidence in compose_inputs_evidence.items()
    ):
        raise AdmissionError(
            "capacity receipt differs from current protected Compose inputs"
        )

    contract_files, runtime_lock_sha256, runtime_tree_sha256 = (
        _capacity_runtime_contract_evidence()
    )
    expected_runtime_file_sha256 = _capacity_runtime_file_evidence()
    if expected_runtime_file_sha256[
        _CAPACITY_RUNTIME_CONTRACT_RELATIVE
    ] != runtime_lock_sha256 or any(
        expected_runtime_file_sha256[relative] != contract_files[relative]
        for relative in _CAPACITY_SEALED_RUNTIME_PATHS
        if relative != _CAPACITY_RUNTIME_CONTRACT_RELATIVE
    ):
        raise AdmissionError("capacity receipt sealed runtime differs from its lock")
    expected_runtime_file_sha256.update(
        {
            f"external:{label}": item["sha256"]
            for label, item in protected_inputs.items()
        }
    )
    identity_material = {
        "file_sha256": expected_runtime_file_sha256,
        "python_executable": "/usr/local/bin/python",
        "python_prefix": "/usr/local",
        "python_version": "3.11",
        "dependency_versions": {"curl_cffi": _CAPACITY_CURL_CFFI_VERSION},
        "worker_image_id": scheduler_image_id,
        "production_deployment": deployment,
    }
    expected_runtime_manifest = hashlib.sha256(
        _authority_canonical_bytes(identity_material)
    ).hexdigest()
    if (
        set(runtime) != _CAPACITY_RUNTIME_IDENTITY_FIELDS
        or runtime.get("file_sha256") != expected_runtime_file_sha256
        or runtime.get("python_executable") != "/usr/local/bin/python"
        or runtime.get("python_prefix") != "/usr/local"
        or runtime.get("python_version") != "3.11"
        or runtime.get("dependency_versions")
        != {"curl_cffi": _CAPACITY_CURL_CFFI_VERSION}
        or runtime.get("manifest_sha256") != expected_runtime_manifest
        or release.get("manifest_sha256") != expected_runtime_manifest
        or sealed_runtime.get("file_count") != len(_CAPACITY_SEALED_RUNTIME_PATHS)
        or sealed_runtime.get("runtime_tree_sha256") != runtime_tree_sha256
        or any(
            expected_runtime_file_sha256.get(relative) is None
            for relative in _CAPACITY_SEALED_RUNTIME_PATHS
        )
    ):
        raise AdmissionError("capacity receipt runtime identity is not sealed")

    baseline = value.get("baseline_containers")
    if not isinstance(baseline, list) or [
        record.get("name") if isinstance(record, dict) else None for record in baseline
    ] != list(COMMON_PROTECTED_SERVICES):
        raise AdmissionError("capacity receipt baseline container set is invalid")
    for service, record in zip(COMMON_PROTECTED_SERVICES, baseline, strict=True):
        _validate_capacity_container_record(
            record,
            service=service,
            running_images=running_images_by_service,
        )
    _validate_capacity_samples(
        samples,
        observed=observed,
        total_elapsed=total_elapsed,
        completed_runs=completed_runs_value,
        total_page_units=page_units,
        baseline=baseline,
        running_images=running_images_by_service,
        report=value,
    )

    return {
        "duration_seconds_observed": value["duration_seconds_observed"],
        "mode": value["mode"],
        "peak_combined_rss_bytes": value["peak_combined_rss_bytes"],
        "projected_page_units_per_day": value["projected_page_units_per_day"],
        "report_sha256": claimed,
        "runtime_release_identity": dict(release),
        "schema_version": value["schema_version"],
        "status": "accepted-v1",
        "worker_count": value["worker_count"],
    }


def _common_parser() -> argparse.ArgumentParser:
    common = argparse.ArgumentParser(add_help=False)
    common.add_argument("--root", type=Path, default=Path.cwd())
    common.add_argument("--attestation", type=Path)
    common.add_argument("--manifest", type=Path)
    common.add_argument("--deployment-attestation", type=Path, required=True)
    return common


def _canonical_rollout_services(
    selected_services: Sequence[str],
) -> tuple[str, ...]:
    """Require the complete paid boundary before declaring rollout GO."""

    selected = tuple(selected_services)
    if (
        len(selected) != len(PROTECTED_SERVICES)
        or frozenset(selected) != _PROTECTED_SERVICE_SET
    ):
        raise AdmissionError(
            "rollout production admission requires every protected service exactly once"
        )
    return PROTECTED_SERVICES


def _parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description=__doc__)
    common = _common_parser()
    commands = parser.add_subparsers(dest="command", required=True)
    generate = commands.add_parser("generate-override", parents=[common])
    generate.add_argument("--common-output", type=Path, required=True)
    generate.add_argument("--gateway-output", type=Path, required=True)
    pause_snapshot = commands.add_parser(
        "snapshot-cutover-dag-pauses", parents=[common]
    )
    pause_snapshot.add_argument("--output", type=Path, required=True)
    quiescence = commands.add_parser("verify-cutover-quiescence", parents=[common])
    quiescence.add_argument("--output", type=Path, required=True)
    cutover = commands.add_parser("verify-cutover-inventory", parents=[common])
    cutover.add_argument("--output", type=Path, required=True)
    rollback = commands.add_parser("verify-rollback-bundle", parents=[common])
    rollback.add_argument("--inventory", type=Path, required=True)
    rollback.add_argument("--post-cleanup", action="store_true")
    rollback.add_argument("--output", type=Path, required=True)
    vacancy = commands.add_parser("verify-create-vacancy", parents=[common])
    vacancy.add_argument("--service", choices=PROTECTED_SERVICES, required=True)
    vacancy.add_argument("--output", type=Path, required=True)
    capture = commands.add_parser("capture-created-object", parents=[common])
    capture.add_argument("--vacancy-receipt", type=Path, required=True)
    capture.add_argument("--allow-partial", action="store_true")
    capture.add_argument("--output", type=Path, required=True)
    rendered = commands.add_parser("verify-rendered", parents=[common])
    rendered.add_argument("--common-override", type=Path, required=True)
    rendered.add_argument("--gateway-override", type=Path, required=True)
    rendered.add_argument("--env-file", type=Path, action="append", required=True)
    rendered.add_argument("--provider-policy", type=Path, required=True)
    rendered.add_argument("--owner-secret-file", type=Path, required=True)
    rendered.add_argument("--provider-quota-receipt", type=Path, required=True)
    rendered.add_argument("--output", type=Path, required=True)
    common_rendered = commands.add_parser("verify-common-rendered", parents=[common])
    common_rendered.add_argument("--common-override", type=Path, required=True)
    common_rendered.add_argument(
        "--env-file", type=Path, action="append", required=True
    )
    common_rendered.add_argument("--output", type=Path, required=True)
    created = commands.add_parser("post-create", parents=[common])
    created.add_argument("--common-override", type=Path, required=True)
    created.add_argument("--gateway-override", type=Path, required=True)
    created.add_argument("--env-file", type=Path, action="append", required=True)
    created.add_argument("--provider-policy", type=Path, required=True)
    created.add_argument("--owner-secret-file", type=Path, required=True)
    created.add_argument("--provider-quota-receipt", type=Path, required=True)
    created.add_argument("--service", action="append", required=True)
    common_created = commands.add_parser("post-create-common", parents=[common])
    common_created.add_argument("--common-override", type=Path, required=True)
    common_created.add_argument("--env-file", type=Path, action="append", required=True)
    common_created.add_argument(
        "--service",
        action="append",
        choices=COMMON_PROTECTED_SERVICES,
        required=True,
    )
    running = commands.add_parser("verify-running", parents=[common])
    running.add_argument("--common-override", type=Path, required=True)
    running.add_argument("--gateway-override", type=Path, required=True)
    running.add_argument("--env-file", type=Path, action="append", required=True)
    running.add_argument("--provider-policy", type=Path, required=True)
    running.add_argument("--owner-secret-file", type=Path, required=True)
    running.add_argument("--deployment-admission-receipt", type=Path, required=True)
    running.add_argument(
        "--capacity-receipt",
        type=Path,
        help=(
            "protected canonical six-hour cache-capacity receipt; required "
            "for final --rollout-id GO"
        ),
    )
    rollout_mode = running.add_mutually_exclusive_group()
    rollout_mode.add_argument(
        "--rollout-id",
        help=(
            "explicit staged-rollout identity; when supplied, verify-running "
            "also requires its immutable ops ledger to be fully accepted"
        ),
    )
    rollout_mode.add_argument(
        "--issuance-rollout-id",
        help=(
            "current signed rollout identity; when supplied, verify-running "
            "replays its exact live predecessor promotion evidence before "
            "daily paid-pointer issuance"
        ),
    )
    running.add_argument("--service", action="append", required=True)
    return parser


def main(argv: Sequence[str] | None = None) -> int:
    if (
        not sys.flags.isolated
        or not sys.flags.no_site
        or not sys.flags.ignore_environment
    ):
        print(
            "WhoScored production admission blocked: invoke exact Python with -I -S",
            file=sys.stderr,
        )
        return EXIT_CONFIG
    try:
        _assert_clean_control_environment()
    except AdmissionError as exc:
        print(f"WhoScored production admission blocked: {exc}", file=sys.stderr)
        return EXIT_CONFIG
    args = _parser().parse_args(argv)
    root = _absolute(args.root)
    canonical_attestation = root / provenance.ATTESTATION_RELATIVE
    canonical_manifest = root / provenance.MANIFEST_RELATIVE
    attestation = _absolute(args.attestation or canonical_attestation)
    manifest = _absolute(args.manifest or canonical_manifest)
    deployment = _absolute(args.deployment_attestation)
    try:
        if (
            args.command == "verify-running"
            and args.capacity_receipt is not None
            and args.rollout_id is None
        ):
            raise AdmissionError(
                "--capacity-receipt is only valid with final --rollout-id"
            )
        selected_services = (
            _canonical_rollout_services(args.service)
            if args.command == "verify-running"
            and (args.rollout_id is not None or args.issuance_rollout_id is not None)
            else getattr(args, "service", ())
        )
        _assert_canonical_release(root)
        if attestation != canonical_attestation or manifest != canonical_manifest:
            raise AdmissionError(
                "provenance manifest and attestation must be canonical release paths"
            )
        bindings_evidence = validate_bindings_with_evidence(
            root=root,
            attestation_path=attestation,
            manifest_path=manifest,
            deployment_attestation_path=deployment,
        )
        bindings = dict(bindings_evidence.bindings)
        if args.command == "generate-override":
            common_output = _absolute(args.common_output)
            gateway_output = _absolute(args.gateway_output)
            if common_output == gateway_output:
                raise AdmissionError("split Compose overrides must be distinct")
            write_new_regular_file(
                common_output,
                compose_override_bytes(bindings, COMMON_PROTECTED_SERVICES),
            )
            write_new_regular_file(
                gateway_output,
                compose_override_bytes(bindings, GATEWAY_PROTECTED_SERVICES),
            )
            report: dict[str, Any] = {
                "outputs": {
                    COMMON_PROJECT: str(common_output),
                    GATEWAY_PROJECT: str(gateway_output),
                },
                "projects": {
                    COMMON_PROJECT: list(COMMON_PROTECTED_SERVICES),
                    GATEWAY_PROJECT: list(GATEWAY_PROTECTED_SERVICES),
                },
                "schema_version": 2,
                "status": "overrides-created-v2",
            }
        elif args.command in {
            "snapshot-cutover-dag-pauses",
            "verify-cutover-quiescence",
            "verify-create-vacancy",
            "capture-created-object",
            "verify-cutover-inventory",
            "verify-rollback-bundle",
        }:
            output = _absolute(args.output)
            if args.command == "snapshot-cutover-dag-pauses":
                evidence = snapshot_cutover_dag_pauses()
            elif args.command == "verify-cutover-quiescence":
                evidence = verify_cutover_quiescence()
            elif args.command == "verify-create-vacancy":
                evidence = verify_create_vacancy(args.service)
            elif args.command == "capture-created-object":
                evidence = capture_created_object(
                    _read_protected_canonical_object(
                        _absolute(args.vacancy_receipt),
                        label="create-vacancy receipt",
                    ),
                    allow_partial=args.allow_partial,
                )
            elif args.command == "verify-rollback-bundle":
                evidence = verify_rollback_bundle(
                    _read_protected_canonical_object(
                        _absolute(args.inventory),
                        label="legacy cutover inventory",
                    ),
                    post_cleanup=args.post_cleanup,
                )
            else:
                evidence = verify_cutover_inventory()
            write_new_regular_file(output, _canonical_bytes(evidence))
            report = {
                (
                    "inventory"
                    if args.command == "verify-cutover-inventory"
                    else "evidence"
                ): evidence,
                "output": str(output),
                "output_sha256": hashlib.sha256(_canonical_bytes(evidence)).hexdigest(),
                "schema_version": 1,
                "status": (
                    "cutover-inventory-recorded-v1"
                    if args.command == "verify-cutover-inventory"
                    else f"{args.command}-recorded-v1"
                ),
            }
        elif args.command in {"verify-common-rendered", "post-create-common"}:
            common_override = _absolute(args.common_override)
            env_files = tuple(_absolute(path) for path in args.env_file)
            if env_files != _COMMON_ONLY_ENV_FILES:
                raise AdmissionError(
                    "common-only admission requires the exact three production "
                    "environment files in canonical order"
                )
            if args.command == "post-create-common" and len(args.service) != 1:
                raise AdmissionError(
                    "common post-create admission requires exactly one service"
                )
            protected_input_evidence = _assert_protected_compose_inputs(
                (
                    root / "compose.yaml",
                    root / "compose.seaweedfs-supervised.yaml",
                    common_override,
                    *env_files,
                )
            )
            projections, config_hashes, config_files, rendered = (
                render_attested_common_project(
                    bindings,
                    root=root,
                    common_override_path=common_override,
                    env_files=env_files,
                    protected_inputs={
                        path: evidence.raw
                        for path, evidence in protected_input_evidence.items()
                    },
                )
            )
            if args.command == "verify-common-rendered":
                output = _absolute(args.output)
                _assert_compose_input_evidence_unchanged(protected_input_evidence)
                write_new_regular_file(output, _canonical_bytes(rendered))
                report = {
                    "config_hashes": config_hashes,
                    "deployment_attestation": {
                        "path": str(deployment),
                        "sha256": hashlib.sha256(
                            bindings_evidence.deployment_attestation_raw
                        ).hexdigest(),
                    },
                    "output": str(output),
                    "projects": {
                        COMMON_PROJECT: list(COMMON_PROTECTED_SERVICES),
                    },
                    "schema_version": 1,
                    "status": "common-rendered-admitted-v1",
                }
            else:
                report = verify_created_containers(
                    bindings,
                    project=COMMON_PROJECT,
                    selected_services=args.service,
                    projections=projections,
                    config_hashes=config_hashes,
                    config_files=config_files,
                    env_files=env_files,
                )
                _assert_compose_input_evidence_unchanged(protected_input_evidence)
                report["admission_scope"] = "common-only-v1"
                report["receipt_type"] = "common-post-create-v1"
                report["schema_version"] = 2
                report["status"] = "common-created-admitted-v1"
        else:
            provider_policy = validate_provider_policy(
                _absolute(args.provider_policy),
                owner_secret_path=_absolute(args.owner_secret_file),
            )
            provider_authority: dict[str, str | int] = {
                **provider_policy,
                "provider_policy_sha256": str(provider_policy["document_sha256"]),
            }
            provider_quota_receipt: dict[str, str | int] | None = None
            deployment_admission_receipt: dict[str, str] | None = None
            if args.command == "verify-running":
                deployment_admission_receipt = validate_deployment_admission_receipt(
                    _absolute(args.deployment_admission_receipt),
                    deployment_attestation_path=deployment,
                    provider_policy=provider_policy,
                )
            else:
                provider_quota_receipt = validate_provider_quota_receipt(
                    _absolute(args.provider_quota_receipt),
                    provider_policy_path=_absolute(args.provider_policy),
                    owner_secret_path=_absolute(args.owner_secret_file),
                )
                provider_authority = provider_quota_receipt
            common_override = _absolute(args.common_override)
            gateway_override = _absolute(args.gateway_override)
            if common_override == gateway_override:
                raise AdmissionError("split Compose overrides must be distinct")
            env_files = tuple(_absolute(path) for path in args.env_file)
            protected_input_evidence = _assert_protected_compose_inputs(
                (
                    root / "compose.yaml",
                    root / "compose.seaweedfs-supervised.yaml",
                    root / "deploy/whoscored/gateway.compose.yaml",
                    common_override,
                    gateway_override,
                    *env_files,
                )
            )
            protected_inputs = {
                path: evidence.raw
                for path, evidence in protected_input_evidence.items()
            }
            projections, config_hashes, config_files, rendered = (
                render_attested_projects(
                    bindings,
                    root=root,
                    common_override_path=common_override,
                    gateway_override_path=gateway_override,
                    env_files=env_files,
                    provider_authority=provider_authority,
                    protected_inputs=protected_inputs,
                )
            )
            if args.command == "verify-rendered":
                assert provider_quota_receipt is not None
                output = _absolute(args.output)
                write_new_regular_file(output, _canonical_bytes(rendered))
                report = {
                    "config_hashes": config_hashes,
                    "deployment_attestation": {
                        "path": str(deployment),
                        "sha256": hashlib.sha256(
                            provenance.read_protected_regular_file(
                                deployment, label="deployment attestation"
                            )
                        ).hexdigest(),
                    },
                    "output": str(output),
                    "provider_quota_receipt": provider_quota_receipt,
                    "projects": {
                        COMMON_PROJECT: list(COMMON_PROTECTED_SERVICES),
                        GATEWAY_PROJECT: list(GATEWAY_PROTECTED_SERVICES),
                    },
                    "schema_version": 2,
                    "status": "rendered-admitted-v2",
                }
            else:
                report = verify_created_containers(
                    bindings,
                    project=_SERVICE_PROJECT,
                    selected_services=selected_services,
                    projections=projections,
                    config_hashes=config_hashes,
                    config_files=config_files,
                    env_files=env_files,
                    expected_state=(
                        "running" if args.command == "verify-running" else "created"
                    ),
                )
                if args.command == "verify-running":
                    assert deployment_admission_receipt is not None
                    report["deployment_admission_receipt"] = (
                        deployment_admission_receipt
                    )
                    report["provider_policy"] = provider_policy
                    rollout_id = args.rollout_id or args.issuance_rollout_id
                    if rollout_id is not None:
                        rollout_authority = (
                            validate_current_rollout_authority(
                                rollout_id,
                                owner_secret_path=_absolute(args.owner_secret_file),
                                provider_policy=provider_policy,
                            )
                            if args.rollout_id is not None
                            else validate_current_issuance_authority(
                                rollout_id,
                                owner_secret_path=_absolute(args.owner_secret_file),
                                provider_policy=provider_policy,
                            )
                        )
                        scheduler_records = [
                            record
                            for record in report.get("images", [])
                            if record.get("service") == "airflow-scheduler"
                        ]
                        if len(scheduler_records) != 1:
                            raise AdmissionError(
                                "rollout replay lacks admitted scheduler identity"
                            )
                        if args.rollout_id is not None:
                            if args.capacity_receipt is None:
                                raise AdmissionError(
                                    "final rollout GO requires --capacity-receipt"
                                )
                            authority_body = rollout_authority.get("authority")
                            if not isinstance(authority_body, Mapping):
                                raise AdmissionError(
                                    "current rollout capacity authority is invalid"
                                )
                            _assert_compose_input_evidence_unchanged(
                                protected_input_evidence
                            )
                            report["capacity_receipt"] = validate_capacity_receipt(
                                _absolute(args.capacity_receipt),
                                capacity_receipt_sha256=str(
                                    authority_body.get("capacity_receipt_sha256")
                                ),
                                bindings_evidence=bindings_evidence,
                                scheduler_image_id=str(
                                    scheduler_records[0].get("image_id")
                                ),
                                compose_inputs_evidence={
                                    "compose:compose.yaml": protected_input_evidence[
                                        root / "compose.yaml"
                                    ],
                                    "compose:compose.seaweedfs-supervised.yaml": (
                                        protected_input_evidence[
                                            root / "compose.seaweedfs-supervised.yaml"
                                        ]
                                    ),
                                    "common-digest-override": (
                                        protected_input_evidence[common_override]
                                    ),
                                    **{
                                        f"compose-env:{index}": (
                                            protected_input_evidence[path]
                                        )
                                        for index, path in enumerate(env_files)
                                    },
                                },
                                common_config_hashes={
                                    service: config_hashes[service]
                                    for service in COMMON_PROTECTED_SERVICES
                                },
                                running_images_evidence=tuple(report.get("images", ())),
                            )
                            report["rollout_acceptance"] = verify_rollout_acceptance(
                                rollout_id,
                                rollout_authority=rollout_authority,
                                scheduler_container_id=scheduler_records[0][
                                    "container_id"
                                ],
                            )
                        else:
                            if args.capacity_receipt is not None:
                                raise AdmissionError(
                                    "--capacity-receipt is only valid with --rollout-id"
                                )
                            report["issuance_rollout"] = verify_issuance_rollout(
                                rollout_id,
                                rollout_authority=rollout_authority,
                                scheduler_container_id=scheduler_records[0][
                                    "container_id"
                                ],
                            )
                else:
                    assert provider_quota_receipt is not None
                    report["provider_quota_receipt"] = provider_quota_receipt
    except AdmissionError as exc:
        print(f"WhoScored production admission blocked: {exc}", file=sys.stderr)
        return EXIT_CONFIG
    sys.stdout.buffer.write(_canonical_bytes(report))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
