#!/usr/bin/env python3
"""Build and administer signed WhoScored paid-proxy campaign approvals.

The CLI intentionally never accepts an HMAC secret on the command line.  Read
it from a protected environment variable or file so it is absent from shell
history and process listings.  Every generated document is written atomically
with mode ``0600``; existing files are never overwritten unless explicitly
requested.
"""

# ruff: noqa: E402 -- the trust anchor must run before every non-built-in import

from __future__ import annotations

import sys as _whoscored_bootstrap_sys

_whoscored_source = __file__
if not _whoscored_source.startswith("/"):
    raise RuntimeError("WhoScored entrypoint requires an absolute source path")
_whoscored_production = _whoscored_source.startswith("/opt/airflow/")
_whoscored_root = "/opt/airflow" if _whoscored_production else _whoscored_source.rsplit("/scripts/", 1)[0]
if _whoscored_production:
    if getattr(_whoscored_bootstrap_sys, "_whoscored_runtime_startup_schema", None) != 2:
        raise RuntimeError("image-baked WhoScored startup anchor is required")
elif getattr(_whoscored_bootstrap_sys, "_whoscored_runtime_startup_root", None) != _whoscored_root:
    _whoscored_anchor_path = (
        _whoscored_root + "/docker/images/airflow/whoscored_runtime_startup.py"
    )
    _whoscored_anchor_globals = {
        "__builtins__": __builtins__,
        "sys": _whoscored_bootstrap_sys,
        "_WHOSCORED_RUNTIME_ROOT": _whoscored_root,
        "_WHOSCORED_REQUIRE_FULL_ATTESTATION": False,
    }
    with open(_whoscored_anchor_path, "rb") as _whoscored_anchor_handle:
        _whoscored_anchor_source = _whoscored_anchor_handle.read()
    exec(
        compile(_whoscored_anchor_source, _whoscored_anchor_path, "exec"),
        _whoscored_anchor_globals,
    )
_WHOSCORED_RUNTIME_CONTRACT = (
    _whoscored_bootstrap_sys._load_whoscored_runtime_contract(_whoscored_root)
)

import argparse
import hashlib
import json
import os
import stat
import sys
import tempfile
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import Mapping, Sequence

from scrapers.whoscored.proxy_campaign import (
    MAX_PROXY_CAMPAIGN_VALIDITY,
    PROXY_APPROVAL_HMAC_SECRET_ENV,
    PROXY_CAMPAIGN_METER,
    PROXY_CAMPAIGN_SCHEMA_VERSION,
    PROXY_CAMPAIGN_SIGNATURE_ALGORITHM,
    PROXY_CAMPAIGN_SOURCE,
    PROXY_LEDGER_HMAC_SECRET_ENV,
    TRANSPORT_POLICY_DIRECT_THEN_PAID,
    WHOSCORED_CANARY_ALLOWED_PATH_FAMILIES,
    WHOSCORED_CANARY_CAP_BYTES,
    WHOSCORED_CANARY_CAPTURE_ALLOCATION_ID,
    WHOSCORED_CANARY_CAPTURE_CAP_BYTES,
    WHOSCORED_CANARY_CAPTURE_LEASE_LIMIT,
    WHOSCORED_CANARY_CAPTURE_REQUEST_LIMIT,
    WHOSCORED_CANARY_CAPTURE_WORK_ITEM_ID,
    WHOSCORED_CANARY_DAG_ID,
    WHOSCORED_CANARY_DISCOVERY_ALLOCATION_ID,
    WHOSCORED_CANARY_DISCOVERY_CAP_BYTES,
    WHOSCORED_CANARY_DISCOVERY_LEASE_LIMIT,
    WHOSCORED_CANARY_DISCOVERY_PATH_FAMILIES,
    WHOSCORED_CANARY_DISCOVERY_REQUEST_LIMIT,
    WHOSCORED_CANARY_DISCOVERY_WORK_ITEM_ID,
    WHOSCORED_CANARY_TASK_ID,
    WHOSCORED_INGEST_DAG_ID,
    WHOSCORED_PROXY_ALLOWED_HOSTS,
    ProxyCampaignApproval,
    ProxyCampaignLedger,
    ProxyCampaignSignatureError,
    ProxyCampaignValidationError,
    canonical_json_bytes,
    sign_proxy_campaign_approval,
    strict_json_loads,
    whoscored_canary_run_id,
)


MAX_DOCUMENT_BYTES = 4 * 1024 * 1024
DEFAULT_APPROVAL_ROOT = "/opt/airflow/config/whoscored_proxy_approvals"
DEFAULT_LEDGER_PATH = "/opt/airflow/logs/proxy_filter/whoscored_campaigns.json"
CANARY_TASK_ID = WHOSCORED_CANARY_TASK_ID
CANARY_WORK_ITEM_ID = WHOSCORED_CANARY_CAPTURE_WORK_ITEM_ID
CANARY_ALLOCATION_ID = WHOSCORED_CANARY_CAPTURE_ALLOCATION_ID
CANARY_DISCOVERY_WORK_ITEM_ID = WHOSCORED_CANARY_DISCOVERY_WORK_ITEM_ID
CANARY_DISCOVERY_ALLOCATION_ID = WHOSCORED_CANARY_DISCOVERY_ALLOCATION_ID
CANARY_DISCOVERY_CAP_BYTES = WHOSCORED_CANARY_DISCOVERY_CAP_BYTES
CANARY_CAPTURE_CAP_BYTES = WHOSCORED_CANARY_CAPTURE_CAP_BYTES
CANARY_DISCOVERY_PATH_FAMILIES = WHOSCORED_CANARY_DISCOVERY_PATH_FAMILIES
CANARY_ALLOWED_PATH_FAMILIES = WHOSCORED_CANARY_ALLOWED_PATH_FAMILIES
_VALIDATION_SECRET = b"unsigned-template-validation-key-000000000000000000"

# --- Daily ingest standing-approval issuance (Path A automation, #954) --------
# These identities mirror the runtime task/work-item ids exactly so each signed
# allocation binds one runtime task.  Discovery + profiles are single fixed work
# items; every active scope gets one capture allocation keyed by the same hash
# the DAG uses (dags.scripts.whoscored_proxy_runtime.stable_scope_work_item).
DAILY_DISCOVERY_TASK_ID = "discover_whoscored_catalog"
DAILY_DISCOVERY_WORK_ITEM_ID = "catalog-discovery"
DAILY_DISCOVERY_ALLOCATION_ID = "catalog-discovery"
DAILY_SCOPE_TASK_ID = "ingest_active_scope"
DAILY_PROFILES_TASK_ID = "refresh_whoscored_profiles"
DAILY_PROFILES_WORK_ITEM_ID = "profiles-daily"
DAILY_PROFILES_ALLOCATION_ID = "profiles-daily"
DAILY_DISCOVERY_WORKLOAD_CLASS = "daily_catalog_refresh"
DAILY_SCOPE_WORKLOAD_CLASS = "daily_active_scope"
DAILY_PROFILES_WORKLOAD_CLASS = "daily_profile_refresh"
# Ceilings for the sub-budgets carved from the daily total before the remainder
# is split evenly across active scopes.  For small totals (a smoke) each is
# clamped to a fraction of the total so the scope pool stays positive; for a full
# active catalog they cap discovery/profiles well below the match-capture spend.
DAILY_DISCOVERY_BUDGET_CEILING_BYTES = 128 * 1024 * 1024
DAILY_PROFILES_BUDGET_CEILING_BYTES = 256 * 1024 * 1024
DAILY_DISCOVERY_BUDGET_DIVISOR = 10
DAILY_PROFILES_BUDGET_DIVISOR = 8
# Per-allocation dial ceilings.  A lease is one source URL; the request ceiling
# permits at most one provider failover dial per lease on average.  Byte caps,
# not these, are the primary spend bound.
DAILY_DISCOVERY_REQUEST_LIMIT = 512
DAILY_DISCOVERY_LEASE_LIMIT = 256
DAILY_PROFILES_REQUEST_LIMIT = 512
DAILY_PROFILES_LEASE_LIMIT = 256
DAILY_SCOPE_REQUEST_LIMIT = 64
DAILY_SCOPE_LEASE_LIMIT = 32
DAILY_CONCURRENCY = 4
MAX_DAILY_ACTIVE_SCOPES = 2000
# A charter must not authorise an unbounded horizon; the owner refreshes it.
MAX_CHARTER_HORIZON = timedelta(days=62)
CHARTER_SCHEMA_VERSION = 1
SCHEDULED_PAID_POINTER_SCHEMA_VERSION = 1
DEFAULT_SCHEDULED_PAID_POINTER_ROOT = "/opt/airflow/config/whoscored_paid_pointers"


class CampaignCliError(RuntimeError):
    """A safe operator-facing campaign command failed."""


def _utc(value: str, field: str) -> datetime:
    try:
        parsed = datetime.fromisoformat(str(value).replace("Z", "+00:00"))
    except ValueError as exc:
        raise CampaignCliError(f"{field} must be an ISO-8601 timestamp") from exc
    if parsed.tzinfo is None or parsed.utcoffset() != timedelta(0):
        raise CampaignCliError(f"{field} must use UTC")
    return parsed.astimezone(timezone.utc)


def _read_json(path: Path) -> Mapping[str, object]:
    if path.is_symlink():
        raise CampaignCliError(f"refusing symlink input: {path}")
    try:
        size = path.stat().st_size
        if size <= 0 or size > MAX_DOCUMENT_BYTES:
            raise CampaignCliError(f"document has an invalid size: {path}")
        value = strict_json_loads(path.read_bytes().decode("utf-8"))
    except CampaignCliError:
        raise
    except ProxyCampaignValidationError as exc:
        raise CampaignCliError(f"cannot read JSON document: {path}: {exc}") from exc
    except (
        OSError,
        UnicodeDecodeError,
        json.JSONDecodeError,
    ) as exc:
        raise CampaignCliError(f"cannot read JSON document: {path}") from exc
    if not isinstance(value, Mapping):
        raise CampaignCliError(f"JSON document must be an object: {path}")
    return value


def _atomic_write_json(
    path: Path,
    value: Mapping[str, object],
    *,
    replace: bool = False,
) -> None:
    """Publish canonical JSON through a same-directory fsync/rename."""

    path = path.expanduser()
    path.parent.mkdir(parents=True, exist_ok=True)
    if path.is_symlink():
        raise CampaignCliError(f"refusing symlink output: {path}")
    if path.exists() and not replace:
        raise CampaignCliError(f"output already exists: {path}")
    payload = canonical_json_bytes(dict(value)) + b"\n"
    descriptor, temporary_name = tempfile.mkstemp(
        prefix=f".{path.name}.", suffix=".tmp", dir=path.parent
    )
    temporary = Path(temporary_name)
    try:
        os.fchmod(descriptor, 0o600)
        with os.fdopen(descriptor, "wb") as stream:
            stream.write(payload)
            stream.flush()
            os.fsync(stream.fileno())
        os.replace(temporary, path)
        os.chmod(path, 0o600)
        directory = os.open(path.parent, os.O_RDONLY | os.O_DIRECTORY)
        try:
            os.fsync(directory)
        finally:
            os.close(directory)
    finally:
        try:
            temporary.unlink()
        except FileNotFoundError:
            pass


def _require_private_file(path: Path) -> None:
    if path.is_symlink():
        raise CampaignCliError(f"refusing symlink approval: {path}")
    try:
        mode = stat.S_IMODE(path.stat().st_mode)
    except OSError as exc:
        raise CampaignCliError(f"cannot stat approval: {path}") from exc
    if mode != 0o600:
        raise CampaignCliError(
            f"approval must have mode 0600 (found {mode:04o}): {path}"
        )


def _read_secret(*, secret_file: str | None, secret_env: str) -> str:
    if secret_file:
        path = Path(secret_file).expanduser()
        if path.is_symlink():
            raise CampaignCliError("refusing symlink HMAC secret file")
        try:
            mode = stat.S_IMODE(path.stat().st_mode)
            if mode & 0o077:
                raise CampaignCliError(
                    "HMAC secret file must not be group/world readable"
                )
            value = path.read_text(encoding="utf-8").strip()
        except CampaignCliError:
            raise
        except OSError as exc:
            raise CampaignCliError("cannot read HMAC secret file") from exc
    else:
        value = str(os.environ.get(secret_env, "")).strip()
    if len(value.encode("utf-8")) < 32:
        raise CampaignCliError("HMAC secret must contain at least 32 bytes")
    return value


def _secret(args: argparse.Namespace) -> str:
    return _read_secret(
        secret_file=args.secret_file,
        secret_env=args.secret_env,
    )


def _ledger_secret(args: argparse.Namespace) -> str:
    return _read_secret(
        secret_file=args.ledger_secret_file,
        secret_env=args.ledger_secret_env,
    )


def _safe_summary(
    approval: ProxyCampaignApproval,
    *,
    signature_status: str,
) -> dict[str, object]:
    return {
        "schema_version": approval.to_dict()["schema_version"],
        "source": PROXY_CAMPAIGN_SOURCE,
        "approval_id": approval.approval_id,
        "campaign_id": approval.campaign_id,
        "run_id": approval.run_id,
        "issued_at": approval.issued_at,
        "expires_at": approval.expires_at,
        "approval_sha256": approval.approval_sha256,
        "signature_status": signature_status,
        "transport_policy": approval.transport_policy,
        "meter": PROXY_CAMPAIGN_METER,
        "total_provider_bytes": approval.caps.total_provider_bytes,
        "daily_provider_bytes": approval.caps.daily_provider_bytes,
        "request_limit": approval.limits.requests,
        "lease_limit": approval.limits.leases,
        "concurrency": approval.limits.concurrency,
        "allowed_dag_ids": list(approval.allowed_dag_ids),
        "allocation_count": len(approval.allocations),
        "exact_measurement_canary": approval.is_exact_canary,
        "required_run_id": approval.run_id,
    }


def _print_summary(value: Mapping[str, object]) -> None:
    print(json.dumps(value, ensure_ascii=False, sort_keys=True))


def _unsigned_canary(args: argparse.Namespace) -> dict[str, object]:
    issued = _utc(args.issued_at, "issued_at")
    expires = _utc(args.expires_at, "expires_at")
    if expires <= issued:
        raise CampaignCliError("expires_at must be after issued_at")
    if expires - issued > MAX_PROXY_CAMPAIGN_VALIDITY:
        raise CampaignCliError("canary validity may not exceed 24 hours")
    concurrency = int(args.concurrency)
    if not 1 <= concurrency <= 2:
        raise CampaignCliError("canary concurrency must be one or two")
    discovery_requests = WHOSCORED_CANARY_DISCOVERY_REQUEST_LIMIT
    discovery_leases = WHOSCORED_CANARY_DISCOVERY_LEASE_LIMIT
    capture_requests = WHOSCORED_CANARY_CAPTURE_REQUEST_LIMIT
    capture_leases = WHOSCORED_CANARY_CAPTURE_LEASE_LIMIT
    request_limit = discovery_requests + capture_requests
    lease_limit = discovery_leases + capture_leases
    paths = CANARY_ALLOWED_PATH_FAMILIES
    unsigned: dict[str, object] = {
        "schema_version": PROXY_CAMPAIGN_SCHEMA_VERSION,
        "source": PROXY_CAMPAIGN_SOURCE,
        "approval_id": args.approval_id,
        "campaign_id": args.campaign_id,
        "run_id": whoscored_canary_run_id(args.campaign_id),
        "issued_at": issued.isoformat(),
        "expires_at": expires.isoformat(),
        "transport_policy": TRANSPORT_POLICY_DIRECT_THEN_PAID,
        "runtime_sha256": args.runtime_sha256,
        "classifier_sha256": args.classifier_sha256,
        "caps": {
            "total_provider_bytes": WHOSCORED_CANARY_CAP_BYTES,
            "discovery_provider_bytes": CANARY_DISCOVERY_CAP_BYTES,
            "capture_provider_bytes": CANARY_CAPTURE_CAP_BYTES,
            "daily_provider_bytes": WHOSCORED_CANARY_CAP_BYTES,
        },
        "limits": {
            "requests": request_limit,
            "leases": lease_limit,
            "concurrency": concurrency,
        },
        "allowed_dag_ids": [WHOSCORED_CANARY_DAG_ID],
        "allowed_hosts": sorted(WHOSCORED_PROXY_ALLOWED_HOSTS),
        "allowed_path_families": list(paths),
        "allocations": [
            {
                "allocation_id": CANARY_DISCOVERY_ALLOCATION_ID,
                "phase": "discovery",
                "workload_class": "catalog_discovery",
                "work_item_id": CANARY_DISCOVERY_WORK_ITEM_ID,
                "task_id": CANARY_TASK_ID,
                "budget_bytes": CANARY_DISCOVERY_CAP_BYTES,
                "request_limit": discovery_requests,
                "lease_limit": discovery_leases,
                "allowed_path_families": list(
                    CANARY_DISCOVERY_PATH_FAMILIES
                ),
            },
            {
                "allocation_id": CANARY_ALLOCATION_ID,
                "phase": "capture",
                "workload_class": "representative_cohort",
                "work_item_id": CANARY_WORK_ITEM_ID,
                "task_id": CANARY_TASK_ID,
                "budget_bytes": CANARY_CAPTURE_CAP_BYTES,
                "request_limit": capture_requests,
                "lease_limit": capture_leases,
                "allowed_path_families": list(paths),
            }
        ],
        "meter": PROXY_CAMPAIGN_METER,
        "signature_algorithm": PROXY_CAMPAIGN_SIGNATURE_ALGORITHM,
    }
    # Strictly validate every field without producing operator authority.
    validated = sign_proxy_campaign_approval(unsigned, _VALIDATION_SECRET)
    approval = ProxyCampaignApproval.from_dict(validated)
    if not approval.is_exact_canary:
        raise CampaignCliError("generated template is not an exact canary")
    return approval.unsigned_dict()


def command_template(args: argparse.Namespace) -> None:
    value = _unsigned_canary(args)
    _atomic_write_json(Path(args.output), value, replace=args.force)
    digest = hashlib.sha256(canonical_json_bytes(value)).hexdigest()
    _print_summary(
        {
            "status": "unsigned_template_written",
            "output": str(Path(args.output)),
            "unsigned_sha256": digest,
            "campaign_id": value["campaign_id"],
            "approval_id": value["approval_id"],
            "required_run_id": whoscored_canary_run_id(str(value["campaign_id"])),
            "total_provider_bytes": WHOSCORED_CANARY_CAP_BYTES,
        }
    )


def _scope_work_item_id(scope: str) -> str:
    """Mirror dags.scripts.whoscored_proxy_runtime.stable_scope_work_item."""

    return "scope-" + hashlib.sha256(str(scope).encode("utf-8")).hexdigest()


def _read_scopes(path: Path) -> list[str]:
    value = _read_json_document(path)
    if isinstance(value, Mapping):
        value = value.get("active_scopes")
    if not isinstance(value, list) or not value:
        raise CampaignCliError("scopes file must be a non-empty JSON array")
    scopes: list[str] = []
    for item in value:
        if not isinstance(item, str) or not item.strip() or len(item) > 512:
            raise CampaignCliError("each scope must be a bounded non-empty string")
        scopes.append(item)
    unique = sorted(set(scopes))
    if len(unique) != len(scopes):
        raise CampaignCliError("scopes file contains duplicates")
    if len(unique) > MAX_DAILY_ACTIVE_SCOPES:
        raise CampaignCliError(
            f"scopes file exceeds {MAX_DAILY_ACTIVE_SCOPES} active scopes"
        )
    return unique


def _read_json_document(path: Path) -> object:
    if path.is_symlink():
        raise CampaignCliError(f"refusing symlink input: {path}")
    try:
        size = path.stat().st_size
        if size <= 0 or size > MAX_DOCUMENT_BYTES:
            raise CampaignCliError(f"document has an invalid size: {path}")
        return strict_json_loads(path.read_bytes().decode("utf-8"))
    except CampaignCliError:
        raise
    except (OSError, UnicodeDecodeError, ProxyCampaignValidationError) as exc:
        raise CampaignCliError(f"cannot read JSON document: {path}: {exc}") from exc


def _read_charter(path: Path, *, now: datetime) -> Mapping[str, object]:
    """Load the owner's monthly standing order that gates daily issuance."""

    _require_private_file(path)
    value = _read_json_document(path)
    if not isinstance(value, Mapping):
        raise CampaignCliError("charter must be a JSON object")
    expected = {"schema_version", "order_id", "valid_until", "daily_mb", "monthly_mb"}
    if set(value) != expected:
        raise CampaignCliError("charter fields are invalid")
    if value.get("schema_version") != CHARTER_SCHEMA_VERSION:
        raise CampaignCliError("unsupported charter schema")
    order_id = value.get("order_id")
    if not isinstance(order_id, str) or not order_id.strip() or len(order_id) > 128:
        raise CampaignCliError("charter order_id is invalid")
    daily_mb = value.get("daily_mb")
    monthly_mb = value.get("monthly_mb")
    for name, number in (("daily_mb", daily_mb), ("monthly_mb", monthly_mb)):
        if isinstance(number, bool) or not isinstance(number, int) or number <= 0:
            raise CampaignCliError(f"charter {name} must be a positive integer")
    valid_until = _utc(str(value.get("valid_until")), "charter.valid_until")
    if valid_until <= now:
        raise CampaignCliError("charter has expired")
    if valid_until - now > MAX_CHARTER_HORIZON:
        raise CampaignCliError("charter valid_until is beyond the allowed horizon")
    return value


def _split_scope_budgets(scope_pool_bytes: int, count: int) -> list[int]:
    """Split the capture scope pool into exact, near-equal positive shares."""

    if count <= 0 or scope_pool_bytes < count:
        raise CampaignCliError(
            "daily total is too small to allocate one byte per active scope"
        )
    base, remainder = divmod(scope_pool_bytes, count)
    return [base + (1 if index < remainder else 0) for index in range(count)]


def _daily_ingest_unsigned(
    *,
    run_id: str,
    scopes: Sequence[str],
    runtime_sha256: str,
    classifier_sha256: str,
    issued: datetime,
    expires: datetime,
    total_bytes: int,
    daily_bytes: int,
    campaign_id: str,
    approval_id: str,
) -> dict[str, object]:
    """Build one unsigned standing daily-ingest approval for a scheduled run."""

    if not scopes:
        raise CampaignCliError("daily ingest approval needs at least one active scope")
    discovery_bytes = min(
        DAILY_DISCOVERY_BUDGET_CEILING_BYTES,
        max(1, total_bytes // DAILY_DISCOVERY_BUDGET_DIVISOR),
    )
    profiles_bytes = min(
        DAILY_PROFILES_BUDGET_CEILING_BYTES,
        max(1, total_bytes // DAILY_PROFILES_BUDGET_DIVISOR),
    )
    scope_pool = total_bytes - discovery_bytes - profiles_bytes
    scope_budgets = _split_scope_budgets(scope_pool, len(scopes))
    capture_bytes = scope_pool + profiles_bytes
    capture_paths = list(CANARY_ALLOWED_PATH_FAMILIES)
    discovery_paths = list(CANARY_DISCOVERY_PATH_FAMILIES)
    campaign_paths = sorted(set(capture_paths) | set(discovery_paths))

    allocations: list[dict[str, object]] = [
        {
            "allocation_id": DAILY_DISCOVERY_ALLOCATION_ID,
            "phase": "discovery",
            "workload_class": DAILY_DISCOVERY_WORKLOAD_CLASS,
            "work_item_id": DAILY_DISCOVERY_WORK_ITEM_ID,
            "task_id": DAILY_DISCOVERY_TASK_ID,
            "budget_bytes": discovery_bytes,
            "request_limit": DAILY_DISCOVERY_REQUEST_LIMIT,
            "lease_limit": DAILY_DISCOVERY_LEASE_LIMIT,
            "allowed_path_families": discovery_paths,
        },
        {
            "allocation_id": DAILY_PROFILES_ALLOCATION_ID,
            "phase": "capture",
            "workload_class": DAILY_PROFILES_WORKLOAD_CLASS,
            "work_item_id": DAILY_PROFILES_WORK_ITEM_ID,
            "task_id": DAILY_PROFILES_TASK_ID,
            "budget_bytes": profiles_bytes,
            "request_limit": DAILY_PROFILES_REQUEST_LIMIT,
            "lease_limit": DAILY_PROFILES_LEASE_LIMIT,
            "allowed_path_families": capture_paths,
        },
    ]
    for scope, budget in zip(scopes, scope_budgets):
        work_item_id = _scope_work_item_id(scope)
        allocations.append(
            {
                "allocation_id": work_item_id,
                "phase": "capture",
                "workload_class": DAILY_SCOPE_WORKLOAD_CLASS,
                "work_item_id": work_item_id,
                "task_id": DAILY_SCOPE_TASK_ID,
                "budget_bytes": budget,
                "request_limit": DAILY_SCOPE_REQUEST_LIMIT,
                "lease_limit": DAILY_SCOPE_LEASE_LIMIT,
                "allowed_path_families": capture_paths,
            }
        )
    allocations.sort(key=lambda item: item["allocation_id"])

    request_limit = sum(int(item["request_limit"]) for item in allocations)
    lease_limit = sum(int(item["lease_limit"]) for item in allocations)
    unsigned: dict[str, object] = {
        "schema_version": PROXY_CAMPAIGN_SCHEMA_VERSION,
        "source": PROXY_CAMPAIGN_SOURCE,
        "approval_id": approval_id,
        "campaign_id": campaign_id,
        "run_id": run_id,
        "issued_at": issued.isoformat(),
        "expires_at": expires.isoformat(),
        "transport_policy": TRANSPORT_POLICY_DIRECT_THEN_PAID,
        "runtime_sha256": runtime_sha256,
        "classifier_sha256": classifier_sha256,
        "caps": {
            "total_provider_bytes": total_bytes,
            "discovery_provider_bytes": discovery_bytes,
            "capture_provider_bytes": capture_bytes,
            "daily_provider_bytes": daily_bytes,
        },
        "limits": {
            "requests": request_limit,
            "leases": lease_limit,
            "concurrency": DAILY_CONCURRENCY,
        },
        "allowed_dag_ids": [WHOSCORED_INGEST_DAG_ID],
        "allowed_hosts": sorted(WHOSCORED_PROXY_ALLOWED_HOSTS),
        "allowed_path_families": campaign_paths,
        "allocations": allocations,
        "meter": PROXY_CAMPAIGN_METER,
        "signature_algorithm": PROXY_CAMPAIGN_SIGNATURE_ALGORITHM,
    }
    return unsigned


def _pointer_document(*, run_id: str, approval: ProxyCampaignApproval) -> dict[str, object]:
    return {
        "schema_version": SCHEDULED_PAID_POINTER_SCHEMA_VERSION,
        "dag_id": WHOSCORED_INGEST_DAG_ID,
        "run_id": run_id,
        "approval_id": approval.approval_id,
        "approval_sha256": approval.approval_sha256,
    }


def command_issue_daily_ingest(args: argparse.Namespace) -> None:
    now = datetime.now(timezone.utc).replace(microsecond=0)
    issued = _utc(args.issued_at, "issued_at") if args.issued_at else now
    expires = (
        _utc(args.expires_at, "expires_at")
        if args.expires_at
        else issued + timedelta(hours=23)
    )
    if expires <= issued:
        raise CampaignCliError("expires_at must be after issued_at")
    if expires - issued > MAX_PROXY_CAMPAIGN_VALIDITY:
        raise CampaignCliError("daily approval validity may not exceed 24 hours")
    run_id = str(args.run_id)
    if not run_id.startswith("scheduled__"):
        raise CampaignCliError("daily ingest run_id must be a scheduled DagRun id")
    charter = _read_charter(Path(args.charter), now=now)
    charter_daily_bytes = int(charter["daily_mb"]) * 1000 * 1000
    total_bytes = int(args.total_mb) * 1000 * 1000
    daily_bytes = int(args.daily_mb) * 1000 * 1000 if args.daily_mb else total_bytes
    if total_bytes > charter_daily_bytes or daily_bytes > charter_daily_bytes:
        raise CampaignCliError("requested budget exceeds the charter daily allowance")
    if daily_bytes > total_bytes:
        raise CampaignCliError("daily_mb cannot exceed total_mb")
    scopes = _read_scopes(Path(args.scopes_file))
    run_digest = hashlib.sha256(run_id.encode("utf-8")).hexdigest()
    campaign_id = f"wsdaily-{run_digest[:32]}"
    approval_id = f"wsdaily-approval-{run_digest[:32]}"

    unsigned = _daily_ingest_unsigned(
        run_id=run_id,
        scopes=scopes,
        runtime_sha256=args.runtime_sha256,
        classifier_sha256=args.classifier_sha256,
        issued=issued,
        expires=expires,
        total_bytes=total_bytes,
        daily_bytes=daily_bytes,
        campaign_id=campaign_id,
        approval_id=approval_id,
    )
    secret = _secret(args)
    signed = sign_proxy_campaign_approval(unsigned, secret)
    approval = ProxyCampaignApproval.from_dict(signed)
    approval.verify(secret, now=issued)
    if approval.is_exact_canary:
        raise CampaignCliError("daily ingest approval must not be an exact canary")
    if approval.run_id != run_id or approval.allowed_dag_ids != (
        WHOSCORED_INGEST_DAG_ID,
    ):
        raise CampaignCliError("signed daily approval identity is inconsistent")

    approval_root = Path(args.approval_root)
    approval_path = approval_root / f"{approval_id}.json"
    _atomic_write_json(approval_path, signed, replace=args.force)
    pointer_root = Path(args.pointer_root)
    pointer_path = pointer_root / f"{run_digest}.json"
    _atomic_write_json(
        pointer_path,
        _pointer_document(run_id=run_id, approval=approval),
        replace=args.force,
    )
    _print_summary(
        {
            "status": "daily_ingest_approval_issued",
            "run_id": run_id,
            "approval_id": approval_id,
            "campaign_id": campaign_id,
            "approval_sha256": approval.approval_sha256,
            "approval_path": str(approval_path),
            "pointer_path": str(pointer_path),
            "order_id": charter["order_id"],
            "active_scope_count": len(scopes),
            "total_provider_bytes": total_bytes,
            "daily_provider_bytes": daily_bytes,
            "expires_at": expires.isoformat(),
        }
    )


def command_sign(args: argparse.Namespace) -> None:
    unsigned = _read_json(Path(args.input))
    secret = _secret(args)
    signed = sign_proxy_campaign_approval(unsigned, secret)
    approval = ProxyCampaignApproval.from_dict(signed)
    approval.verify(secret, now=_utc(approval.issued_at, "approval.issued_at"))
    if args.require_exact_canary and not approval.is_exact_canary:
        raise CampaignCliError("refusing to sign a non-canary approval")
    _atomic_write_json(Path(args.output), signed, replace=args.force)
    _print_summary(_safe_summary(approval, signature_status="valid_at_issue_time"))


def _load_signed(path: Path) -> ProxyCampaignApproval:
    _require_private_file(path)
    return ProxyCampaignApproval.from_dict(_read_json(path))


def command_verify(args: argparse.Namespace) -> None:
    approval = _load_signed(Path(args.approval))
    approval.verify(_secret(args))
    if args.require_exact_canary and not approval.is_exact_canary:
        raise CampaignCliError("approval is valid but is not an exact canary")
    _print_summary(_safe_summary(approval, signature_status="valid_now"))


def command_inspect(args: argparse.Namespace) -> None:
    value = _read_json(Path(args.approval))
    approval = ProxyCampaignApproval.from_dict(value)
    summary = _safe_summary(approval, signature_status="not_verified")
    if args.ledger:
        summary["ledger_path"] = str(Path(args.ledger))
        summary["ledger_present"] = Path(args.ledger).is_file()
    _print_summary(summary)


def command_revoke(args: argparse.Namespace) -> None:
    approval = _load_signed(Path(args.approval))
    approval_secret = _secret(args)
    # Authenticate the document at its signed issue instant so an expiry can
    # never prevent an emergency kill-switch action.
    approval.verify(
        approval_secret,
        now=_utc(approval.issued_at, "approval.issued_at"),
    )
    if args.approval_id and args.approval_id != approval.approval_id:
        raise CampaignCliError("approval ID pin does not match the document")
    if args.approval_sha256 and args.approval_sha256 != approval.approval_sha256:
        raise CampaignCliError("approval SHA-256 pin does not match the document")
    ledger = ProxyCampaignLedger(
        Path(args.ledger),
        secret=_ledger_secret(args),
        approval_secret=approval_secret,
    )
    ledger.revoke(approval.campaign_id, reason=args.reason)
    receipt = {
        "schema_version": 1,
        "event_type": "whoscored_proxy_campaign_revoked",
        "campaign_id": approval.campaign_id,
        "approval_id": approval.approval_id,
        "approval_sha256": approval.approval_sha256,
        "reason": args.reason.strip(),
        "revoked_at": datetime.now(timezone.utc).isoformat(),
        "ledger_path_sha256": hashlib.sha256(
            str(Path(args.ledger)).encode("utf-8")
        ).hexdigest(),
    }
    _atomic_write_json(Path(args.output), receipt, replace=args.force)
    _print_summary(
        {
            "status": "revoked",
            "campaign_id": approval.campaign_id,
            "approval_id": approval.approval_id,
            "receipt": str(Path(args.output)),
        }
    )


def _secret_arguments(parser: argparse.ArgumentParser) -> None:
    group = parser.add_mutually_exclusive_group()
    group.add_argument("--secret-file")
    group.add_argument(
        "--secret-env",
        default=PROXY_APPROVAL_HMAC_SECRET_ENV,
        help="environment variable containing the approval HMAC key",
    )


def _ledger_secret_arguments(parser: argparse.ArgumentParser) -> None:
    group = parser.add_mutually_exclusive_group()
    group.add_argument("--ledger-secret-file")
    group.add_argument(
        "--ledger-secret-env",
        default=PROXY_LEDGER_HMAC_SECRET_ENV,
        help="environment variable containing the ledger HMAC key",
    )


def _output_arguments(parser: argparse.ArgumentParser) -> None:
    parser.add_argument("--output", required=True)
    parser.add_argument("--force", action="store_true")


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        description="Build, sign, inspect, verify and revoke WhoScored campaigns"
    )
    subparsers = parser.add_subparsers(dest="command", required=True)

    template = subparsers.add_parser(
        "template", help="write an unsigned exact 1 GB canary template"
    )
    template.add_argument("--campaign-id", required=True)
    template.add_argument("--approval-id", required=True)
    template.add_argument("--runtime-sha256", required=True)
    template.add_argument("--classifier-sha256", required=True)
    now = datetime.now(timezone.utc).replace(microsecond=0)
    template.add_argument("--issued-at", default=now.isoformat())
    template.add_argument(
        "--expires-at", default=(now + timedelta(hours=24)).isoformat()
    )
    template.add_argument("--concurrency", type=int, default=1)
    _output_arguments(template)
    template.set_defaults(handler=command_template)

    issue_daily = subparsers.add_parser(
        "issue-daily-ingest",
        help="issue and sign one standing daily-ingest approval for a scheduled run",
    )
    issue_daily.add_argument("--run-id", required=True)
    issue_daily.add_argument("--scopes-file", required=True)
    issue_daily.add_argument("--charter", required=True)
    issue_daily.add_argument("--runtime-sha256", required=True)
    issue_daily.add_argument("--classifier-sha256", required=True)
    issue_daily.add_argument("--total-mb", type=int, required=True)
    issue_daily.add_argument("--daily-mb", type=int, default=0)
    issue_daily.add_argument("--issued-at", default="")
    issue_daily.add_argument("--expires-at", default="")
    issue_daily.add_argument(
        "--approval-root", default=DEFAULT_APPROVAL_ROOT
    )
    issue_daily.add_argument(
        "--pointer-root", default=DEFAULT_SCHEDULED_PAID_POINTER_ROOT
    )
    issue_daily.add_argument("--force", action="store_true")
    _secret_arguments(issue_daily)
    issue_daily.set_defaults(handler=command_issue_daily_ingest)

    sign = subparsers.add_parser("sign", help="sign one unsigned template")
    sign.add_argument("--input", required=True)
    sign.add_argument("--require-exact-canary", action="store_true", default=True)
    _secret_arguments(sign)
    _output_arguments(sign)
    sign.set_defaults(handler=command_sign)

    verify = subparsers.add_parser("verify", help="verify signature and validity")
    verify.add_argument("--approval", required=True)
    verify.add_argument("--require-exact-canary", action="store_true")
    _secret_arguments(verify)
    verify.set_defaults(handler=command_verify)

    inspect = subparsers.add_parser(
        "inspect", help="show non-secret structural metadata"
    )
    inspect.add_argument("--approval", required=True)
    inspect.add_argument("--ledger")
    inspect.set_defaults(handler=command_inspect)

    revoke = subparsers.add_parser("revoke", help="activate the durable kill switch")
    revoke.add_argument("--approval", required=True)
    revoke.add_argument("--approval-id")
    revoke.add_argument("--approval-sha256")
    revoke.add_argument(
        "--ledger",
        default=os.environ.get(
            "WHOSCORED_PROXY_CAMPAIGN_LEDGER_PATH", DEFAULT_LEDGER_PATH
        ),
    )
    revoke.add_argument("--reason", required=True)
    _secret_arguments(revoke)
    _ledger_secret_arguments(revoke)
    _output_arguments(revoke)
    revoke.set_defaults(handler=command_revoke)
    return parser


def main(argv: Sequence[str] | None = None) -> int:
    parser = build_parser()
    args = parser.parse_args(argv)
    _WHOSCORED_RUNTIME_CONTRACT.require_production_runtime_class(
        operation="WhoScored proxy campaign administration"
    )
    try:
        args.handler(args)
    except (
        CampaignCliError,
        ProxyCampaignSignatureError,
        ProxyCampaignValidationError,
    ) as exc:
        parser.error(str(exc))
    return 0


if __name__ == "__main__":
    sys.exit(main())
