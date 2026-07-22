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
_whoscored_root = (
    "/opt/airflow"
    if _whoscored_production
    else _whoscored_source.rsplit("/scripts/", 1)[0]
)
if _whoscored_production:
    if (
        getattr(_whoscored_bootstrap_sys, "_whoscored_runtime_startup_schema", None)
        != 2
    ):
        raise RuntimeError("image-baked WhoScored startup anchor is required")
elif (
    getattr(_whoscored_bootstrap_sys, "_whoscored_runtime_startup_root", None)
    != _whoscored_root
):
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
_WHOSCORED_RUNTIME_CONTRACT = _whoscored_bootstrap_sys._load_whoscored_runtime_contract(
    _whoscored_root
)

import argparse
import fcntl
import hashlib
import hmac
import json
import os
import re
import stat
import sys
import tempfile
from datetime import datetime, time, timedelta, timezone
from pathlib import Path
from typing import Mapping, Sequence

from scrapers.whoscored.proxy_campaign import (
    DAILY_INGEST_SCHEDULED_APPROVAL_HORIZON,
    MAX_PROXY_CAMPAIGN_VALIDITY,
    PROXY_APPROVAL_HMAC_SECRET_ENV,
    PROXY_CAMPAIGN_METER,
    PROXY_CAMPAIGN_SCHEMA_VERSION,
    PROXY_CAMPAIGN_SIGNATURE_ALGORITHM,
    PROXY_CAMPAIGN_SOURCE,
    SCHEDULED_DISCOVERY_EXPANSION_HEADROOM,
    SCHEDULED_DISCOVERY_TARGET_LIMIT_MAX,
    SCHEDULED_SCOPE_MAX_MONTHS,
    PROXY_LEDGER_HMAC_SECRET_ENV,
    SCHEDULED_PROXY_CAMPAIGN_SCHEMA_VERSION,
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
    WHOSCORED_CHARTER_SCHEMA_VERSION,
    WHOSCORED_DAILY_ACTIVE_SCOPE_CEILING,
    WHOSCORED_DAILY_PLAN_SCHEMA_VERSION,
    WHOSCORED_DAILY_PROVIDER_SAFETY_CAP_BYTES,
    WHOSCORED_INGEST_DAG_ID,
    WHOSCORED_PROXY_ALLOWED_HOSTS,
    WHOSCORED_ROLLOUT_GENESIS_PROOF_SHA256,
    WHOSCORED_ROLLOUT_MANIFEST_FIELDS,
    WHOSCORED_ROLLOUT_MANIFEST_SCHEMA_VERSION,
    WHOSCORED_ROLLOUT_WAVE_CONTRACTS,
    ProxyCampaignApproval,
    ProxyCampaignLedger,
    ProxyCampaignSignatureError,
    ProxyCampaignValidationError,
    canonical_json_bytes,
    sign_proxy_campaign_approval,
    scheduled_scope_player_pagination_target_limit,
    scheduled_scope_schedule_target_limit,
    scheduled_target_ids_sha256,
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
MAX_DAILY_ACTIVE_SCOPES = WHOSCORED_DAILY_ACTIVE_SCOPE_CEILING
# This release is admitted against the provider-side 300 MB order safety cap.
# Raising it is a separate reviewed release, not something a rollout document
# or environment variable may do.
DAILY_PROVIDER_SAFETY_CAP_BYTES = WHOSCORED_DAILY_PROVIDER_SAFETY_CAP_BYTES
# A charter must not authorise an unbounded horizon; the owner refreshes it.
MAX_CHARTER_HORIZON = timedelta(days=62)
PROVIDER_POLICY_SCHEMA_VERSION = 1
CHARTER_SCHEMA_VERSION = WHOSCORED_CHARTER_SCHEMA_VERSION
ROLLOUT_MANIFEST_SCHEMA_VERSION = WHOSCORED_ROLLOUT_MANIFEST_SCHEMA_VERSION
DAILY_PLAN_SCHEMA_VERSION = WHOSCORED_DAILY_PLAN_SCHEMA_VERSION
ISSUANCE_LEDGER_SCHEMA_VERSION = 3
SCHEDULED_PAID_POINTER_SCHEMA_VERSION = 1
DAILY_ISSUER_WINDOW_START = time(9, 0, tzinfo=timezone.utc)
DAILY_ISSUER_WINDOW_END = time(9, 30, tzinfo=timezone.utc)
DAILY_DAG_LOGICAL_HOUR_UTC = 10
DEFAULT_SCHEDULED_PAID_POINTER_ROOT = "/opt/airflow/config/whoscored_paid_pointers"
DEFAULT_ISSUANCE_LEDGER_PATH = (
    "/opt/airflow/logs/proxy_filter/whoscored_issuance_ledger.json"
)
OWNER_HMAC_SECRET_ENV = "WHOSCORED_PROXY_OWNER_HMAC_SECRET"
ISSUANCE_LEDGER_HMAC_SECRET_ENV = "WHOSCORED_PROXY_ISSUANCE_LEDGER_HMAC_SECRET"
_SHA256_RE = re.compile(r"\A[0-9a-f]{64}\Z")
_TOKEN_RE = re.compile(r"\A[A-Za-z0-9][A-Za-z0-9._:-]{0,127}\Z")
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
        "valid_from",
        "valid_until",
        "daily_cap_bytes",
        "monthly_cap_bytes",
        "order_cap_bytes",
        "max_issuances",
        "signature_algorithm",
    }
)
_SIGNED_AUTHORITY_SUFFIX_FIELDS = frozenset({"document_sha256", "signature"})
_ROLLOUT_FIELDS = WHOSCORED_ROLLOUT_MANIFEST_FIELDS
_ROLLOUT_WAVE_CONTRACTS = WHOSCORED_ROLLOUT_WAVE_CONTRACTS
_ROLLOUT_WAVE_PROMOTIONS = {
    "wave-20": "wave-70",
    "wave-70": "wave-all",
}
_DAILY_PLAN_FIELDS = frozenset(
    {
        "schema_version",
        "cohort_id",
        "cohort_sha256",
        "rollout_id",
        "wave_id",
        "max_scopes",
        "require_full_active",
        "catalog_batch_id",
        "catalog_payload_sha256",
        "catalog_active_scope_count",
        "catalog_active_scopes_sha256",
        "ranked_scope_ids_sha256",
        "ranked_workload_sha256",
        "ranked_scope_workloads",
        "workload_sha256",
        "scope_workloads",
        "discovery_parent_target_count",
        "discovery_expansion_headroom",
        "discovery_target_limit",
        "profile_target_count",
        "profile_targets_sha256",
    }
)


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


def _issuance_now() -> datetime:
    return datetime.now(timezone.utc)


def _expected_daily_logical_date(now: datetime) -> datetime:
    return (now - timedelta(days=1)).replace(
        hour=DAILY_DAG_LOGICAL_HOUR_UTC,
        minute=0,
        second=0,
        microsecond=0,
    )


def _read_json(path: Path) -> Mapping[str, object]:
    try:
        value = strict_json_loads(_read_bounded_file(path).decode("utf-8"))
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
    try:
        descriptor = os.open(path, os.O_RDONLY | os.O_CLOEXEC | os.O_NOFOLLOW)
    except OSError as exc:
        raise CampaignCliError(f"cannot open private artifact safely: {path}") from exc
    try:
        metadata = os.fstat(descriptor)
        mode = stat.S_IMODE(metadata.st_mode)
        if (
            not stat.S_ISREG(metadata.st_mode)
            or metadata.st_uid != os.geteuid()
            or metadata.st_nlink != 1
            or mode != 0o600
        ):
            raise CampaignCliError(
                f"private artifact must be owned, regular, single-link mode 0600: {path}"
            )
    finally:
        os.close(descriptor)


def _read_bounded_file(
    path: Path,
    *,
    maximum: int = MAX_DOCUMENT_BYTES,
    require_private: bool = False,
    require_frozen: bool = False,
) -> bytes:
    """Read exactly one validated inode without following the final link."""

    if require_private and require_frozen:
        raise CampaignCliError("document cannot be both private and frozen")

    try:
        descriptor = os.open(path, os.O_RDONLY | os.O_CLOEXEC | os.O_NOFOLLOW)
    except OSError as exc:
        raise CampaignCliError(f"cannot open document safely: {path}") from exc
    try:
        metadata = os.fstat(descriptor)
        mode = stat.S_IMODE(metadata.st_mode)
        private_metadata = metadata.st_uid == os.geteuid() and mode in {0o400, 0o600}
        frozen_owner = metadata.st_uid == 0 and metadata.st_gid == 0
        if not _whoscored_production:
            frozen_owner = frozen_owner or metadata.st_uid == os.geteuid()
        frozen_metadata = frozen_owner and mode == 0o440
        if (
            not stat.S_ISREG(metadata.st_mode)
            or metadata.st_nlink != 1
            or metadata.st_size <= 0
            or metadata.st_size > maximum
            or (require_private and not private_metadata)
            or (require_frozen and not frozen_metadata)
            or not require_frozen
            and metadata.st_uid != os.geteuid()
        ):
            raise CampaignCliError(f"document metadata is unsafe: {path}")
        chunks: list[bytes] = []
        remaining = metadata.st_size
        while remaining:
            chunk = os.read(descriptor, min(remaining, 64 * 1024))
            if not chunk:
                raise CampaignCliError(f"document was truncated: {path}")
            chunks.append(chunk)
            remaining -= len(chunk)
        if os.read(descriptor, 1):
            raise CampaignCliError(f"document grew while reading: {path}")
        return b"".join(chunks)
    finally:
        os.close(descriptor)


def _read_secret(*, secret_file: str | None, secret_env: str) -> str:
    if secret_file:
        path = Path(secret_file).expanduser()
        try:
            value = (
                _read_bounded_file(
                    path,
                    maximum=64 * 1024,
                    require_private=True,
                )
                .decode("utf-8")
                .strip()
            )
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


def _owner_secret(args: argparse.Namespace) -> str:
    return _read_secret(
        secret_file=args.owner_secret_file,
        secret_env=args.owner_secret_env,
    )


def _issuance_ledger_secret(args: argparse.Namespace) -> str:
    return _read_secret(
        secret_file=args.issuance_ledger_secret_file,
        secret_env=args.issuance_ledger_secret_env,
    )


def _authority_signature(body: Mapping[str, object], secret: str) -> str:
    return hmac.new(
        secret.encode("utf-8"), canonical_json_bytes(dict(body)), hashlib.sha256
    ).hexdigest()


def _sign_authority_document(
    unsigned: Mapping[str, object],
    *,
    expected_fields: frozenset[str],
    secret: str,
) -> dict[str, object]:
    if frozenset(unsigned) != expected_fields:
        raise CampaignCliError("owner authority fields are invalid")
    canonical = json.loads(canonical_json_bytes(dict(unsigned)).decode("utf-8"))
    digest = hashlib.sha256(canonical_json_bytes(canonical)).hexdigest()
    signed_body = {**canonical, "document_sha256": digest}
    return {**signed_body, "signature": _authority_signature(signed_body, secret)}


def _verify_authority_document(
    value: object,
    *,
    expected_fields: frozenset[str],
    secret: str,
    label: str,
) -> Mapping[str, object]:
    if not isinstance(value, Mapping) or frozenset(value) != (
        expected_fields | _SIGNED_AUTHORITY_SUFFIX_FIELDS
    ):
        raise CampaignCliError(f"{label} fields are invalid")
    unsigned = {field: value[field] for field in expected_fields}
    expected_digest = hashlib.sha256(canonical_json_bytes(unsigned)).hexdigest()
    digest = value.get("document_sha256")
    signature = value.get("signature")
    if (
        not isinstance(digest, str)
        or _SHA256_RE.fullmatch(digest) is None
        or not hmac.compare_digest(digest, expected_digest)
        or not isinstance(signature, str)
        or _SHA256_RE.fullmatch(signature) is None
        or not hmac.compare_digest(
            signature,
            _authority_signature({**unsigned, "document_sha256": digest}, secret),
        )
    ):
        raise CampaignCliError(f"{label} digest/signature is invalid")
    return dict(value)


def _positive_bytes(value: object, field: str) -> int:
    if isinstance(value, bool) or not isinstance(value, int) or value <= 0:
        raise CampaignCliError(f"{field} must be a positive byte count")
    return value


def _token(value: object, field: str) -> str:
    if not isinstance(value, str) or _TOKEN_RE.fullmatch(value) is None:
        raise CampaignCliError(f"{field} must be a canonical token")
    return value


def _digest(value: object, field: str) -> str:
    if not isinstance(value, str) or _SHA256_RE.fullmatch(value) is None:
        raise CampaignCliError(f"{field} must be a lowercase SHA-256")
    return value


def _rollout_contract(
    value: Mapping[str, object], *, label: str
) -> tuple[str, str, int, bool]:
    """Validate one of the three code-reviewed cumulative rollout waves."""

    rollout_id = _token(value.get("rollout_id"), f"{label} rollout_id")
    wave_id = _token(value.get("wave_id"), f"{label} wave_id")
    max_scopes = value.get("max_scopes")
    require_full_active = value.get("require_full_active")
    expected = _ROLLOUT_WAVE_CONTRACTS.get(wave_id)
    if (
        expected is None
        or isinstance(max_scopes, bool)
        or not isinstance(max_scopes, int)
        or type(require_full_active) is not bool
        or (max_scopes, require_full_active) != expected
    ):
        raise CampaignCliError(
            f"{label} must bind an exact wave-20, wave-70, or wave-all contract"
        )
    return rollout_id, wave_id, max_scopes, require_full_active


def _signed_provider_policy(
    path: Path, *, owner_secret: str, now: datetime
) -> Mapping[str, object]:
    value = _verify_authority_document(
        _read_json_document(path, require_private=True),
        expected_fields=_PROVIDER_POLICY_UNSIGNED_FIELDS,
        secret=owner_secret,
        label="provider policy",
    )
    if (
        value.get("schema_version") != PROVIDER_POLICY_SCHEMA_VERSION
        or value.get("source") != PROXY_CAMPAIGN_SOURCE
        or value.get("signature_algorithm") != PROXY_CAMPAIGN_SIGNATURE_ALGORITHM
    ):
        raise CampaignCliError("provider policy identity is invalid")
    _token(value.get("provider_id"), "provider policy provider_id")
    _token(value.get("order_id"), "provider policy order_id")
    _token(value.get("plan_id"), "provider policy plan_id")
    valid_from = _utc(str(value.get("valid_from")), "provider policy valid_from")
    valid_until = _utc(str(value.get("valid_until")), "provider policy valid_until")
    if not valid_from <= now < valid_until:
        raise CampaignCliError("provider policy is not active")
    _digest(value.get("receipt_sha256"), "provider policy receipt_sha256")
    quota = _positive_bytes(
        value.get("provider_quota_bytes"), "provider policy provider_quota_bytes"
    )
    safety = _positive_bytes(
        value.get("safety_cap_bytes"), "provider policy safety_cap_bytes"
    )
    daily = _positive_bytes(
        value.get("daily_cap_bytes"), "provider policy daily_cap_bytes"
    )
    monthly = _positive_bytes(
        value.get("monthly_cap_bytes"), "provider policy monthly_cap_bytes"
    )
    order = _positive_bytes(
        value.get("order_cap_bytes"), "provider policy order_cap_bytes"
    )
    if (
        not daily <= monthly <= order <= safety <= quota
        or safety > DAILY_PROVIDER_SAFETY_CAP_BYTES
    ):
        raise CampaignCliError("provider policy quota/safety caps are inconsistent")
    return value


def _signed_charter(
    path: Path,
    *,
    owner_secret: str,
    policy: Mapping[str, object],
    now: datetime,
) -> Mapping[str, object]:
    value = _verify_authority_document(
        _read_json_document(path, require_private=True),
        expected_fields=_CHARTER_UNSIGNED_FIELDS,
        secret=owner_secret,
        label="charter",
    )
    if (
        value.get("schema_version") != CHARTER_SCHEMA_VERSION
        or value.get("source") != PROXY_CAMPAIGN_SOURCE
        or value.get("signature_algorithm") != PROXY_CAMPAIGN_SIGNATURE_ALGORITHM
        or value.get("provider_policy_sha256") != policy.get("document_sha256")
        or value.get("order_id") != policy.get("order_id")
    ):
        raise CampaignCliError("charter identity/policy binding is invalid")
    valid_from = _utc(str(value.get("valid_from")), "charter.valid_from")
    valid_until = _utc(str(value.get("valid_until")), "charter.valid_until")
    policy_valid_from = _utc(
        str(policy.get("valid_from")), "provider policy valid_from"
    )
    policy_valid_until = _utc(
        str(policy.get("valid_until")), "provider policy valid_until"
    )
    if not valid_from <= now < valid_until or valid_until - now > MAX_CHARTER_HORIZON:
        raise CampaignCliError("charter validity is outside the allowed horizon")
    if valid_from < policy_valid_from or valid_until > policy_valid_until:
        raise CampaignCliError("charter validity is outside the provider policy window")
    billing_month = str(value.get("billing_month") or "")
    if billing_month != now.strftime("%Y-%m"):
        raise CampaignCliError("charter billing_month is not current")
    _token(value.get("cohort_id"), "charter cohort_id")
    _digest(value.get("cohort_sha256"), "charter cohort_sha256")
    _rollout_contract(value, label="charter")
    _digest(
        value.get("ranked_scope_ids_sha256"),
        "charter ranked_scope_ids_sha256",
    )
    _digest(value.get("runtime_sha256"), "charter runtime_sha256")
    _digest(value.get("classifier_sha256"), "charter classifier_sha256")
    promotion_acceptance_sha256 = _digest(
        value.get("promotion_acceptance_sha256"),
        "charter promotion_acceptance_sha256",
    )
    promotion_terminal_receipt_sha256 = _digest(
        value.get("promotion_terminal_receipt_sha256"),
        "charter promotion_terminal_receipt_sha256",
    )
    if (
        value.get("wave_id") == "wave-20"
        and (
            promotion_acceptance_sha256 != WHOSCORED_ROLLOUT_GENESIS_PROOF_SHA256
            or promotion_terminal_receipt_sha256
            != WHOSCORED_ROLLOUT_GENESIS_PROOF_SHA256
        )
    ) or (
        value.get("wave_id") != "wave-20"
        and (
            promotion_acceptance_sha256 == WHOSCORED_ROLLOUT_GENESIS_PROOF_SHA256
            or promotion_terminal_receipt_sha256
            == WHOSCORED_ROLLOUT_GENESIS_PROOF_SHA256
        )
    ):
        raise CampaignCliError("charter promotion proof is invalid for its wave")
    daily = _positive_bytes(value.get("daily_cap_bytes"), "charter daily_cap_bytes")
    monthly = _positive_bytes(
        value.get("monthly_cap_bytes"), "charter monthly_cap_bytes"
    )
    order = _positive_bytes(value.get("order_cap_bytes"), "charter order_cap_bytes")
    max_issuances = value.get("max_issuances")
    if (
        not daily <= monthly <= order
        or daily > int(policy["daily_cap_bytes"])
        or monthly > int(policy["monthly_cap_bytes"])
        or order > int(policy["order_cap_bytes"])
        or isinstance(max_issuances, bool)
        or not isinstance(max_issuances, int)
        or max_issuances <= 0
    ):
        raise CampaignCliError("charter caps/max_issuances are invalid")
    return value


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
                "allowed_path_families": list(CANARY_DISCOVERY_PATH_FAMILIES),
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
            },
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


def _command_sign_owner_authority(
    args: argparse.Namespace,
    *,
    expected_fields: frozenset[str],
    label: str,
) -> None:
    value = _read_json(Path(args.input))
    signed = _sign_authority_document(
        value,
        expected_fields=expected_fields,
        secret=_owner_secret(args),
    )
    _atomic_write_json(Path(args.output), signed, replace=args.force)
    _print_summary(
        {
            "status": f"{label}_signed",
            "output": str(Path(args.output)),
            "document_sha256": signed["document_sha256"],
        }
    )


def command_sign_provider_policy(args: argparse.Namespace) -> None:
    _command_sign_owner_authority(
        args,
        expected_fields=_PROVIDER_POLICY_UNSIGNED_FIELDS,
        label="provider_policy",
    )


def command_sign_charter(args: argparse.Namespace) -> None:
    _command_sign_owner_authority(
        args,
        expected_fields=_CHARTER_UNSIGNED_FIELDS,
        label="charter",
    )


def _scope_work_item_id(scope: str) -> str:
    """Mirror dags.scripts.whoscored_proxy_runtime.stable_scope_work_item."""

    return "scope-" + hashlib.sha256(str(scope).encode("utf-8")).hexdigest()


def _scope_specs_sha256(scopes: Sequence[str]) -> str:
    """Mirror the DAG's newline-delimited active-scope identity."""

    return hashlib.sha256(
        ("\n".join(scopes) + ("\n" if scopes else "")).encode("utf-8")
    ).hexdigest()


def _catalog_discovery_parent_target_count(catalog: object) -> int:
    """Return a deterministic known-target baseline for incremental discovery."""

    rows = catalog.to_rows()
    if not isinstance(rows, Mapping):
        raise CampaignCliError("parent catalog rows are invalid")
    competitions = rows.get("competitions", ())
    seasons = rows.get("seasons", ())
    stages = rows.get("stages", ())
    if any(
        not isinstance(values, Sequence) for values in (competitions, seasons, stages)
    ):
        raise CampaignCliError("parent catalog target rows are invalid")
    # Root, tournament menus, season pages, then calendar plus the two bounded
    # first/last activity probes for every known stage. Newly advertised rows
    # consume the separately signed expansion headroom.
    return 1 + len(competitions) + len(seasons) + 3 * len(stages)


def _inclusive_season_month_count(runtime: object, *, scope: str) -> int:
    start = getattr(runtime, "start", None)
    end = getattr(runtime, "end", None)
    if start is None or end is None or end < start:
        raise CampaignCliError(
            f"scope {scope} has no bounded season interval for schedule planning"
        )
    months = (end.year - start.year) * 12 + end.month - start.month + 1
    if not 1 <= months <= SCHEDULED_SCOPE_MAX_MONTHS:
        raise CampaignCliError(
            f"scope {scope} schedule month span is outside "
            f"1..{SCHEDULED_SCOPE_MAX_MONTHS}: {months}"
        )
    return months


def _read_json_document(
    path: Path,
    *,
    require_private: bool = False,
    require_frozen: bool = False,
) -> object:
    try:
        return strict_json_loads(
            _read_bounded_file(
                path,
                require_private=require_private,
                require_frozen=require_frozen,
            ).decode("utf-8")
        )
    except CampaignCliError:
        raise
    except (OSError, UnicodeDecodeError, ProxyCampaignValidationError) as exc:
        raise CampaignCliError(f"cannot read JSON document: {path}: {exc}") from exc


def _read_rollout(path: Path, *, require_private: bool = False) -> Mapping[str, object]:
    value = _read_json_document(path, require_private=require_private)
    if not isinstance(value, Mapping) or frozenset(value) != _ROLLOUT_FIELDS:
        raise CampaignCliError("rollout manifest fields are invalid")
    if value.get("schema_version") != ROLLOUT_MANIFEST_SCHEMA_VERSION:
        raise CampaignCliError("unsupported rollout manifest schema")
    _token(value.get("cohort_id"), "rollout cohort_id")
    _rollout_id, wave_id, _maximum, _require_full = _rollout_contract(
        value, label="rollout"
    )
    _digest(value.get("runtime_sha256"), "rollout runtime_sha256")
    _digest(value.get("classifier_sha256"), "rollout classifier_sha256")
    promotion_acceptance_sha256 = _digest(
        value.get("promotion_acceptance_sha256"),
        "rollout promotion_acceptance_sha256",
    )
    promotion_terminal_receipt_sha256 = _digest(
        value.get("promotion_terminal_receipt_sha256"),
        "rollout promotion_terminal_receipt_sha256",
    )
    genesis_proof = WHOSCORED_ROLLOUT_GENESIS_PROOF_SHA256
    if (
        wave_id == "wave-20"
        and (
            promotion_acceptance_sha256 != genesis_proof
            or promotion_terminal_receipt_sha256 != genesis_proof
        )
    ) or (
        wave_id != "wave-20"
        and (
            promotion_acceptance_sha256 == genesis_proof
            or promotion_terminal_receipt_sha256 == genesis_proof
        )
    ):
        raise CampaignCliError("rollout promotion proof is invalid for its wave")
    ranked_scope_ids = value.get("ranked_scope_ids")
    if (
        not isinstance(ranked_scope_ids, list)
        or not ranked_scope_ids
        or len(ranked_scope_ids) > MAX_DAILY_ACTIVE_SCOPES
        or any(
            not isinstance(item, str)
            or not item
            or item != item.strip()
            or len(item) > 512
            for item in ranked_scope_ids
        )
        or len(ranked_scope_ids) != len(set(ranked_scope_ids))
    ):
        raise CampaignCliError(
            "rollout ranked_scope_ids must be a bounded unique ordered list"
        )
    ranked_scope_ids_sha256 = _digest(
        value.get("ranked_scope_ids_sha256"),
        "rollout ranked_scope_ids_sha256",
    )
    if ranked_scope_ids_sha256 != _scope_specs_sha256(ranked_scope_ids):
        raise CampaignCliError("rollout ranked scope identity is invalid")
    basis = _normalize_daily_workloads(
        value.get("ranking_basis_scope_workloads"),
        label="rollout ranking basis",
    )
    if (
        [str(item["scope"]) for item in basis] != ranked_scope_ids
        or basis
        != sorted(
            basis,
            key=lambda workload: (
                -int(workload["paid_target_count"]),
                str(workload["scope"]),
            ),
        )
        or value.get("ranking_basis_workload_sha256")
        != hashlib.sha256(canonical_json_bytes(basis)).hexdigest()
    ):
        raise CampaignCliError(
            "rollout ranking basis is not the exact heavy-first scope order"
        )
    return dict(value)


def _promotion_acceptance_evidence(
    *, source: Mapping[str, object], receipt_paths: object
) -> tuple[str, str]:
    """Validate the exact immutable receipt chain required for one promotion."""

    from dags.scripts import whoscored_rollout_acceptance as acceptance

    source_wave = str(source["wave_id"])
    try:
        source_wave_index = acceptance.WAVE_ORDER.index(source_wave)
    except ValueError as exc:
        raise CampaignCliError("promotion source wave is invalid") from exc
    if source_wave_index >= len(acceptance.WAVE_ORDER) - 1:
        raise CampaignCliError("the terminal rollout wave cannot be promoted")
    expected_waves = list(acceptance.WAVE_ORDER[: source_wave_index + 1])
    expected_receipt_count = 2 * len(expected_waves)
    if (
        not isinstance(receipt_paths, list)
        or len(receipt_paths) != expected_receipt_count
        or len(set(receipt_paths)) != len(receipt_paths)
    ):
        raise CampaignCliError(
            "promotion requires the exact two-receipt accepted chain per source wave"
        )
    records: list[tuple[str, Mapping[str, object]]] = []
    rollout_id = str(source["rollout_id"])
    prefix = acceptance.receipts_prefix(rollout_id)
    for raw_path in receipt_paths:
        path = Path(str(raw_path))
        if (
            path.suffix != ".json"
            or _SHA256_RE.fullmatch(path.stem) is None
            or path.name != f"{path.stem}.json"
        ):
            raise CampaignCliError(
                "acceptance receipt filename must be its lowercase content SHA-256"
            )
        receipt = _read_json_document(path, require_private=True)
        if not isinstance(receipt, Mapping):
            raise CampaignCliError("acceptance receipt must be a JSON object")
        records.append((f"{prefix}/{path.name}", dict(receipt)))
    try:
        validated = acceptance.validated_receipts(records)
    except acceptance.WhoScoredRolloutAcceptanceError as exc:
        raise CampaignCliError(
            f"promotion acceptance evidence is invalid: {exc}"
        ) from exc
    terminal_candidates = [
        (key, receipt)
        for key, receipt in validated
        if receipt["scope"]["wave_id"] == source_wave
        and receipt["wave_accepted"] is True
    ]
    if not terminal_candidates:
        raise CampaignCliError(
            "promotion evidence must contain an accepted source-wave terminal receipt"
        )
    # A third and every later consecutive success is also accepted.  The exact
    # caller-supplied chain still contains only two receipts per wave, so select
    # the chronologically latest accepted source receipt and let the public
    # replay helper prove that its immediate predecessor is present.
    terminal_key, terminal_receipt = terminal_candidates[-1]
    terminal_digest = Path(terminal_key).stem
    terminal_scope = terminal_receipt["scope"]
    ranked_scope_ids = list(source["ranked_scope_ids"])
    source_cohort_sha256 = hashlib.sha256(
        canonical_json_bytes(dict(source))
    ).hexdigest()
    selected_count = min(int(source["max_scopes"]), len(ranked_scope_ids))
    if (
        terminal_scope.get("rollout_id") != rollout_id
        or terminal_scope.get("wave_id") != source_wave
        or terminal_scope.get("cohort_sha256") != source_cohort_sha256
        or terminal_scope.get("ranked_scope_ids_sha256")
        != source["ranked_scope_ids_sha256"]
        or terminal_scope.get("classifier_sha256") != source["classifier_sha256"]
        or terminal_scope.get("promotion_acceptance_sha256")
        != source["promotion_acceptance_sha256"]
        or terminal_scope.get("promotion_terminal_receipt_sha256")
        != source["promotion_terminal_receipt_sha256"]
        or terminal_scope.get("catalog_active_scope_count") != len(ranked_scope_ids)
        or terminal_scope.get("catalog_active_scopes_sha256")
        != _scope_specs_sha256(sorted(ranked_scope_ids))
        or terminal_scope.get("selected_scope_count") != selected_count
        or terminal_scope.get("selected_scopes") != ranked_scope_ids[:selected_count]
        or terminal_receipt["release"].get("code_tree_sha256")
        != source["runtime_sha256"]
    ):
        raise CampaignCliError(
            "accepted source-wave receipt differs from the frozen rollout/release"
        )
    try:
        evidence = acceptance.promotion_acceptance_evidence(
            records,
            rollout_id=rollout_id,
            source_wave_id=source_wave,
            expected_terminal_receipt_sha256=terminal_digest,
        )
    except acceptance.WhoScoredRolloutAcceptanceError as exc:
        raise CampaignCliError(
            f"promotion acceptance evidence is invalid: {exc}"
        ) from exc
    if (
        evidence.get("source_cohort_sha256") != source_cohort_sha256
        or evidence.get("runtime_sha256") != source["runtime_sha256"]
        or evidence.get("classifier_sha256") != source["classifier_sha256"]
        or evidence.get("terminal_receipt_sha256") != terminal_digest
        or len(evidence.get("terminal_runs", ())) != expected_receipt_count
    ):
        raise CampaignCliError(
            "promotion acceptance proof differs from the frozen source rollout"
        )
    return str(evidence["promotion_acceptance_sha256"]), terminal_digest


def _planned_scope_workload(
    repository: object,
    *,
    scope: object,
    runtime: object,
) -> dict[str, object]:
    """Freeze the exact paid target demand for one active scope."""

    candidates = repository.list_match_candidates(
        scope.competition_id,
        scope.season_id,
        limit=101,
        include_exact_count=True,
    )
    match_count = (
        int(candidates[0].exact_candidate_count)
        if candidates and candidates[0].exact_candidate_count is not None
        else len(candidates)
    )
    if match_count > 100:
        raise CampaignCliError(
            f"scope {scope.spec} match backlog exceeds daily runtime cap: "
            f"{match_count} > 100"
        )
    match_ids = sorted({int(candidate.game_id) for candidate in candidates})
    if len(match_ids) != match_count:
        raise CampaignCliError(
            f"scope {scope.spec} match candidate snapshot is incomplete"
        )
    preview_candidates = repository.list_preview_candidates(
        scope.competition_id,
        scope.season_id,
        limit=257,
    )
    preview_count = len(preview_candidates)
    if preview_count > 256:
        raise CampaignCliError(
            f"scope {scope.spec} preview workload exceeds 256 targets"
        )
    preview_ids = sorted(
        {int(candidate["game_id"]) for candidate in preview_candidates}
    )
    if len(preview_ids) != preview_count:
        raise CampaignCliError(
            f"scope {scope.spec} preview candidate snapshot is not unique"
        )
    stage_ids = sorted(
        {int(stage_id) for stage_id in getattr(runtime, "stage_ids", ())}
    )
    if not stage_ids:
        raise CampaignCliError(f"scope {scope.spec} has no schedule stages")
    schedule_months = _inclusive_season_month_count(runtime, scope=scope.spec)
    # One season page plus, per stage: calendar, every bounded schedule
    # month, 67 structured first pages and one referee page.
    schedule_target_limit = scheduled_scope_schedule_target_limit(
        stage_count=len(stage_ids),
        season_month_count=schedule_months,
    )
    non_pagination_count = schedule_target_limit + match_count + preview_count
    player_pagination_target_limit = scheduled_scope_player_pagination_target_limit(
        stage_count=len(stage_ids),
        non_pagination_target_count=non_pagination_count,
    )
    count = non_pagination_count + player_pagination_target_limit
    if not 1 <= count <= WHOSCORED_CANARY_CAPTURE_LEASE_LIMIT:
        raise CampaignCliError(
            "scope "
            f"{scope.spec} paid workload exceeds the reviewed capture target "
            f"ceiling: {count}"
        )
    return {
        "scope": scope.spec,
        "work_item_id": _scope_work_item_id(scope.spec),
        "schedule_target_limit": schedule_target_limit,
        "schedule_targets_sha256": scheduled_target_ids_sha256(
            [f"season:{scope.spec}", *(f"stage:{value}" for value in stage_ids)]
        ),
        "player_pagination_target_limit": player_pagination_target_limit,
        "match_target_count": match_count,
        "match_targets_sha256": scheduled_target_ids_sha256(match_ids),
        "preview_target_count": preview_count,
        "preview_targets_sha256": scheduled_target_ids_sha256(preview_ids),
        "paid_target_count": count,
    }


def command_create_rollout(args: argparse.Namespace) -> None:
    """Freeze one heavy-first scope ranking from exact current paid demand."""

    rollout_id = _token(args.rollout_id, "rollout_id")
    cohort_id = _token(args.cohort_id, "cohort_id")
    wave_id = _token(args.wave_id, "wave_id")
    if wave_id != "wave-20":
        raise CampaignCliError(
            "create-rollout can only create wave-20; use promote-rollout for later waves"
        )
    runtime_sha256 = _digest(args.runtime_sha256, "runtime_sha256")
    classifier_sha256 = _digest(args.classifier_sha256, "classifier_sha256")
    wave_contract = _ROLLOUT_WAVE_CONTRACTS.get(wave_id)
    if wave_contract is None:
        raise CampaignCliError("wave_id is outside the reviewed rollout contract")
    maximum, require_full_active = wave_contract
    from dags.scripts import run_whoscored_scraper as runner

    repository = runner._new_repository()
    generation, catalog = repository.load_catalog_generation_snapshot()
    selected = runner._select_catalog_snapshot_scopes(catalog, [], active_only=True)
    active = {scope.spec: (scope, runtime) for scope, runtime in selected}
    if (
        not active
        or len(active) != len(selected)
        or len(active) > MAX_DAILY_ACTIVE_SCOPES
    ):
        raise CampaignCliError("active catalog is outside the rollout scope ceiling")
    ranking_basis = [
        _planned_scope_workload(repository, scope=scope, runtime=runtime)
        for scope, runtime in (active[scope_spec] for scope_spec in sorted(active))
    ]
    ranking_basis.sort(
        key=lambda workload: (
            -int(workload["paid_target_count"]),
            str(workload["scope"]),
        )
    )
    ranked_scope_ids = [str(item["scope"]) for item in ranking_basis]
    result = {
        "schema_version": ROLLOUT_MANIFEST_SCHEMA_VERSION,
        "cohort_id": cohort_id,
        "rollout_id": rollout_id,
        "wave_id": wave_id,
        "max_scopes": maximum,
        "require_full_active": require_full_active,
        "ranked_scope_ids": ranked_scope_ids,
        "ranked_scope_ids_sha256": _scope_specs_sha256(ranked_scope_ids),
        "ranking_basis_workload_sha256": hashlib.sha256(
            canonical_json_bytes(ranking_basis)
        ).hexdigest(),
        "ranking_basis_scope_workloads": ranking_basis,
        "runtime_sha256": runtime_sha256,
        "classifier_sha256": classifier_sha256,
        "promotion_acceptance_sha256": WHOSCORED_ROLLOUT_GENESIS_PROOF_SHA256,
        "promotion_terminal_receipt_sha256": (WHOSCORED_ROLLOUT_GENESIS_PROOF_SHA256),
    }
    cohort_sha256 = hashlib.sha256(canonical_json_bytes(result)).hexdigest()
    _atomic_write_json(Path(args.output), result, replace=args.force)
    _print_summary(
        {
            "status": "rollout_ranking_frozen",
            "output": str(Path(args.output)),
            "rollout_id": rollout_id,
            "wave_id": wave_id,
            "catalog_batch_id": generation["catalog_batch_id"],
            "catalog_active_scope_count": len(ranked_scope_ids),
            "cohort_sha256": cohort_sha256,
            "ranked_scope_ids_sha256": result["ranked_scope_ids_sha256"],
            "ranking_basis_workload_sha256": result["ranking_basis_workload_sha256"],
        }
    )


def command_promote_rollout(args: argparse.Namespace) -> None:
    """Derive the adjacent wave without consulting or reranking the live catalog."""

    source = _read_rollout(Path(args.input), require_private=True)
    source_wave = str(source["wave_id"])
    target_wave = _token(args.wave_id, "wave_id")
    expected_target = _ROLLOUT_WAVE_PROMOTIONS.get(source_wave)
    if target_wave != expected_target:
        raise CampaignCliError(
            "rollout promotion must be exactly wave-20 to wave-70 or "
            "wave-70 to wave-all"
        )
    cohort_id = _token(args.cohort_id, "cohort_id")
    if cohort_id == source["cohort_id"]:
        raise CampaignCliError("rollout promotion requires a new cohort_id")
    promotion_acceptance_sha256, terminal_receipt_sha256 = (
        _promotion_acceptance_evidence(
            source=source,
            receipt_paths=args.acceptance_receipt,
        )
    )
    maximum, require_full_active = _ROLLOUT_WAVE_CONTRACTS[target_wave]
    result = dict(source)
    result.update(
        {
            "cohort_id": cohort_id,
            "wave_id": target_wave,
            "max_scopes": maximum,
            "require_full_active": require_full_active,
            "promotion_acceptance_sha256": promotion_acceptance_sha256,
            "promotion_terminal_receipt_sha256": terminal_receipt_sha256,
        }
    )
    # Revalidate the derived document before publication. All frozen ranking and
    # workload-basis fields came from the already validated source unchanged.
    if any(
        result[field] != source[field]
        for field in _ROLLOUT_FIELDS
        - {
            "cohort_id",
            "wave_id",
            "max_scopes",
            "require_full_active",
            "promotion_acceptance_sha256",
            "promotion_terminal_receipt_sha256",
        }
    ):
        raise CampaignCliError("rollout promotion changed frozen rollout identity")
    _rollout_contract(result, label="promoted rollout")
    cohort_sha256 = hashlib.sha256(canonical_json_bytes(result)).hexdigest()
    _atomic_write_json(Path(args.output), result, replace=args.force)
    _print_summary(
        {
            "status": "rollout_wave_promoted",
            "output": str(Path(args.output)),
            "rollout_id": result["rollout_id"],
            "source_wave_id": source_wave,
            "wave_id": target_wave,
            "cohort_id": cohort_id,
            "cohort_sha256": cohort_sha256,
            "ranked_scope_ids_sha256": result["ranked_scope_ids_sha256"],
            "promotion_acceptance_sha256": promotion_acceptance_sha256,
            "promotion_terminal_receipt_sha256": terminal_receipt_sha256,
        }
    )


def command_plan_daily_ingest(args: argparse.Namespace) -> None:
    """Freeze the current catalog and exact heavy-first rollout wave."""

    rollout = _read_rollout(Path(args.rollout_file))
    _rollout_id, _wave_id, maximum, require_full_active = _rollout_contract(
        rollout, label="rollout"
    )
    from dags.scripts import run_whoscored_scraper as runner

    repository = runner._new_repository()
    generation, catalog = repository.load_catalog_generation_snapshot()
    selected = runner._select_catalog_snapshot_scopes(catalog, [], active_only=True)
    active = {scope.spec: (scope, runtime) for scope, runtime in selected}
    if not active or len(active) != len(selected):
        raise CampaignCliError("active catalog contains no unique rollout scopes")
    active_scopes = sorted(active)
    ranked_scope_ids = list(rollout["ranked_scope_ids"])
    if sorted(ranked_scope_ids) != active_scopes:
        missing = sorted(set(active_scopes) - set(ranked_scope_ids))
        extra = sorted(set(ranked_scope_ids) - set(active_scopes))
        raise CampaignCliError(
            "rollout scope universe differs from the exact active catalog: "
            f"missing={missing}, extra={extra}"
        )
    ranked_workloads: list[dict[str, object]] = []
    for scope_spec in ranked_scope_ids:
        scope, runtime = active[str(scope_spec)]
        ranked_workloads.append(
            _planned_scope_workload(repository, scope=scope, runtime=runtime)
        )
    workloads = ranked_workloads[: min(maximum, len(ranked_workloads))]
    if require_full_active and len(workloads) != len(ranked_workloads):
        raise CampaignCliError("wave-all did not select the complete active catalog")
    from scrapers.whoscored.profile_policy import daily_profile_candidate_hard_cap

    selected_values = [active[str(item["scope"])] for item in workloads]
    profile_snapshot = repository.profile_candidate_snapshot(
        scopes=[
            getattr(runtime, "scope", runtime) for _scope, runtime in selected_values
        ],
        hard_cap=daily_profile_candidate_hard_cap(),
    )
    profile_target_count = int(profile_snapshot.count)
    profile_targets_sha256 = _digest(
        profile_snapshot.payload_sha256,
        "profile candidate payload_sha256",
    )
    if not 0 <= profile_target_count <= 256:
        raise CampaignCliError(
            "exact profile workload is outside the scheduled 0..256 bound"
        )
    cohort_sha256 = hashlib.sha256(canonical_json_bytes(rollout)).hexdigest()
    ranked_workload_sha256 = hashlib.sha256(
        canonical_json_bytes(ranked_workloads)
    ).hexdigest()
    workload_sha256 = hashlib.sha256(canonical_json_bytes(workloads)).hexdigest()
    discovery_parent_target_count = _catalog_discovery_parent_target_count(catalog)
    discovery_target_limit = min(
        SCHEDULED_DISCOVERY_TARGET_LIMIT_MAX,
        discovery_parent_target_count + SCHEDULED_DISCOVERY_EXPANSION_HEADROOM,
    )
    result = {
        "schema_version": DAILY_PLAN_SCHEMA_VERSION,
        "cohort_id": rollout["cohort_id"],
        "cohort_sha256": cohort_sha256,
        "rollout_id": rollout["rollout_id"],
        "wave_id": rollout["wave_id"],
        "max_scopes": maximum,
        "require_full_active": require_full_active,
        "catalog_batch_id": generation["catalog_batch_id"],
        "catalog_payload_sha256": generation["catalog_payload_sha256"],
        "catalog_active_scope_count": len(active_scopes),
        "catalog_active_scopes_sha256": _scope_specs_sha256(active_scopes),
        "ranked_scope_ids_sha256": rollout["ranked_scope_ids_sha256"],
        "ranked_workload_sha256": ranked_workload_sha256,
        "ranked_scope_workloads": ranked_workloads,
        "workload_sha256": workload_sha256,
        "scope_workloads": workloads,
        "discovery_parent_target_count": discovery_parent_target_count,
        "discovery_expansion_headroom": SCHEDULED_DISCOVERY_EXPANSION_HEADROOM,
        "discovery_target_limit": discovery_target_limit,
        "profile_target_count": profile_target_count,
        "profile_targets_sha256": profile_targets_sha256,
    }
    _atomic_write_json(Path(args.output), result, replace=args.force)
    _print_summary(
        {
            "status": "daily_ingest_plan_frozen",
            "output": str(Path(args.output)),
            "cohort_id": rollout["cohort_id"],
            "rollout_id": rollout["rollout_id"],
            "wave_id": rollout["wave_id"],
            "catalog_batch_id": generation["catalog_batch_id"],
            "catalog_active_scope_count": len(active_scopes),
            "scope_count": len(workloads),
            "workload_sha256": workload_sha256,
        }
    )


def _normalize_daily_workloads(
    workloads: object, *, label: str
) -> list[dict[str, object]]:
    if not isinstance(workloads, list) or not workloads:
        raise CampaignCliError(f"daily plan {label} must be non-empty")
    normalized: list[dict[str, object]] = []
    for item in workloads:
        if not isinstance(item, Mapping) or set(item) != {
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
        }:
            raise CampaignCliError(f"daily plan {label} fields are invalid")
        scope = str(item.get("scope") or "")
        expected = _scope_work_item_id(scope)
        count = item.get("paid_target_count")
        schedule_count = item.get("schedule_target_limit")
        pagination_count = item.get("player_pagination_target_limit")
        match_count = item.get("match_target_count")
        preview_count = item.get("preview_target_count")
        if (
            not scope
            or scope != scope.strip()
            or len(scope) > 512
            or item.get("work_item_id") != expected
            or isinstance(count, bool)
            or not isinstance(count, int)
            or not 1 <= count <= WHOSCORED_CANARY_CAPTURE_LEASE_LIMIT
            or isinstance(schedule_count, bool)
            or not isinstance(schedule_count, int)
            or schedule_count <= 0
            or isinstance(pagination_count, bool)
            or not isinstance(pagination_count, int)
            or pagination_count < 0
            or isinstance(match_count, bool)
            or not isinstance(match_count, int)
            or not 0 <= match_count <= 100
            or isinstance(preview_count, bool)
            or not isinstance(preview_count, int)
            or not 0 <= preview_count <= 256
            or count != schedule_count + pagination_count + match_count + preview_count
        ):
            raise CampaignCliError(f"daily plan {label} item is invalid")
        for digest_field in (
            "schedule_targets_sha256",
            "match_targets_sha256",
            "preview_targets_sha256",
        ):
            _digest(item.get(digest_field), f"daily plan {digest_field}")
        normalized.append(dict(item))
    observed_scopes = [str(item["scope"]) for item in normalized]
    if len(observed_scopes) != len(set(observed_scopes)):
        raise CampaignCliError(f"daily plan {label} scopes must be unique")
    return normalized


def _read_daily_plan(
    path: Path, *, require_frozen: bool = False
) -> Mapping[str, object]:
    value = _read_json_document(
        path,
        require_private=not require_frozen,
        require_frozen=require_frozen,
    )
    if not isinstance(value, Mapping) or frozenset(value) != _DAILY_PLAN_FIELDS:
        raise CampaignCliError("daily plan fields are invalid")
    if value.get("schema_version") != DAILY_PLAN_SCHEMA_VERSION:
        raise CampaignCliError("unsupported daily plan schema")
    _token(value.get("cohort_id"), "plan cohort_id")
    _digest(value.get("cohort_sha256"), "plan cohort_sha256")
    _rollout_id, _wave_id, max_scopes, require_full_active = _rollout_contract(
        value, label="daily plan"
    )
    _token(value.get("catalog_batch_id"), "plan catalog_batch_id")
    _digest(value.get("catalog_payload_sha256"), "plan catalog_payload_sha256")
    active_scopes_sha256 = _digest(
        value.get("catalog_active_scopes_sha256"),
        "plan catalog_active_scopes_sha256",
    )
    active_count = value.get("catalog_active_scope_count")
    if (
        isinstance(active_count, bool)
        or not isinstance(active_count, int)
        or not 1 <= active_count <= MAX_DAILY_ACTIVE_SCOPES
    ):
        raise CampaignCliError(
            "daily plan catalog_active_scope_count is outside its bound"
        )
    for field, allow_zero, maximum in (
        ("discovery_parent_target_count", False, None),
        ("discovery_expansion_headroom", False, None),
        ("discovery_target_limit", False, SCHEDULED_DISCOVERY_TARGET_LIMIT_MAX),
        ("profile_target_count", True, 256),
    ):
        count = value.get(field)
        if (
            isinstance(count, bool)
            or not isinstance(count, int)
            or count < (0 if allow_zero else 1)
            or (maximum is not None and count > maximum)
        ):
            raise CampaignCliError(f"daily plan {field} is outside its bound")
    if value.get(
        "discovery_expansion_headroom"
    ) != SCHEDULED_DISCOVERY_EXPANSION_HEADROOM or value.get(
        "discovery_target_limit"
    ) != min(
        SCHEDULED_DISCOVERY_TARGET_LIMIT_MAX,
        int(value["discovery_parent_target_count"])
        + SCHEDULED_DISCOVERY_EXPANSION_HEADROOM,
    ):
        raise CampaignCliError("daily plan discovery target limit is invalid")
    _digest(value.get("profile_targets_sha256"), "plan profile_targets_sha256")
    ranked = _normalize_daily_workloads(
        value.get("ranked_scope_workloads"), label="ranked_scope_workloads"
    )
    selected = _normalize_daily_workloads(
        value.get("scope_workloads"), label="scope_workloads"
    )
    if len(ranked) != active_count:
        raise CampaignCliError("daily plan ranked workloads are incomplete")
    ranked_scopes = [str(workload["scope"]) for workload in ranked]
    stable_ranked_sha256 = _digest(
        value.get("ranked_scope_ids_sha256"),
        "plan ranked_scope_ids_sha256",
    )
    if stable_ranked_sha256 != _scope_specs_sha256(ranked_scopes):
        raise CampaignCliError("daily plan stable scope ranking is invalid")
    if active_scopes_sha256 != _scope_specs_sha256(sorted(ranked_scopes)):
        raise CampaignCliError("daily plan active scope digest is invalid")
    ranked_sha = hashlib.sha256(canonical_json_bytes(ranked)).hexdigest()
    if value.get("ranked_workload_sha256") != ranked_sha:
        raise CampaignCliError("daily plan ranked workload digest is invalid")
    expected_selected = ranked[: min(max_scopes, active_count)]
    if selected != expected_selected or (
        require_full_active and len(selected) != active_count
    ):
        raise CampaignCliError(
            "daily plan selected workloads are not the exact rollout prefix"
        )
    expected_sha = hashlib.sha256(canonical_json_bytes(selected)).hexdigest()
    if value.get("workload_sha256") != expected_sha:
        raise CampaignCliError("daily plan workload digest is invalid")
    return dict(value)


def _verify_daily_plan_authority(
    *,
    plan: Mapping[str, object],
    rollout: Mapping[str, object],
    charter: Mapping[str, object],
) -> None:
    """Re-derive the owner-approved selection inside the offline signer."""

    cohort_sha256 = hashlib.sha256(canonical_json_bytes(dict(rollout))).hexdigest()
    plan_rollout_fields = (
        "rollout_id",
        "wave_id",
        "max_scopes",
        "require_full_active",
        "ranked_scope_ids_sha256",
    )
    charter_rollout_fields = (
        *plan_rollout_fields,
        "runtime_sha256",
        "classifier_sha256",
        "promotion_acceptance_sha256",
        "promotion_terminal_receipt_sha256",
    )
    if (
        plan.get("cohort_id") != rollout.get("cohort_id")
        or plan.get("cohort_sha256") != cohort_sha256
        or charter.get("cohort_id") != rollout.get("cohort_id")
        or charter.get("cohort_sha256") != cohort_sha256
        or any(plan.get(field) != rollout.get(field) for field in plan_rollout_fields)
        or any(
            charter.get(field) != rollout.get(field) for field in charter_rollout_fields
        )
    ):
        raise CampaignCliError("daily plan is outside the signed rollout charter")
    raw_scopes = rollout.get("ranked_scope_ids")
    ranked = plan.get("ranked_scope_workloads")
    workloads = plan.get("scope_workloads")
    if not all(isinstance(value, list) for value in (raw_scopes, ranked, workloads)):
        raise CampaignCliError("daily plan rollout selection is invalid")
    ranked_scopes = [
        str(workload.get("scope"))
        for workload in ranked
        if isinstance(workload, Mapping)
    ]
    if ranked_scopes != raw_scopes or len(ranked_scopes) != len(ranked):
        raise CampaignCliError(
            "daily plan ranked workloads differ from the frozen rollout order"
        )
    maximum = int(rollout["max_scopes"])
    expected = ranked[: min(maximum, len(ranked))]
    if workloads != expected:
        raise CampaignCliError("daily plan workloads are not the heavy-first wave")


def _split_scope_budgets(
    scope_pool_bytes: int, workloads: Sequence[Mapping[str, object]]
) -> list[int]:
    """Deterministic weighted largest-remainder allocation with saturation."""

    demands = [int(item["paid_target_count"]) for item in workloads]
    floors = [1_000_000 for _item in workloads]
    ceilings = [2_000_000 * demand for demand in demands]
    if scope_pool_bytes < sum(floors):
        raise CampaignCliError("scope budget does not fit workload-aware floor bounds")
    scope_pool_bytes = min(scope_pool_bytes, sum(ceilings))
    budgets = list(floors)
    remaining = scope_pool_bytes - sum(budgets)
    while remaining:
        active = [
            index for index, budget in enumerate(budgets) if budget < ceilings[index]
        ]
        if not active:
            raise CampaignCliError("scope budget allocation exhausted all ceilings")
        weight_sum = sum(demands[index] for index in active)
        before = remaining
        additions = [0] * len(workloads)
        for index in active:
            additions[index] = min(
                ceilings[index] - budgets[index],
                before * demands[index] // weight_sum,
            )
        progressed = sum(additions)
        for index, addition in enumerate(additions):
            budgets[index] += addition
        remaining -= progressed
        if remaining:
            ranked = sorted(
                (index for index in active if budgets[index] < ceilings[index]),
                key=lambda index: (
                    -(before * demands[index] % weight_sum),
                    str(workloads[index]["scope"]),
                ),
            )
            if not ranked:
                raise CampaignCliError("scope budget largest-remainder step stalled")
            for index in ranked[:remaining]:
                budgets[index] += 1
                remaining -= 1
            if progressed == 0 and remaining and len(ranked) == 0:
                raise CampaignCliError("scope budget allocation made no progress")
    return budgets


def _daily_ingest_unsigned(
    *,
    run_id: str,
    plan: Mapping[str, object],
    policy: Mapping[str, object],
    charter: Mapping[str, object],
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

    workloads = plan.get("scope_workloads")
    if not isinstance(workloads, list) or not workloads:
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
    scope_budgets = _split_scope_budgets(scope_pool, workloads)
    effective_scope_bytes = sum(scope_budgets)
    effective_total_bytes = discovery_bytes + profiles_bytes + effective_scope_bytes
    capture_bytes = effective_scope_bytes + profiles_bytes
    capture_paths = list(CANARY_ALLOWED_PATH_FAMILIES)
    discovery_paths = list(CANARY_DISCOVERY_PATH_FAMILIES)
    campaign_paths = sorted(set(capture_paths) | set(discovery_paths))
    discovery_target_limit = int(plan["discovery_target_limit"])
    profile_target_count = int(plan["profile_target_count"])
    profile_allocation_target_count = max(1, profile_target_count)

    allocations: list[dict[str, object]] = [
        {
            "allocation_id": DAILY_DISCOVERY_ALLOCATION_ID,
            "phase": "discovery",
            "workload_class": DAILY_DISCOVERY_WORKLOAD_CLASS,
            "work_item_id": DAILY_DISCOVERY_WORK_ITEM_ID,
            "task_id": DAILY_DISCOVERY_TASK_ID,
            "budget_bytes": discovery_bytes,
            "request_limit": 2 * discovery_target_limit,
            "lease_limit": discovery_target_limit,
            "allowed_path_families": discovery_paths,
        },
        {
            "allocation_id": DAILY_PROFILES_ALLOCATION_ID,
            "phase": "capture",
            "workload_class": DAILY_PROFILES_WORKLOAD_CLASS,
            "work_item_id": DAILY_PROFILES_WORK_ITEM_ID,
            "task_id": DAILY_PROFILES_TASK_ID,
            "budget_bytes": profiles_bytes,
            "request_limit": 2 * profile_allocation_target_count,
            "lease_limit": profile_allocation_target_count,
            "allowed_path_families": capture_paths,
        },
    ]
    for workload, budget in zip(workloads, scope_budgets):
        work_item_id = str(workload["work_item_id"])
        demand = int(workload["paid_target_count"])
        allocations.append(
            {
                "allocation_id": work_item_id,
                "phase": "capture",
                "workload_class": DAILY_SCOPE_WORKLOAD_CLASS,
                "work_item_id": work_item_id,
                "task_id": DAILY_SCOPE_TASK_ID,
                "budget_bytes": budget,
                "request_limit": 2 * demand,
                "lease_limit": demand,
                "allowed_path_families": capture_paths,
            }
        )
    allocations.sort(key=lambda item: item["allocation_id"])

    request_limit = sum(int(item["request_limit"]) for item in allocations)
    lease_limit = sum(int(item["lease_limit"]) for item in allocations)
    unsigned: dict[str, object] = {
        "schema_version": SCHEDULED_PROXY_CAMPAIGN_SCHEMA_VERSION,
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
            "total_provider_bytes": effective_total_bytes,
            "discovery_provider_bytes": discovery_bytes,
            "capture_provider_bytes": capture_bytes,
            "daily_provider_bytes": min(daily_bytes, effective_total_bytes),
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
        "scheduled_authority": {
            "provider_policy_sha256": policy["document_sha256"],
            "charter_sha256": charter["document_sha256"],
            "provider_id": policy["provider_id"],
            "order_id": policy["order_id"],
            "billing_month": charter["billing_month"],
            "cohort_id": plan["cohort_id"],
            "cohort_sha256": plan["cohort_sha256"],
            "rollout_id": plan["rollout_id"],
            "wave_id": plan["wave_id"],
            "max_scopes": plan["max_scopes"],
            "require_full_active": plan["require_full_active"],
            "catalog_batch_id": plan["catalog_batch_id"],
            "catalog_payload_sha256": plan["catalog_payload_sha256"],
            "catalog_active_scope_count": plan["catalog_active_scope_count"],
            "catalog_active_scopes_sha256": plan["catalog_active_scopes_sha256"],
            "ranked_scope_ids_sha256": plan["ranked_scope_ids_sha256"],
            "ranked_workload_sha256": plan["ranked_workload_sha256"],
            "runtime_sha256": charter["runtime_sha256"],
            "classifier_sha256": charter["classifier_sha256"],
            "promotion_acceptance_sha256": charter["promotion_acceptance_sha256"],
            "promotion_terminal_receipt_sha256": charter[
                "promotion_terminal_receipt_sha256"
            ],
            "workload_sha256": plan["workload_sha256"],
            "scope_workloads": list(workloads),
            "discovery_parent_target_count": plan["discovery_parent_target_count"],
            "discovery_expansion_headroom": plan["discovery_expansion_headroom"],
            "discovery_target_limit": plan["discovery_target_limit"],
            "profile_target_count": plan["profile_target_count"],
            "profile_targets_sha256": plan["profile_targets_sha256"],
            "daily_cap_bytes": charter["daily_cap_bytes"],
            "monthly_cap_bytes": charter["monthly_cap_bytes"],
            "order_cap_bytes": charter["order_cap_bytes"],
            "max_issuances": charter["max_issuances"],
        },
        "meter": PROXY_CAMPAIGN_METER,
        "signature_algorithm": PROXY_CAMPAIGN_SIGNATURE_ALGORITHM,
    }
    return unsigned


def _pointer_document(
    *, run_id: str, approval: ProxyCampaignApproval
) -> dict[str, object]:
    return {
        "schema_version": SCHEDULED_PAID_POINTER_SCHEMA_VERSION,
        "dag_id": WHOSCORED_INGEST_DAG_ID,
        "run_id": run_id,
        "approval_id": approval.approval_id,
        "approval_sha256": approval.approval_sha256,
    }


def _issuance_ledger_empty() -> dict[str, object]:
    return {"schema_version": ISSUANCE_LEDGER_SCHEMA_VERSION, "entries": []}


def _seal_issuance_ledger(
    body: Mapping[str, object], *, secret: str
) -> dict[str, object]:
    return {**body, "signature": _authority_signature(body, secret)}


def _validate_issuance_rollout_sequence(
    values: Sequence[Mapping[str, object]],
) -> None:
    """Reject first-wave skips, rollback, reactivation and release drift."""

    states: dict[str, Mapping[str, object]] = {}
    wave_order = tuple(_ROLLOUT_WAVE_CONTRACTS)
    stable_fields = (
        "ranked_scope_ids_sha256",
        "runtime_sha256",
        "classifier_sha256",
    )
    for value in values:
        rollout_id = str(value["rollout_id"])
        wave_id = str(value["wave_id"])
        wave_index = wave_order.index(wave_id)
        proof_values = (
            value["promotion_acceptance_sha256"],
            value["promotion_terminal_receipt_sha256"],
        )
        if (
            wave_index == 0
            and any(
                proof != WHOSCORED_ROLLOUT_GENESIS_PROOF_SHA256
                for proof in proof_values
            )
        ) or (
            wave_index != 0
            and any(
                proof == WHOSCORED_ROLLOUT_GENESIS_PROOF_SHA256
                for proof in proof_values
            )
        ):
            raise CampaignCliError(
                "issuance rollout promotion proof is invalid for its active wave"
            )
        previous = states.get(rollout_id)
        if previous is None:
            if wave_index != 0:
                raise CampaignCliError(
                    "issuance rollout must begin at wave-20 before provider spend"
                )
        else:
            previous_wave_index = wave_order.index(str(previous["wave_id"]))
            if any(value[field] != previous[field] for field in stable_fields):
                raise CampaignCliError(
                    "existing rollout_id has frozen ranking or release-pin drift"
                )
            if wave_index < previous_wave_index:
                raise CampaignCliError(
                    "issuance rollout cannot roll back or reactivate an earlier wave"
                )
            if wave_index > previous_wave_index + 1:
                raise CampaignCliError("issuance rollout cannot skip a rollout wave")
            if wave_index == previous_wave_index and any(
                value[field] != previous[field]
                for field in (
                    "cohort_sha256",
                    "promotion_acceptance_sha256",
                    "promotion_terminal_receipt_sha256",
                )
            ):
                raise CampaignCliError(
                    "active rollout wave cannot change cohort or acceptance proof"
                )
            if wave_index == previous_wave_index + 1 and any(
                value[field] == previous[field]
                for field in (
                    "promotion_acceptance_sha256",
                    "promotion_terminal_receipt_sha256",
                )
            ):
                raise CampaignCliError(
                    "adjacent rollout wave cannot reuse prior promotion evidence"
                )
        states[rollout_id] = value


def _read_issuance_ledger(path: Path, *, secret: str) -> dict[str, object]:
    if not path.exists():
        return _issuance_ledger_empty()
    value = _read_json_document(path, require_private=True)
    if not isinstance(value, Mapping) or set(value) != {
        "schema_version",
        "entries",
        "signature",
    }:
        raise CampaignCliError("issuance ledger fields are invalid")
    body = {
        "schema_version": value.get("schema_version"),
        "entries": value.get("entries"),
    }
    signature = value.get("signature")
    if (
        body["schema_version"] != ISSUANCE_LEDGER_SCHEMA_VERSION
        or not isinstance(body["entries"], list)
        or not isinstance(signature, str)
        or _SHA256_RE.fullmatch(signature) is None
        or not hmac.compare_digest(signature, _authority_signature(body, secret))
    ):
        raise CampaignCliError("issuance ledger authentication failed")
    entries = body["entries"]
    run_ids: set[str] = set()
    for entry in entries:
        if not isinstance(entry, Mapping) or set(entry) != {
            "run_id",
            "request_sha256",
            "order_id",
            "billing_month",
            "day",
            "charter_sha256",
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
            "total_provider_bytes",
            "approval",
            "pointer",
        }:
            raise CampaignCliError("issuance ledger entry is malformed")
        run_id = str(entry.get("run_id") or "")
        if not run_id or run_id in run_ids:
            raise CampaignCliError("issuance ledger run ids are not unique")
        run_ids.add(run_id)
        _digest(entry.get("request_sha256"), "ledger request_sha256")
        _digest(entry.get("charter_sha256"), "ledger charter_sha256")
        _token(entry.get("rollout_id"), "ledger rollout_id")
        _rollout_contract(entry, label="ledger entry")
        _digest(entry.get("cohort_sha256"), "ledger cohort_sha256")
        _digest(
            entry.get("ranked_scope_ids_sha256"),
            "ledger ranked_scope_ids_sha256",
        )
        _digest(entry.get("runtime_sha256"), "ledger runtime_sha256")
        _digest(entry.get("classifier_sha256"), "ledger classifier_sha256")
        _digest(
            entry.get("promotion_acceptance_sha256"),
            "ledger promotion_acceptance_sha256",
        )
        _digest(
            entry.get("promotion_terminal_receipt_sha256"),
            "ledger promotion_terminal_receipt_sha256",
        )
        _positive_bytes(
            entry.get("total_provider_bytes"), "ledger total_provider_bytes"
        )
        if not isinstance(entry.get("approval"), Mapping) or not isinstance(
            entry.get("pointer"), Mapping
        ):
            raise CampaignCliError("issuance ledger artifacts are malformed")
    _validate_issuance_rollout_sequence(entries)
    return dict(body)


def _issuance_lock(path: Path) -> int:
    path.parent.mkdir(parents=True, exist_ok=True)
    lock_path = path.with_suffix(path.suffix + ".lock")
    try:
        descriptor = os.open(
            lock_path,
            os.O_RDWR | os.O_CREAT | os.O_CLOEXEC | os.O_NOFOLLOW,
            0o600,
        )
    except OSError as exc:
        raise CampaignCliError("cannot open issuance ledger lock safely") from exc
    metadata = os.fstat(descriptor)
    if (
        not stat.S_ISREG(metadata.st_mode)
        or metadata.st_uid != os.geteuid()
        or metadata.st_nlink != 1
        or stat.S_IMODE(metadata.st_mode) != 0o600
    ):
        os.close(descriptor)
        raise CampaignCliError("issuance ledger lock metadata is unsafe")
    fcntl.flock(descriptor, fcntl.LOCK_EX)
    return descriptor


def _publish_or_verify(path: Path, value: Mapping[str, object]) -> None:
    if path.exists() or path.is_symlink():
        if _read_json_document(path, require_private=True) != value:
            raise CampaignCliError(f"immutable issued artifact differs: {path}")
        return
    _atomic_write_json(path, value, replace=False)


def command_issue_daily_ingest(args: argparse.Namespace) -> None:
    now = _issuance_now().astimezone(timezone.utc).replace(microsecond=0)
    wall_time = now.timetz()
    if not DAILY_ISSUER_WINDOW_START <= wall_time <= DAILY_ISSUER_WINDOW_END:
        raise CampaignCliError(
            "daily issuer wall clock must be inside 09:00..09:30 UTC"
        )
    issued = _utc(args.issued_at, "issued_at") if args.issued_at else now
    if not now - timedelta(minutes=5) <= issued <= now:
        raise CampaignCliError("issued_at must be within five minutes of wall clock")
    run_id = str(args.run_id)
    if not run_id.startswith("scheduled__"):
        raise CampaignCliError("daily ingest run_id must be a scheduled DagRun id")
    try:
        logical_run = _utc(run_id.removeprefix("scheduled__"), "run_id")
    except CampaignCliError as exc:
        raise CampaignCliError("daily ingest run_id timestamp is invalid") from exc
    if logical_run != _expected_daily_logical_date(now):
        raise CampaignCliError(
            "scheduled DagRun id is not the exact daily data-interval start"
        )
    owner_secret = _owner_secret(args)
    policy = _signed_provider_policy(
        Path(args.provider_policy), owner_secret=owner_secret, now=now
    )
    charter = _signed_charter(
        Path(args.charter), owner_secret=owner_secret, policy=policy, now=now
    )
    authority_valid_from = max(
        _utc(str(policy["valid_from"]), "provider policy valid_from"),
        _utc(str(charter["valid_from"]), "charter.valid_from"),
    )
    authority_valid_until = min(
        _utc(str(policy["valid_until"]), "provider policy valid_until"),
        _utc(str(charter["valid_until"]), "charter.valid_until"),
    )
    if issued < authority_valid_from:
        raise CampaignCliError(
            "issued_at is before the provider policy or charter validity window"
        )
    expires = (
        _utc(args.expires_at, "expires_at")
        if args.expires_at
        else min(issued + timedelta(hours=23), authority_valid_until)
    )
    if expires <= issued:
        raise CampaignCliError("expires_at must be after issued_at")
    if expires - issued > MAX_PROXY_CAMPAIGN_VALIDITY:
        raise CampaignCliError("daily approval validity may not exceed 24 hours")
    if expires > authority_valid_until:
        raise CampaignCliError(
            "expires_at exceeds the provider policy or charter validity window"
        )
    scheduled_start = logical_run + timedelta(days=1)
    if expires - scheduled_start < DAILY_INGEST_SCHEDULED_APPROVAL_HORIZON:
        raise CampaignCliError(
            "expires_at cannot cover the complete scheduled DagRun timeout window"
        )
    rollout = _read_rollout(Path(args.rollout_file), require_private=True)
    plan = _read_daily_plan(Path(args.plan_file), require_frozen=True)
    _verify_daily_plan_authority(
        plan=plan,
        rollout=rollout,
        charter=charter,
    )
    if (
        args.runtime_sha256 != charter["runtime_sha256"]
        or args.classifier_sha256 != charter["classifier_sha256"]
    ):
        raise CampaignCliError(
            "issuer release pins differ from the signed rollout charter"
        )
    total_bytes = int(charter["daily_cap_bytes"])
    daily_bytes = total_bytes
    discovery_reservation = min(
        DAILY_DISCOVERY_BUDGET_CEILING_BYTES,
        max(1, total_bytes // DAILY_DISCOVERY_BUDGET_DIVISOR),
    )
    profiles_reservation = min(
        DAILY_PROFILES_BUDGET_CEILING_BYTES,
        max(1, total_bytes // DAILY_PROFILES_BUDGET_DIVISOR),
    )
    scope_reservations = _split_scope_budgets(
        total_bytes - discovery_reservation - profiles_reservation,
        plan["scope_workloads"],
    )
    issued_total_bytes = (
        discovery_reservation + profiles_reservation + sum(scope_reservations)
    )
    if issued_total_bytes > int(charter["daily_cap_bytes"]) or min(
        daily_bytes, issued_total_bytes
    ) > int(charter["daily_cap_bytes"]):
        raise CampaignCliError("effective budget exceeds the charter daily allowance")
    run_digest = hashlib.sha256(run_id.encode("utf-8")).hexdigest()
    campaign_id = f"wsdaily-{run_digest[:32]}"
    approval_id = f"wsdaily-approval-{run_digest[:32]}"

    secret = _secret(args)
    approval_root = Path(args.approval_root)
    approval_path = approval_root / f"{approval_id}.json"
    pointer_root = Path(args.pointer_root)
    pointer_path = pointer_root / f"{run_digest}.json"
    request_body = {
        "run_id": run_id,
        "provider_policy_sha256": policy["document_sha256"],
        "charter_sha256": charter["document_sha256"],
        "plan_sha256": hashlib.sha256(canonical_json_bytes(plan)).hexdigest(),
        "rollout_id": charter["rollout_id"],
        "wave_id": charter["wave_id"],
        "max_scopes": charter["max_scopes"],
        "require_full_active": charter["require_full_active"],
        "ranked_scope_ids_sha256": charter["ranked_scope_ids_sha256"],
        "runtime_sha256": charter["runtime_sha256"],
        "classifier_sha256": charter["classifier_sha256"],
        "promotion_acceptance_sha256": charter["promotion_acceptance_sha256"],
        "promotion_terminal_receipt_sha256": charter[
            "promotion_terminal_receipt_sha256"
        ],
        "total_provider_bytes": total_bytes,
        "effective_total_provider_bytes": issued_total_bytes,
        "daily_provider_bytes": daily_bytes,
    }
    request_sha256 = hashlib.sha256(canonical_json_bytes(request_body)).hexdigest()
    ledger_path = Path(args.issuance_ledger)
    ledger_secret = _issuance_ledger_secret(args)
    lock_descriptor = _issuance_lock(ledger_path)
    try:
        ledger = _read_issuance_ledger(ledger_path, secret=ledger_secret)
        entries = list(ledger["entries"])
        rollout_state = {
            field: charter[field]
            for field in (
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
            )
        }
        _validate_issuance_rollout_sequence([*entries, rollout_state])
        existing = next(
            (entry for entry in entries if entry.get("run_id") == run_id), None
        )
        idempotent = existing is not None
        if existing is not None:
            if existing.get("request_sha256") != request_sha256:
                raise CampaignCliError("existing issuance run_id has request drift")
            signed = dict(existing["approval"])
            pointer = dict(existing["pointer"])
            approval = ProxyCampaignApproval.from_dict(signed)
            approval.verify(secret, now=now)
        else:
            day = now.date().isoformat()
            month = now.strftime("%Y-%m")
            order_id = str(charter["order_id"])
            charter_sha = str(charter["document_sha256"])
            order_total = sum(
                int(entry["total_provider_bytes"])
                for entry in entries
                if entry.get("order_id") == order_id
            )
            month_total = sum(
                int(entry["total_provider_bytes"])
                for entry in entries
                if entry.get("order_id") == order_id
                and entry.get("billing_month") == month
            )
            day_total = sum(
                int(entry["total_provider_bytes"])
                for entry in entries
                if entry.get("order_id") == order_id and entry.get("day") == day
            )
            charter_count = sum(
                1 for entry in entries if entry.get("charter_sha256") == charter_sha
            )
            if (
                month != charter["billing_month"]
                or day_total + issued_total_bytes > int(charter["daily_cap_bytes"])
                or month_total + issued_total_bytes > int(charter["monthly_cap_bytes"])
                or order_total + issued_total_bytes > int(charter["order_cap_bytes"])
                or charter_count >= int(charter["max_issuances"])
            ):
                raise CampaignCliError(
                    "issuance would exceed signed day/month/order/max_issuances caps"
                )
            unsigned = _daily_ingest_unsigned(
                run_id=run_id,
                plan=plan,
                policy=policy,
                charter=charter,
                runtime_sha256=args.runtime_sha256,
                classifier_sha256=args.classifier_sha256,
                issued=issued,
                expires=expires,
                total_bytes=total_bytes,
                daily_bytes=daily_bytes,
                campaign_id=campaign_id,
                approval_id=approval_id,
            )
            signed = sign_proxy_campaign_approval(unsigned, secret)
            approval = ProxyCampaignApproval.from_dict(signed)
            approval.verify(secret, now=now)
            pointer = _pointer_document(run_id=run_id, approval=approval)
            entries.append(
                {
                    "run_id": run_id,
                    "request_sha256": request_sha256,
                    "order_id": order_id,
                    "billing_month": month,
                    "day": day,
                    "charter_sha256": charter_sha,
                    "rollout_id": charter["rollout_id"],
                    "wave_id": charter["wave_id"],
                    "max_scopes": charter["max_scopes"],
                    "require_full_active": charter["require_full_active"],
                    "cohort_sha256": charter["cohort_sha256"],
                    "ranked_scope_ids_sha256": charter["ranked_scope_ids_sha256"],
                    "runtime_sha256": charter["runtime_sha256"],
                    "classifier_sha256": charter["classifier_sha256"],
                    "promotion_acceptance_sha256": charter[
                        "promotion_acceptance_sha256"
                    ],
                    "promotion_terminal_receipt_sha256": charter[
                        "promotion_terminal_receipt_sha256"
                    ],
                    "total_provider_bytes": approval.caps.total_provider_bytes,
                    "approval": signed,
                    "pointer": pointer,
                }
            )
            body = {
                "schema_version": ISSUANCE_LEDGER_SCHEMA_VERSION,
                "entries": entries,
            }
            _atomic_write_json(
                ledger_path,
                _seal_issuance_ledger(body, secret=ledger_secret),
                replace=ledger_path.exists(),
            )
        _publish_or_verify(approval_path, signed)
        _publish_or_verify(pointer_path, pointer)
    finally:
        fcntl.flock(lock_descriptor, fcntl.LOCK_UN)
        os.close(lock_descriptor)
    _print_summary(
        {
            "status": (
                "daily_ingest_approval_reused"
                if idempotent
                else "daily_ingest_approval_issued"
            ),
            "run_id": run_id,
            "approval_id": approval_id,
            "campaign_id": campaign_id,
            "approval_sha256": approval.approval_sha256,
            "approval_path": str(approval_path),
            "pointer_path": str(pointer_path),
            "order_id": charter["order_id"],
            "rollout_id": charter["rollout_id"],
            "wave_id": charter["wave_id"],
            "active_scope_count": len(plan["scope_workloads"]),
            "total_provider_bytes": approval.caps.total_provider_bytes,
            "daily_provider_bytes": approval.caps.daily_provider_bytes,
            "expires_at": approval.expires_at,
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
    return ProxyCampaignApproval.from_dict(
        _read_json_document(path, require_private=True)
    )


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


def _owner_secret_arguments(parser: argparse.ArgumentParser) -> None:
    group = parser.add_mutually_exclusive_group()
    group.add_argument("--owner-secret-file")
    group.add_argument(
        "--owner-secret-env",
        default=OWNER_HMAC_SECRET_ENV,
        help="environment variable containing the owner policy/charter HMAC key",
    )


def _issuance_ledger_secret_arguments(parser: argparse.ArgumentParser) -> None:
    group = parser.add_mutually_exclusive_group()
    group.add_argument("--issuance-ledger-secret-file")
    group.add_argument(
        "--issuance-ledger-secret-env",
        default=ISSUANCE_LEDGER_HMAC_SECRET_ENV,
        help="environment variable containing the issuance-ledger HMAC key",
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

    create_rollout = subparsers.add_parser(
        "create-rollout",
        help="freeze one exact heavy-first active-catalog ranking",
    )
    create_rollout.add_argument("--rollout-id", required=True)
    create_rollout.add_argument("--cohort-id", required=True)
    create_rollout.add_argument("--runtime-sha256", required=True)
    create_rollout.add_argument("--classifier-sha256", required=True)
    create_rollout.add_argument(
        "--wave-id",
        choices=("wave-20",),
        required=True,
    )
    _output_arguments(create_rollout)
    create_rollout.set_defaults(handler=command_create_rollout)

    promote_rollout = subparsers.add_parser(
        "promote-rollout",
        help="derive only the adjacent wave while preserving the frozen ranking",
    )
    promote_rollout.add_argument("--input", required=True)
    promote_rollout.add_argument("--cohort-id", required=True)
    promote_rollout.add_argument(
        "--acceptance-receipt",
        action="append",
        required=True,
        help="private <content-sha256>.json receipt; repeat for the exact source chain",
    )
    promote_rollout.add_argument(
        "--wave-id",
        choices=tuple(_ROLLOUT_WAVE_CONTRACTS),
        required=True,
    )
    _output_arguments(promote_rollout)
    promote_rollout.set_defaults(handler=command_promote_rollout)

    sign_policy = subparsers.add_parser(
        "sign-provider-policy", help="owner-sign one provider-policy-v1 document"
    )
    sign_policy.add_argument("--input", required=True)
    _owner_secret_arguments(sign_policy)
    _output_arguments(sign_policy)
    sign_policy.set_defaults(handler=command_sign_provider_policy)

    sign_charter = subparsers.add_parser(
        "sign-charter", help="owner-sign one scheduled rollout charter-v4 document"
    )
    sign_charter.add_argument("--input", required=True)
    _owner_secret_arguments(sign_charter)
    _output_arguments(sign_charter)
    sign_charter.set_defaults(handler=command_sign_charter)

    plan_daily = subparsers.add_parser(
        "plan-daily-ingest",
        help="freeze an exact heavy-first paid rollout wave",
    )
    plan_daily.add_argument("--rollout-file", required=True)
    _output_arguments(plan_daily)
    plan_daily.set_defaults(handler=command_plan_daily_ingest)

    issue_daily = subparsers.add_parser(
        "issue-daily-ingest",
        help="issue and sign one standing daily-ingest approval for a scheduled run",
    )
    issue_daily.add_argument("--run-id", required=True)
    issue_daily.add_argument("--plan-file", required=True)
    issue_daily.add_argument("--rollout-file", required=True)
    issue_daily.add_argument("--provider-policy", required=True)
    issue_daily.add_argument("--charter", required=True)
    issue_daily.add_argument("--runtime-sha256", required=True)
    issue_daily.add_argument("--classifier-sha256", required=True)
    issue_daily.add_argument("--issued-at", default="")
    issue_daily.add_argument("--expires-at", default="")
    issue_daily.add_argument("--approval-root", default=DEFAULT_APPROVAL_ROOT)
    issue_daily.add_argument(
        "--pointer-root", default=DEFAULT_SCHEDULED_PAID_POINTER_ROOT
    )
    issue_daily.add_argument("--issuance-ledger", default=DEFAULT_ISSUANCE_LEDGER_PATH)
    issue_daily.add_argument("--force", action="store_true")
    _secret_arguments(issue_daily)
    _owner_secret_arguments(issue_daily)
    _issuance_ledger_secret_arguments(issue_daily)
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
