"""Signed, immutable and restart-safe WhoScored paid-proxy campaigns.

The WhoScored transport is direct-only unless a caller presents a complete
HMAC-SHA256 approval.  An approval fixes every byte/request/lease ceiling and
every work allocation before the filtering proxy can dial a residential
provider.  :class:`ProxyCampaignLedger` then carries the spend across Airflow
continuations, retries, UTC-day changes and proxy-filter restarts.

This module deliberately has no Airflow, HTTP or proxy imports.  DAG code, the
transport client and the filtering proxy can therefore validate the exact same
wire document without creating a second policy implementation.
"""

from __future__ import annotations

import fcntl
import hashlib
import hmac
import json
import os
import re
import stat
import uuid
from dataclasses import dataclass
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import Any, Mapping, Sequence
from urllib.parse import urlsplit


PROXY_CAMPAIGN_SCHEMA_VERSION = 2
PROXY_CAMPAIGN_LEDGER_SCHEMA_VERSION = 1
PROXY_CAMPAIGN_ATTEMPT_JOURNAL_SCHEMA_VERSION = 1
PROXY_CAMPAIGN_RECONCILIATION_SEAL_SCHEMA_VERSION = 2
PROXY_CAMPAIGN_SOURCE = "whoscored"
PROXY_CAMPAIGN_SIGNATURE_ALGORITHM = "hmac-sha256"
PROXY_CAMPAIGN_METER = "proxy_filter_provider_billed_bytes_v1"
MAX_PROXY_CAMPAIGN_VALIDITY = timedelta(hours=24)
DEFAULT_WHOSCORED_PAID_CAP_BYTES = 0
# These release sentinels are deliberately code-owned rather than
# environment-controlled.  The exact 1 GB canary now has both an external
# provider traffic ceiling and durable provider-byte reconciliation.
WHOSCORED_PROVIDER_INVOICE_HARD_CAP_AVAILABLE = True
# The paid path is isolated behind the authenticated WhoScored gateway.  It
# validates the exact URL/TLS origin, owns browser sessions and receipts, and
# keeps approval/ledger/alert authorities out of source subprocesses.
WHOSCORED_PAID_APPLICATION_GATEWAY_AVAILABLE = True
# The measurement canary and a full paid crawl have intentionally separate
# release gates. Normal ingest/backfill reconciliation is not yet strong enough
# to authorize provider spend even after the canary boundary exists.
WHOSCORED_FULL_PAID_CRAWL_AVAILABLE = False
WHOSCORED_CANARY_CAP_BYTES = 1_000_000_000
WHOSCORED_CANARY_DISCOVERY_CAP_BYTES = 250_000_000
WHOSCORED_CANARY_CAPTURE_CAP_BYTES = (
    WHOSCORED_CANARY_CAP_BYTES - WHOSCORED_CANARY_DISCOVERY_CAP_BYTES
)
# Code-owned dial ceilings for the frozen two-phase canary.  A lease represents
# one source URL; the higher request ceiling allows at most one provider
# failover dial per lease on average.  Hitting either ceiling makes the
# measurement fail safely and requires a reviewed release change--the signing
# CLI cannot enlarge these limits.
WHOSCORED_CANARY_DISCOVERY_LEASE_LIMIT = 2_500
WHOSCORED_CANARY_CAPTURE_LEASE_LIMIT = 5_000
WHOSCORED_CANARY_DISCOVERY_REQUEST_LIMIT = 5_000
WHOSCORED_CANARY_CAPTURE_REQUEST_LIMIT = 10_000
WHOSCORED_CANARY_LEASE_LIMIT = (
    WHOSCORED_CANARY_DISCOVERY_LEASE_LIMIT + WHOSCORED_CANARY_CAPTURE_LEASE_LIMIT
)
WHOSCORED_CANARY_REQUEST_LIMIT = (
    WHOSCORED_CANARY_DISCOVERY_REQUEST_LIMIT + WHOSCORED_CANARY_CAPTURE_REQUEST_LIMIT
)

WHOSCORED_INGEST_DAG_ID = "dag_ingest_whoscored"
WHOSCORED_BACKFILL_DAG_ID = "dag_backfill_whoscored"
WHOSCORED_CANARY_DAG_ID = "dag_canary_whoscored_proxy"
WHOSCORED_CANARY_TASK_ID = "run_whoscored_proxy_canary"
WHOSCORED_CANARY_DISCOVERY_ALLOCATION_ID = "canary-full-history-catalog"
WHOSCORED_CANARY_DISCOVERY_WORK_ITEM_ID = "full-history-catalog"
WHOSCORED_CANARY_CAPTURE_ALLOCATION_ID = "canary-representative-cohort"
WHOSCORED_CANARY_CAPTURE_WORK_ITEM_ID = "representative-cohort"
WHOSCORED_CANARY_FIXED_SCOPES = (
    "ENG-Premier League=2526",
    "INT-World Cup=2026",
)
WHOSCORED_CANARY_DISCOVERY_PATH_FAMILIES = ("/", "/Regions", "/tournaments")
WHOSCORED_CANARY_ALLOWED_PATH_FAMILIES = (
    "/",
    "/Matches",
    "/Players",
    "/Regions",
    "/stagestatfeed",
    "/statisticsfeed/1/getplayerstatistics",
    "/statisticsfeed/1/getteamstatistics",
    "/tournaments",
)
WHOSCORED_CANARY_MAX_CONCURRENCY = 2
WHOSCORED_PAID_DAG_IDS = frozenset(
    {
        WHOSCORED_INGEST_DAG_ID,
        WHOSCORED_BACKFILL_DAG_ID,
        WHOSCORED_CANARY_DAG_ID,
    }
)

WHOSCORED_PROXY_ALLOWED_HOSTS = frozenset(
    {
        "www.whoscored.com",
        "cdn.whoscored.com",
        "challenges.cloudflare.com",
        "turnstile.cloudflare.com",
    }
)
PROXY_CAMPAIGN_PHASES = ("discovery", "capture")
PROXY_CAMPAIGN_STATES = frozenset(
    {"active", "awaiting_approval", "revoked", "complete", "sealed"}
)
TRANSPORT_POLICY_DIRECT_THEN_PAID = "direct_then_paid"
PROXY_APPROVAL_PATH_ENV = "WHOSCORED_PROXY_APPROVAL_PATH"
PROXY_APPROVAL_ID_ENV = "WHOSCORED_PROXY_APPROVAL_ID"
PROXY_APPROVAL_SHA256_ENV = "WHOSCORED_PROXY_APPROVAL_SHA256"
PROXY_ALLOCATION_ID_ENV = "WHOSCORED_PROXY_ALLOCATION_ID"
PROXY_ATTEMPT_ID_ENV = "WHOSCORED_PROXY_ATTEMPT_ID"
PROXY_APPROVAL_HMAC_SECRET_ENV = "WHOSCORED_PROXY_APPROVAL_HMAC_SECRET"
PROXY_LEDGER_HMAC_SECRET_ENV = "WHOSCORED_PROXY_LEDGER_HMAC_SECRET"
PROXY_CAMPAIGN_AUTHORITY_CONTEXT_FIELDS = frozenset(
    {
        "dag_id",
        "run_id",
        "transport_policy",
        "proxy_campaign_approval",
        "proxy_campaign_id",
        "proxy_approval_id",
        "proxy_approval_sha256",
    }
)
PROXY_CAMPAIGN_CONTROL_SCHEMA_VERSION = 1
PROXY_CAMPAIGN_CONTROL_ARGUMENT_FIELDS = {
    "snapshot": frozenset(),
    "complete_allocation": frozenset(
        {
            "allocation_id",
            "dag_id",
            "run_id",
            "task_id",
            "attempt_id",
            "report_sha256",
            "request_ledger_sha256",
        }
    ),
    "assert_exact_accounting": frozenset(
        {
            "task_report_provider_bytes",
            "request_ledger_provider_bytes",
            "proxy_ledger_provider_bytes",
            "require_complete",
        }
    ),
    "seal_for_reconciliation": frozenset(
        {
            "dag_id",
            "run_id",
            "provider_billed_bytes",
            "attempt_accounting_sha256",
        }
    ),
    "sealed_snapshot": frozenset(),
}
PROXY_CAMPAIGN_CONTROL_RESULT_FIELDS = {
    "snapshot": frozenset({"campaign"}),
    "complete_allocation": frozenset({"allocation"}),
    "assert_exact_accounting": frozenset({"provider_billed_bytes"}),
    "seal_for_reconciliation": frozenset({"campaign"}),
    "sealed_snapshot": frozenset({"campaign"}),
}
MAX_APPROVAL_DOCUMENT_BYTES = 4 * 1024 * 1024
MAX_ATTEMPT_JOURNAL_RECORD_BYTES = 16 * 1024

_ATTEMPT_FIELDS = frozenset(
    {
        "lease_id_hash",
        "attempt_id_hash",
        "canonical_url_sha256",
        "provider_billed_bytes",
        "provider_requests",
        "completed",
        "expired",
        "finished_at",
    }
)
_ATTEMPT_WITH_RESERVATION_FIELDS = _ATTEMPT_FIELDS | {
    "unsettled_provider_reservation_bytes"
}
_ATTEMPT_JOURNAL_STATE_FIELDS = frozenset(
    {"schema_version", "count", "offset", "tail_sha256"}
)
_ATTEMPT_JOURNAL_RECORD_BODY_FIELDS = frozenset(
    {
        "schema_version",
        "campaign_id",
        "approval_sha256",
        "sequence",
        "previous_record_sha256",
        "allocation_id",
        "retain_active_claim",
        "attempt",
    }
)
_ATTEMPT_JOURNAL_RECORD_FIELDS = _ATTEMPT_JOURNAL_RECORD_BODY_FIELDS | {
    "record_sha256",
    "signature",
}

_APPROVAL_FIELDS = frozenset(
    {
        "schema_version",
        "source",
        "approval_id",
        "campaign_id",
        "run_id",
        "issued_at",
        "expires_at",
        "transport_policy",
        "runtime_sha256",
        "classifier_sha256",
        "caps",
        "limits",
        "allowed_dag_ids",
        "allowed_hosts",
        "allowed_path_families",
        "allocations",
        "meter",
        "signature_algorithm",
        "approval_sha256",
        "signature",
    }
)
_UNSIGNED_APPROVAL_FIELDS = _APPROVAL_FIELDS - {"approval_sha256", "signature"}
_CAP_FIELDS = frozenset(
    {
        "total_provider_bytes",
        "discovery_provider_bytes",
        "capture_provider_bytes",
        "daily_provider_bytes",
    }
)
_LIMIT_FIELDS = frozenset({"requests", "leases", "concurrency"})
_ALLOCATION_FIELDS = frozenset(
    {
        "allocation_id",
        "phase",
        "workload_class",
        "work_item_id",
        "task_id",
        "budget_bytes",
        "request_limit",
        "lease_limit",
        "allowed_path_families",
    }
)
_TOKEN_RE = re.compile(r"\A[A-Za-z0-9][A-Za-z0-9._:-]{0,127}\Z", re.ASCII)
_SHA256_RE = re.compile(r"\A[0-9a-f]{64}\Z", re.ASCII)


class ProxyCampaignError(RuntimeError):
    """Base class for signed campaign and durable-accounting failures."""


class ProxyCampaignValidationError(ValueError, ProxyCampaignError):
    """An approval or work allocation is malformed or internally inconsistent."""


class ProxyCampaignSignatureError(ProxyCampaignValidationError):
    """An approval digest/signature is absent, stale or signed by another key."""


class ProxyCampaignExpired(ProxyCampaignValidationError):
    """An approval is not currently within its signed validity interval."""


class ProxyCampaignAccountingError(ProxyCampaignError):
    """Durable campaign state or caller-reported billed bytes are inconsistent."""


class ProxyCampaignBudgetExceeded(ProxyCampaignAccountingError):
    """The next lease/request/provider byte would cross a signed hard ceiling."""


class ProxyCampaignConcurrencyLimited(ProxyCampaignAccountingError):
    """The signed number of simultaneous campaign leases is already active."""


class ProxyCampaignRevoked(ProxyCampaignAccountingError):
    """The campaign was durably revoked and cannot be re-enabled by a retry."""


class _DuplicateJsonKey(ValueError):
    """Internal strict-decoder signal for a misleading JSON object."""


def _unique_json_object(pairs: list[tuple[str, Any]]) -> dict[str, Any]:
    result: dict[str, Any] = {}
    for key, value in pairs:
        if key in result:
            raise _DuplicateJsonKey(f"duplicate JSON key {key!r}")
        result[key] = value
    return result


def strict_json_loads(value: str | bytes | bytearray) -> Any:
    """Decode JSON while rejecting duplicate keys at every nesting level."""

    try:
        return json.loads(value, object_pairs_hook=_unique_json_object)
    except _DuplicateJsonKey as exc:
        raise ProxyCampaignValidationError(str(exc)) from exc


def canonical_json_bytes(value: object) -> bytes:
    """Return the one byte representation used by every campaign digest/HMAC."""

    try:
        return json.dumps(
            value,
            ensure_ascii=False,
            allow_nan=False,
            sort_keys=True,
            separators=(",", ":"),
        ).encode("utf-8")
    except (TypeError, ValueError) as exc:
        raise ProxyCampaignValidationError(
            "proxy campaign values must be canonical JSON"
        ) from exc


def _secret_bytes(secret: str | bytes) -> bytes:
    if isinstance(secret, str):
        value = secret.encode("utf-8")
    elif isinstance(secret, bytes):
        value = secret
    else:
        raise ProxyCampaignSignatureError("campaign HMAC secret must be text or bytes")
    if len(value) < 32:
        raise ProxyCampaignSignatureError(
            "campaign HMAC secret must contain at least 32 bytes"
        )
    return value


def _strict_fields(
    value: Mapping[str, object], expected: frozenset[str], name: str
) -> None:
    fields = frozenset(value)
    if fields != expected:
        missing = sorted(expected - fields)
        extra = sorted(fields - expected)
        detail = []
        if missing:
            detail.append("missing " + ", ".join(missing))
        if extra:
            detail.append("unknown " + ", ".join(extra))
        raise ProxyCampaignValidationError(
            f"{name} fields are invalid ({'; '.join(detail)})"
        )


def _token(value: object, field: str) -> str:
    if not isinstance(value, str) or _TOKEN_RE.fullmatch(value) is None:
        raise ProxyCampaignValidationError(f"{field} is not a canonical token")
    return value


def _identity(value: object, field: str) -> str:
    """Validate an external scheduler identity without rewriting its bytes."""

    if (
        not isinstance(value, str)
        or not value
        or value != value.strip()
        or len(value) > 512
        or any(ord(character) < 32 or ord(character) == 127 for character in value)
    ):
        raise ProxyCampaignValidationError(f"{field} is not a bounded identity")
    return value


def whoscored_canary_run_id(campaign_id: str) -> str:
    """Return the only Airflow DagRun identity valid for an exact canary."""

    return f"manual__{_token(campaign_id, 'campaign_id')}"


def _digest(value: object, field: str) -> str:
    if not isinstance(value, str) or _SHA256_RE.fullmatch(value) is None:
        raise ProxyCampaignValidationError(f"{field} must be a lowercase SHA-256")
    return value


def _integer(value: object, field: str, *, allow_zero: bool = False) -> int:
    minimum = 0 if allow_zero else 1
    if isinstance(value, bool) or not isinstance(value, int) or value < minimum:
        qualifier = "non-negative" if allow_zero else "positive"
        raise ProxyCampaignValidationError(f"{field} must be a {qualifier} integer")
    return value


def _utc_timestamp(value: object, field: str) -> datetime:
    if not isinstance(value, str) or not value.strip():
        raise ProxyCampaignValidationError(f"{field} must be an ISO-8601 timestamp")
    try:
        parsed = datetime.fromisoformat(value.replace("Z", "+00:00"))
    except ValueError as exc:
        raise ProxyCampaignValidationError(
            f"{field} must be an ISO-8601 timestamp"
        ) from exc
    if parsed.tzinfo is None or parsed.utcoffset() != timezone.utc.utcoffset(parsed):
        raise ProxyCampaignValidationError(f"{field} must use UTC")
    return parsed.astimezone(timezone.utc)


def _now(value: datetime | None = None) -> datetime:
    result = value or datetime.now(timezone.utc)
    if result.tzinfo is None:
        raise ProxyCampaignValidationError("verification time must be timezone-aware")
    return result.astimezone(timezone.utc)


def _sorted_unique_tokens(
    value: object,
    field: str,
    *,
    allowed: frozenset[str] | None = None,
) -> tuple[str, ...]:
    if not isinstance(value, list) or not value:
        raise ProxyCampaignValidationError(f"{field} must be a non-empty array")
    items = tuple(_token(item, f"{field}[]") for item in value)
    if items != tuple(sorted(set(items))):
        raise ProxyCampaignValidationError(f"{field} must be sorted and unique")
    if allowed is not None and not set(items) <= allowed:
        raise ProxyCampaignValidationError(f"{field} contains an unsupported value")
    return items


_INVALID_PATH_ESCAPE = re.compile(r"%(?![0-9A-Fa-f]{2})")
_AMBIGUOUS_PATH_ESCAPE = re.compile(r"%(?:25|2e|2f|5c)", re.IGNORECASE)


def _is_canonical_target_path(value: object) -> bool:
    """Reject path spellings that an HTTP layer may normalize differently."""

    if not isinstance(value, str) or not value.startswith("/"):
        return False
    if (
        "//" in value
        or "\\" in value
        or _INVALID_PATH_ESCAPE.search(value) is not None
        # Encoded percent is rejected too: it can expose an encoded dot/slash
        # after a second decoding pass in a downstream proxy or origin.
        or _AMBIGUOUS_PATH_ESCAPE.search(value) is not None
        or any(part in (".", "..") for part in value.split("/"))
    ):
        return False
    return True


def _path_family(value: object, field: str) -> str:
    if not isinstance(value, str) or not value.startswith("/"):
        raise ProxyCampaignValidationError(f"{field} must be an absolute URL path")
    if (
        len(value) > 512
        or "?" in value
        or "#" in value
        or not _is_canonical_target_path(value)
    ):
        raise ProxyCampaignValidationError(f"{field} is not a canonical path family")
    return value


def _path_families(value: object, field: str) -> tuple[str, ...]:
    if not isinstance(value, list) or not value:
        raise ProxyCampaignValidationError(f"{field} must be a non-empty array")
    paths = tuple(_path_family(item, f"{field}[]") for item in value)
    if paths != tuple(sorted(set(paths))):
        raise ProxyCampaignValidationError(f"{field} must be sorted and unique")
    return paths


def path_matches_family(path: str, family: str) -> bool:
    """Match an exact root or a prefix on a slash boundary.

    ``/`` is deliberately the exact site root rather than a wildcard.  This
    lets catalog discovery approve the WhoScored landing document without
    silently authorising every future path on the host.
    """

    if not _is_canonical_target_path(path) or not _is_canonical_target_path(family):
        return False
    if family == "/":
        return path == "/"
    prefix = family.rstrip("/")
    return path == prefix or path.startswith(prefix + "/")


@dataclass(frozen=True)
class ProxyWorkAllocation:
    """One HMAC-bound work item whose allowance survives task retries."""

    allocation_id: str
    phase: str
    workload_class: str
    work_item_id: str
    task_id: str
    budget_bytes: int
    request_limit: int
    lease_limit: int
    allowed_path_families: tuple[str, ...]

    @classmethod
    def from_dict(cls, value: object) -> "ProxyWorkAllocation":
        if not isinstance(value, Mapping):
            raise ProxyCampaignValidationError("proxy allocation must be an object")
        _strict_fields(value, _ALLOCATION_FIELDS, "proxy allocation")
        phase = _token(value.get("phase"), "allocation.phase")
        if phase not in PROXY_CAMPAIGN_PHASES:
            raise ProxyCampaignValidationError("allocation.phase is unsupported")
        result = cls(
            allocation_id=_token(value.get("allocation_id"), "allocation_id"),
            phase=phase,
            workload_class=_token(value.get("workload_class"), "workload_class"),
            work_item_id=_token(value.get("work_item_id"), "work_item_id"),
            task_id=_token(value.get("task_id"), "task_id"),
            budget_bytes=_integer(value.get("budget_bytes"), "budget_bytes"),
            request_limit=_integer(value.get("request_limit"), "request_limit"),
            lease_limit=_integer(value.get("lease_limit"), "lease_limit"),
            allowed_path_families=_path_families(
                value.get("allowed_path_families"),
                "allocation.allowed_path_families",
            ),
        )
        if result.lease_limit > result.request_limit:
            raise ProxyCampaignValidationError(
                "allocation.lease_limit cannot exceed request_limit"
            )
        return result

    def to_dict(self) -> dict[str, object]:
        return {
            "allocation_id": self.allocation_id,
            "phase": self.phase,
            "workload_class": self.workload_class,
            "work_item_id": self.work_item_id,
            "task_id": self.task_id,
            "budget_bytes": self.budget_bytes,
            "request_limit": self.request_limit,
            "lease_limit": self.lease_limit,
            "allowed_path_families": list(self.allowed_path_families),
        }

    def allows_url(self, url: str, *, allowed_hosts: Sequence[str]) -> bool:
        parts = urlsplit(str(url))
        host = (parts.hostname or "").lower().rstrip(".")
        return bool(
            parts.scheme == "https"
            and host in allowed_hosts
            and parts.port is None
            and parts.username is None
            and parts.password is None
            and not parts.fragment
            and any(
                path_matches_family(parts.path or "/", family)
                for family in self.allowed_path_families
            )
        )


@dataclass(frozen=True)
class ProxyCampaignCaps:
    total_provider_bytes: int
    discovery_provider_bytes: int
    capture_provider_bytes: int
    daily_provider_bytes: int

    @classmethod
    def from_dict(cls, value: object) -> "ProxyCampaignCaps":
        if not isinstance(value, Mapping):
            raise ProxyCampaignValidationError("campaign caps must be an object")
        _strict_fields(value, _CAP_FIELDS, "campaign caps")
        result = cls(
            total_provider_bytes=_integer(
                value.get("total_provider_bytes"), "caps.total_provider_bytes"
            ),
            discovery_provider_bytes=_integer(
                value.get("discovery_provider_bytes"),
                "caps.discovery_provider_bytes",
                allow_zero=True,
            ),
            capture_provider_bytes=_integer(
                value.get("capture_provider_bytes"),
                "caps.capture_provider_bytes",
                allow_zero=True,
            ),
            daily_provider_bytes=_integer(
                value.get("daily_provider_bytes"), "caps.daily_provider_bytes"
            ),
        )
        if (
            result.discovery_provider_bytes + result.capture_provider_bytes
            != result.total_provider_bytes
        ):
            raise ProxyCampaignValidationError(
                "phase byte caps must sum exactly to total_provider_bytes"
            )
        if result.daily_provider_bytes > result.total_provider_bytes:
            raise ProxyCampaignValidationError(
                "daily_provider_bytes cannot exceed total_provider_bytes"
            )
        return result

    def to_dict(self) -> dict[str, int]:
        return {
            "total_provider_bytes": self.total_provider_bytes,
            "discovery_provider_bytes": self.discovery_provider_bytes,
            "capture_provider_bytes": self.capture_provider_bytes,
            "daily_provider_bytes": self.daily_provider_bytes,
        }

    def phase_cap(self, phase: str) -> int:
        if phase == "discovery":
            return self.discovery_provider_bytes
        if phase == "capture":
            return self.capture_provider_bytes
        raise ProxyCampaignValidationError("unknown proxy campaign phase")


@dataclass(frozen=True)
class ProxyCampaignLimits:
    requests: int
    leases: int
    concurrency: int

    @classmethod
    def from_dict(cls, value: object) -> "ProxyCampaignLimits":
        if not isinstance(value, Mapping):
            raise ProxyCampaignValidationError("campaign limits must be an object")
        _strict_fields(value, _LIMIT_FIELDS, "campaign limits")
        result = cls(
            requests=_integer(value.get("requests"), "limits.requests"),
            leases=_integer(value.get("leases"), "limits.leases"),
            concurrency=_integer(value.get("concurrency"), "limits.concurrency"),
        )
        if result.leases > result.requests:
            raise ProxyCampaignValidationError("limits.leases cannot exceed requests")
        if result.concurrency > result.leases:
            raise ProxyCampaignValidationError(
                "limits.concurrency cannot exceed leases"
            )
        return result

    def to_dict(self) -> dict[str, int]:
        return {
            "requests": self.requests,
            "leases": self.leases,
            "concurrency": self.concurrency,
        }


@dataclass(frozen=True)
class ProxyCampaignApproval:
    """Canonical signed authority for one WhoScored paid campaign."""

    approval_id: str
    campaign_id: str
    run_id: str
    issued_at: str
    expires_at: str
    transport_policy: str
    runtime_sha256: str
    classifier_sha256: str
    caps: ProxyCampaignCaps
    limits: ProxyCampaignLimits
    allowed_dag_ids: tuple[str, ...]
    allowed_hosts: tuple[str, ...]
    allowed_path_families: tuple[str, ...]
    allocations: tuple[ProxyWorkAllocation, ...]
    approval_sha256: str
    signature: str

    @classmethod
    def from_dict(cls, value: object) -> "ProxyCampaignApproval":
        if not isinstance(value, Mapping):
            raise ProxyCampaignValidationError(
                "proxy campaign approval must be an object"
            )
        _strict_fields(value, _APPROVAL_FIELDS, "proxy campaign approval")
        if value.get("schema_version") != PROXY_CAMPAIGN_SCHEMA_VERSION:
            raise ProxyCampaignValidationError("unsupported campaign approval schema")
        if value.get("source") != PROXY_CAMPAIGN_SOURCE:
            raise ProxyCampaignValidationError("campaign source must be whoscored")
        if value.get("meter") != PROXY_CAMPAIGN_METER:
            raise ProxyCampaignValidationError("campaign uses an untrusted byte meter")
        if value.get("signature_algorithm") != PROXY_CAMPAIGN_SIGNATURE_ALGORITHM:
            raise ProxyCampaignValidationError(
                "unsupported campaign signature algorithm"
            )
        if value.get("transport_policy") != TRANSPORT_POLICY_DIRECT_THEN_PAID:
            raise ProxyCampaignValidationError(
                "paid approval requires transport_policy=direct_then_paid"
            )
        issued = _utc_timestamp(value.get("issued_at"), "issued_at")
        expires = _utc_timestamp(value.get("expires_at"), "expires_at")
        if expires <= issued:
            raise ProxyCampaignValidationError("expires_at must be after issued_at")
        if expires - issued > MAX_PROXY_CAMPAIGN_VALIDITY:
            raise ProxyCampaignValidationError(
                "proxy campaign validity may not exceed 24 hours"
            )
        raw_allocations = value.get("allocations")
        if not isinstance(raw_allocations, list) or not raw_allocations:
            raise ProxyCampaignValidationError("allocations must be a non-empty array")
        allocations = tuple(
            ProxyWorkAllocation.from_dict(item) for item in raw_allocations
        )
        allocation_ids = tuple(item.allocation_id for item in allocations)
        if allocation_ids != tuple(sorted(set(allocation_ids))):
            raise ProxyCampaignValidationError(
                "allocations must be sorted by unique allocation_id"
            )
        caps = ProxyCampaignCaps.from_dict(value.get("caps"))
        limits = ProxyCampaignLimits.from_dict(value.get("limits"))
        allowed_dags = _sorted_unique_tokens(
            value.get("allowed_dag_ids"),
            "allowed_dag_ids",
            allowed=WHOSCORED_PAID_DAG_IDS,
        )
        if len(allowed_dags) != 1:
            raise ProxyCampaignValidationError(
                "paid approval must bind exactly one Airflow DAG"
            )
        allowed_hosts = _sorted_unique_tokens(
            value.get("allowed_hosts"), "allowed_hosts"
        )
        if frozenset(allowed_hosts) != WHOSCORED_PROXY_ALLOWED_HOSTS:
            raise ProxyCampaignValidationError(
                "allowed_hosts must equal the strict WhoScored/Cloudflare allowlist"
            )
        allowed_paths = _path_families(
            value.get("allowed_path_families"), "allowed_path_families"
        )
        allowed_path_set = set(allowed_paths)
        if any(
            not set(item.allowed_path_families) <= allowed_path_set
            for item in allocations
        ):
            raise ProxyCampaignValidationError(
                "allocation path families must be a subset of campaign paths"
            )
        if sum(item.budget_bytes for item in allocations) != caps.total_provider_bytes:
            raise ProxyCampaignValidationError(
                "allocation byte budgets must sum exactly to the campaign cap"
            )
        for phase in PROXY_CAMPAIGN_PHASES:
            if sum(
                item.budget_bytes for item in allocations if item.phase == phase
            ) != caps.phase_cap(phase):
                raise ProxyCampaignValidationError(
                    f"{phase} allocation budgets must equal the signed phase cap"
                )
        if sum(item.request_limit for item in allocations) > limits.requests:
            raise ProxyCampaignValidationError(
                "allocation request limits exceed the campaign request cap"
            )
        if sum(item.lease_limit for item in allocations) > limits.leases:
            raise ProxyCampaignValidationError(
                "allocation lease limits exceed the campaign lease cap"
            )
        result = cls(
            approval_id=_token(value.get("approval_id"), "approval_id"),
            campaign_id=_token(value.get("campaign_id"), "campaign_id"),
            run_id=_identity(value.get("run_id"), "run_id"),
            issued_at=str(value["issued_at"]),
            expires_at=str(value["expires_at"]),
            transport_policy=TRANSPORT_POLICY_DIRECT_THEN_PAID,
            runtime_sha256=_digest(value.get("runtime_sha256"), "runtime_sha256"),
            classifier_sha256=_digest(
                value.get("classifier_sha256"), "classifier_sha256"
            ),
            caps=caps,
            limits=limits,
            allowed_dag_ids=allowed_dags,
            allowed_hosts=allowed_hosts,
            allowed_path_families=allowed_paths,
            allocations=allocations,
            approval_sha256=_digest(value.get("approval_sha256"), "approval_sha256"),
            signature=_digest(value.get("signature"), "signature"),
        )
        return result

    def unsigned_dict(self) -> dict[str, object]:
        return {
            "schema_version": PROXY_CAMPAIGN_SCHEMA_VERSION,
            "source": PROXY_CAMPAIGN_SOURCE,
            "approval_id": self.approval_id,
            "campaign_id": self.campaign_id,
            "run_id": self.run_id,
            "issued_at": self.issued_at,
            "expires_at": self.expires_at,
            "transport_policy": self.transport_policy,
            "runtime_sha256": self.runtime_sha256,
            "classifier_sha256": self.classifier_sha256,
            "caps": self.caps.to_dict(),
            "limits": self.limits.to_dict(),
            "allowed_dag_ids": list(self.allowed_dag_ids),
            "allowed_hosts": list(self.allowed_hosts),
            "allowed_path_families": list(self.allowed_path_families),
            "allocations": [item.to_dict() for item in self.allocations],
            "meter": PROXY_CAMPAIGN_METER,
            "signature_algorithm": PROXY_CAMPAIGN_SIGNATURE_ALGORITHM,
        }

    def to_dict(self) -> dict[str, object]:
        result = self.unsigned_dict()
        result["approval_sha256"] = self.approval_sha256
        result["signature"] = self.signature
        return result

    def verify(
        self,
        secret: str | bytes,
        now: datetime | None = None,
    ) -> None:
        self.verify_signature(secret)
        self.verify_validity(now)

    def verify_validity(self, now: datetime | None = None) -> None:
        """Validate only the signed time window, without reading an HMAC key."""

        current = _now(now)
        if current < _utc_timestamp(self.issued_at, "issued_at"):
            raise ProxyCampaignExpired("campaign approval is not active yet")
        if current >= _utc_timestamp(self.expires_at, "expires_at"):
            raise ProxyCampaignExpired("campaign approval has expired")

    def verify_signature(self, secret: str | bytes) -> None:
        """Authenticate immutable authority without granting current spend.

        This is intentionally separate from :meth:`verify`.  Only settlement
        of an already persisted claim may use signature-only verification;
        claim, request, reservation and byte-consume paths still require a
        currently active approval.
        """

        self.verify_digest()
        expected_signature = compute_approval_signature(
            {**self.unsigned_dict(), "approval_sha256": self.approval_sha256},
            secret,
        )
        if not hmac.compare_digest(self.signature, expected_signature):
            raise ProxyCampaignSignatureError("campaign approval HMAC is invalid")

    def verify_digest(self) -> None:
        """Bind the public approval digest to its canonical body.

        Source runners may use this structural check before forwarding the
        document to the isolated gateway.  It grants no authority: only the
        gateway/filter verify the approval HMAC.
        """

        expected_digest = hashlib.sha256(
            canonical_json_bytes(self.unsigned_dict())
        ).hexdigest()
        if not hmac.compare_digest(self.approval_sha256, expected_digest):
            raise ProxyCampaignSignatureError(
                "campaign approval SHA-256 does not match its canonical body"
            )

    def allocation(self, allocation_id: str) -> ProxyWorkAllocation:
        normalized = _token(allocation_id, "allocation_id")
        try:
            return next(
                item for item in self.allocations if item.allocation_id == normalized
            )
        except StopIteration as exc:
            raise ProxyCampaignValidationError(
                f"allocation {normalized!r} is absent from the approval"
            ) from exc

    @property
    def is_exact_canary(self) -> bool:
        if (
            self.allowed_dag_ids != (WHOSCORED_CANARY_DAG_ID,)
            or self.run_id != whoscored_canary_run_id(self.campaign_id)
            or self.caps.total_provider_bytes != WHOSCORED_CANARY_CAP_BYTES
            or self.caps.discovery_provider_bytes
            != WHOSCORED_CANARY_DISCOVERY_CAP_BYTES
            or self.caps.capture_provider_bytes != WHOSCORED_CANARY_CAPTURE_CAP_BYTES
            or self.caps.daily_provider_bytes != WHOSCORED_CANARY_CAP_BYTES
            or self.allowed_path_families != WHOSCORED_CANARY_ALLOWED_PATH_FAMILIES
            or len(self.allocations) != 2
            or not 1 <= self.limits.concurrency <= WHOSCORED_CANARY_MAX_CONCURRENCY
            or self.limits.requests != WHOSCORED_CANARY_REQUEST_LIMIT
            or self.limits.leases != WHOSCORED_CANARY_LEASE_LIMIT
        ):
            return False
        expected = {
            WHOSCORED_CANARY_DISCOVERY_ALLOCATION_ID: {
                "phase": "discovery",
                "workload_class": "catalog_discovery",
                "work_item_id": WHOSCORED_CANARY_DISCOVERY_WORK_ITEM_ID,
                "task_id": WHOSCORED_CANARY_TASK_ID,
                "budget_bytes": WHOSCORED_CANARY_DISCOVERY_CAP_BYTES,
                "request_limit": WHOSCORED_CANARY_DISCOVERY_REQUEST_LIMIT,
                "lease_limit": WHOSCORED_CANARY_DISCOVERY_LEASE_LIMIT,
                "allowed_path_families": (WHOSCORED_CANARY_DISCOVERY_PATH_FAMILIES),
            },
            WHOSCORED_CANARY_CAPTURE_ALLOCATION_ID: {
                "phase": "capture",
                "workload_class": "representative_cohort",
                "work_item_id": WHOSCORED_CANARY_CAPTURE_WORK_ITEM_ID,
                "task_id": WHOSCORED_CANARY_TASK_ID,
                "budget_bytes": WHOSCORED_CANARY_CAPTURE_CAP_BYTES,
                "request_limit": WHOSCORED_CANARY_CAPTURE_REQUEST_LIMIT,
                "lease_limit": WHOSCORED_CANARY_CAPTURE_LEASE_LIMIT,
                "allowed_path_families": WHOSCORED_CANARY_ALLOWED_PATH_FAMILIES,
            },
        }
        for item in self.allocations:
            policy = expected.get(item.allocation_id)
            if policy is None or any(
                getattr(item, field) != value for field, value in policy.items()
            ):
                return False
        return True

    def allows_url(self, url: str, *, allocation_id: str) -> bool:
        parts = urlsplit(str(url))
        host = (parts.hostname or "").lower().rstrip(".")
        path = parts.path or "/"
        return bool(
            parts.scheme == "https"
            and host in self.allowed_hosts
            and any(
                path_matches_family(path, item) for item in self.allowed_path_families
            )
            and self.allocation(allocation_id).allows_url(
                url, allowed_hosts=self.allowed_hosts
            )
        )


def compute_approval_signature(
    signed_body: Mapping[str, object], secret: str | bytes
) -> str:
    """HMAC a body that contains ``approval_sha256`` but no signature."""

    if frozenset(signed_body) != _UNSIGNED_APPROVAL_FIELDS | {"approval_sha256"}:
        raise ProxyCampaignSignatureError(
            "campaign signature body has missing or unknown fields"
        )
    return hmac.new(
        _secret_bytes(secret),
        canonical_json_bytes(dict(signed_body)),
        hashlib.sha256,
    ).hexdigest()


def sign_proxy_campaign_approval(
    unsigned_approval: Mapping[str, object], secret: str | bytes
) -> dict[str, object]:
    """Return a canonical signed approval from its exact unsigned wire body."""

    if frozenset(unsigned_approval) != _UNSIGNED_APPROVAL_FIELDS:
        raise ProxyCampaignValidationError(
            "unsigned campaign approval has missing or unknown fields"
        )
    unsigned = json.loads(canonical_json_bytes(dict(unsigned_approval)).decode("utf-8"))
    digest = hashlib.sha256(canonical_json_bytes(unsigned)).hexdigest()
    signed_body = {**unsigned, "approval_sha256": digest}
    result = {
        **signed_body,
        "signature": compute_approval_signature(signed_body, secret),
    }
    # Signing is also strict construction: no malformed document can be emitted.
    ProxyCampaignApproval.from_dict(result)
    return result


@dataclass(frozen=True, repr=False)
class ProxyCampaignClaim:
    campaign_id: str
    approval_id: str
    approval_sha256: str
    allocation_id: str
    phase: str
    lease_id: str
    claim_token: str
    allocation_budget_bytes: int
    allocation_spent_provider_bytes: int
    remaining_provider_bytes: int
    total_remaining_provider_bytes: int
    phase_remaining_provider_bytes: int
    daily_remaining_provider_bytes: int

    def __repr__(self) -> str:
        return (
            "ProxyCampaignClaim("
            f"campaign_id={self.campaign_id!r}, "
            f"allocation_id={self.allocation_id!r}, "
            f"remaining_provider_bytes={self.remaining_provider_bytes})"
        )


def _state_token_hash(value: str) -> str:
    return hashlib.sha256(value.encode("utf-8")).hexdigest()


class ProxyCampaignLedger:
    """HMAC-protected campaign ledger with atomic claims and exact byte deltas."""

    def __init__(
        self,
        path: os.PathLike[str] | str,
        *,
        secret: str | bytes,
        approval_secret: str | bytes | None = None,
        require_existing: bool = False,
    ) -> None:
        self.path = Path(path)
        self.lock_path = self.path.with_suffix(self.path.suffix + ".lock")
        # Ledger-state authentication and approval-signing authority are
        # separate trust domains in production.  The compatibility default is
        # intentionally limited to existing offline/tests which use one key.
        self._secret = _secret_bytes(secret)
        self._approval_secret = _secret_bytes(
            secret if approval_secret is None else approval_secret
        )
        self._require_existing = bool(require_existing)

    def attempt_journal_path(self, campaign_id: str) -> Path:
        """Return the non-secret per-campaign append-only evidence path."""

        normalized = _token(campaign_id, "campaign_id")
        suffix = hashlib.sha256(normalized.encode("utf-8")).hexdigest()
        return self.path.with_name(f"{self.path.name}.attempts-{suffix}.jsonl")

    def initialize_empty(self) -> None:
        """Create a first authenticated empty state; never replace one."""

        handle = self._locked()
        try:
            if self.path.exists() or self.path.is_symlink():
                raise ProxyCampaignAccountingError(
                    "campaign ledger is already initialized"
                )
            self._write(self._empty())
        finally:
            fcntl.flock(handle.fileno(), fcntl.LOCK_UN)
            handle.close()

    def verify_integrity(self) -> Mapping[str, object]:
        """Authenticate the main state and every referenced journal."""

        handle = self._locked()
        try:
            if self.path.is_symlink() or not self.path.is_file():
                raise ProxyCampaignAccountingError("campaign ledger state is missing")
            return self._read()
        finally:
            fcntl.flock(handle.fileno(), fcntl.LOCK_UN)
            handle.close()

    @classmethod
    def _provider_order_accounting_from_body(
        cls,
        body: Mapping[str, Any],
        *,
        current_day: str,
    ) -> dict[str, int]:
        campaigns = body.get("campaigns")
        if not isinstance(campaigns, Mapping):
            raise ProxyCampaignAccountingError(
                "campaign ledger campaigns are malformed"
            )
        spent = 0
        reserved = 0
        current_day_spent = 0
        current_day_reserved = 0
        for campaign in campaigns.values():
            if not isinstance(campaign, Mapping):
                raise ProxyCampaignAccountingError(
                    "campaign ledger campaign is malformed"
                )
            campaign_spent = campaign.get("spent_provider_bytes")
            daily_spend = campaign.get("daily_spend")
            if (
                isinstance(campaign_spent, bool)
                or not isinstance(campaign_spent, int)
                or campaign_spent < 0
                or not isinstance(daily_spend, Mapping)
            ):
                raise ProxyCampaignAccountingError(
                    "campaign provider accounting is malformed"
                )
            day_spent = daily_spend.get(current_day, 0)
            if (
                isinstance(day_spent, bool)
                or not isinstance(day_spent, int)
                or day_spent < 0
            ):
                raise ProxyCampaignAccountingError(
                    "campaign daily provider accounting is malformed"
                )
            spent += campaign_spent
            current_day_spent += day_spent
            reserved += cls._reserved_provider_bytes(campaign)
            current_day_reserved += cls._reserved_provider_bytes(
                campaign,
                day=current_day,
            )
        return {
            "spent_provider_bytes": spent,
            "reserved_provider_bytes": reserved,
            "exposure_provider_bytes": spent + reserved,
            "current_day_spent_provider_bytes": current_day_spent,
            "current_day_reserved_provider_bytes": current_day_reserved,
        }

    def provider_order_accounting(
        self,
        *,
        now: datetime | None = None,
    ) -> Mapping[str, int]:
        """Return durable spend and escrow across every campaign in this state.

        A protected filter-state directory represents one residential provider
        order.  Summing every campaign, rather than trusting the current
        approval, prevents a second approval or a UTC-day rollover from
        resetting the provider-order safety margin.
        """

        current_day = _now(now).date().isoformat()
        handle = self._locked()
        try:
            return self._provider_order_accounting_from_body(
                self._read(),
                current_day=current_day,
            )
        finally:
            fcntl.flock(handle.fileno(), fcntl.LOCK_UN)
            handle.close()

    @staticmethod
    def _journal_seed(campaign_id: str, approval_sha256: str) -> str:
        return hashlib.sha256(
            canonical_json_bytes(
                {
                    "schema_version": PROXY_CAMPAIGN_ATTEMPT_JOURNAL_SCHEMA_VERSION,
                    "campaign_id": campaign_id,
                    "approval_sha256": approval_sha256,
                }
            )
        ).hexdigest()

    @classmethod
    def _empty_journal_state(
        cls, campaign_id: str, approval_sha256: str
    ) -> dict[str, object]:
        return {
            "schema_version": PROXY_CAMPAIGN_ATTEMPT_JOURNAL_SCHEMA_VERSION,
            "count": 0,
            "offset": 0,
            "tail_sha256": cls._journal_seed(campaign_id, approval_sha256),
        }

    @classmethod
    def _journal_state(cls, campaign: Mapping[str, Any]) -> dict[str, Any] | None:
        value = campaign.get("attempt_journal")
        if value is None:
            return None
        if not isinstance(value, dict) or frozenset(value) != (
            _ATTEMPT_JOURNAL_STATE_FIELDS
        ):
            raise ProxyCampaignAccountingError(
                "campaign attempt journal state is malformed"
            )
        count = value.get("count")
        offset = value.get("offset")
        if (
            value.get("schema_version") != PROXY_CAMPAIGN_ATTEMPT_JOURNAL_SCHEMA_VERSION
            or isinstance(count, bool)
            or not isinstance(count, int)
            or count < 0
            or isinstance(offset, bool)
            or not isinstance(offset, int)
            or offset < 0
        ):
            raise ProxyCampaignAccountingError(
                "campaign attempt journal state is malformed"
            )
        try:
            tail = _digest(value.get("tail_sha256"), "tail_sha256")
            campaign_id = _token(campaign.get("campaign_id"), "campaign_id")
            approval_sha256 = _digest(
                campaign.get("approval_sha256"), "approval_sha256"
            )
        except ProxyCampaignValidationError as exc:
            raise ProxyCampaignAccountingError(
                "campaign attempt journal state is malformed"
            ) from exc
        if count == 0 and (
            offset != 0 or tail != cls._journal_seed(campaign_id, approval_sha256)
        ):
            raise ProxyCampaignAccountingError(
                "empty campaign attempt journal state is inconsistent"
            )
        if count > 0 and offset == 0:
            raise ProxyCampaignAccountingError(
                "campaign attempt journal offset is inconsistent"
            )
        return value

    @staticmethod
    def _validate_attempt(value: object) -> dict[str, Any]:
        if not isinstance(value, dict) or frozenset(value) not in {
            _ATTEMPT_FIELDS,
            _ATTEMPT_WITH_RESERVATION_FIELDS,
        }:
            raise ProxyCampaignAccountingError(
                "campaign attempt journal record is malformed"
            )
        try:
            _digest(value.get("lease_id_hash"), "lease_id_hash")
            _digest(value.get("attempt_id_hash"), "attempt_id_hash")
            _digest(value.get("canonical_url_sha256"), "canonical_url_sha256")
            _utc_timestamp(value.get("finished_at"), "finished_at")
        except ProxyCampaignValidationError as exc:
            raise ProxyCampaignAccountingError(
                "campaign attempt journal record is malformed"
            ) from exc
        for field in ("provider_billed_bytes", "provider_requests"):
            item = value.get(field)
            if isinstance(item, bool) or not isinstance(item, int) or item < 0:
                raise ProxyCampaignAccountingError(
                    "campaign attempt journal record is malformed"
                )
        if not isinstance(value.get("completed"), bool) or not isinstance(
            value.get("expired"), bool
        ):
            raise ProxyCampaignAccountingError(
                "campaign attempt journal record is malformed"
            )
        reservation = value.get("unsettled_provider_reservation_bytes", 0)
        if (
            isinstance(reservation, bool)
            or not isinstance(reservation, int)
            or reservation < 0
        ):
            raise ProxyCampaignAccountingError(
                "campaign attempt journal record is malformed"
            )
        return value

    def _seal_journal_record(self, body: Mapping[str, object]) -> dict[str, object]:
        digest = hashlib.sha256(canonical_json_bytes(body)).hexdigest()
        signature = hmac.new(
            self._secret,
            canonical_json_bytes({**body, "record_sha256": digest}),
            hashlib.sha256,
        ).hexdigest()
        return {**body, "record_sha256": digest, "signature": signature}

    def _decode_journal_record(
        self,
        raw: bytes,
        campaign: Mapping[str, Any],
        *,
        expected_sequence: int,
        expected_previous: str | None,
    ) -> dict[str, Any]:
        if (
            not raw.endswith(b"\n")
            or len(raw) > MAX_ATTEMPT_JOURNAL_RECORD_BYTES
            or raw == b"\n"
        ):
            raise ProxyCampaignAccountingError(
                "campaign attempt journal record is truncated or oversized"
            )
        try:
            value = strict_json_loads(raw[:-1])
        except (
            UnicodeDecodeError,
            json.JSONDecodeError,
            ProxyCampaignValidationError,
        ) as exc:
            raise ProxyCampaignAccountingError(
                "campaign attempt journal record is corrupt"
            ) from exc
        if (
            not isinstance(value, dict)
            or frozenset(value) != _ATTEMPT_JOURNAL_RECORD_FIELDS
            or canonical_json_bytes(value) + b"\n" != raw
        ):
            raise ProxyCampaignAccountingError(
                "campaign attempt journal record is non-canonical"
            )
        body = {
            field: value.get(field) for field in _ATTEMPT_JOURNAL_RECORD_BODY_FIELDS
        }
        try:
            campaign_id = _token(campaign.get("campaign_id"), "campaign_id")
            approval_sha256 = _digest(
                campaign.get("approval_sha256"), "approval_sha256"
            )
            record_campaign_id = _token(value.get("campaign_id"), "campaign_id")
            record_approval_sha256 = _digest(
                value.get("approval_sha256"), "approval_sha256"
            )
            previous = _digest(
                value.get("previous_record_sha256"), "previous_record_sha256"
            )
            allocation_id = _token(value.get("allocation_id"), "allocation_id")
            record_sha256 = _digest(value.get("record_sha256"), "record_sha256")
            signature = _digest(value.get("signature"), "signature")
        except ProxyCampaignValidationError as exc:
            raise ProxyCampaignAccountingError(
                "campaign attempt journal record is malformed"
            ) from exc
        sequence = value.get("sequence")
        retain = value.get("retain_active_claim")
        attempt = self._validate_attempt(value.get("attempt"))
        if (
            value.get("schema_version") != PROXY_CAMPAIGN_ATTEMPT_JOURNAL_SCHEMA_VERSION
            or record_campaign_id != campaign_id
            or record_approval_sha256 != approval_sha256
            or isinstance(sequence, bool)
            or sequence != expected_sequence
            or not isinstance(retain, bool)
            or (
                expected_previous is not None
                and not hmac.compare_digest(previous, expected_previous)
            )
            or allocation_id not in campaign.get("allocations", {})
        ):
            raise ProxyCampaignAccountingError(
                "campaign attempt journal chain is inconsistent"
            )
        reservation = int(attempt.get("unsettled_provider_reservation_bytes", 0))
        if (retain and reservation <= 0) or (not retain and reservation != 0):
            raise ProxyCampaignAccountingError(
                "campaign attempt journal transition is inconsistent"
            )
        expected_digest = hashlib.sha256(canonical_json_bytes(body)).hexdigest()
        expected_signature = hmac.new(
            self._secret,
            canonical_json_bytes({**body, "record_sha256": expected_digest}),
            hashlib.sha256,
        ).hexdigest()
        if not hmac.compare_digest(record_sha256, expected_digest) or not (
            hmac.compare_digest(signature, expected_signature)
        ):
            raise ProxyCampaignAccountingError(
                "campaign attempt journal authentication failed"
            )
        return value

    @staticmethod
    def _journal_descriptor(path: Path, flags: int, mode: int = 0o600) -> int:
        try:
            descriptor = os.open(
                path,
                flags | os.O_CLOEXEC | os.O_NOFOLLOW,
                mode,
            )
        except OSError as exc:
            raise ProxyCampaignAccountingError(
                "campaign attempt journal cannot be opened safely"
            ) from exc
        metadata = os.fstat(descriptor)
        if (
            not stat.S_ISREG(metadata.st_mode)
            or metadata.st_uid != os.geteuid()
            or stat.S_IMODE(metadata.st_mode) != 0o600
            or metadata.st_nlink != 1
        ):
            os.close(descriptor)
            raise ProxyCampaignAccountingError(
                "campaign attempt journal ownership or mode is unsafe"
            )
        return descriptor

    def _validate_committed_journal_tail(
        self,
        descriptor: int,
        campaign: Mapping[str, Any],
        state: Mapping[str, Any],
    ) -> None:
        count = int(state["count"])
        offset = int(state["offset"])
        if count == 0:
            return
        start = max(0, offset - MAX_ATTEMPT_JOURNAL_RECORD_BYTES)
        data = os.pread(descriptor, offset - start, start)
        if len(data) != offset - start or not data.endswith(b"\n"):
            raise ProxyCampaignAccountingError(
                "campaign attempt journal committed tail is truncated"
            )
        previous_newline = data.rfind(b"\n", 0, len(data) - 1)
        record = data[previous_newline + 1 :]
        decoded = self._decode_journal_record(
            record,
            campaign,
            expected_sequence=count,
            expected_previous=None,
        )
        if not hmac.compare_digest(
            str(decoded["record_sha256"]), str(state["tail_sha256"])
        ):
            raise ProxyCampaignAccountingError(
                "campaign attempt journal tail differs from signed state"
            )

    @staticmethod
    def _find_transition_claim(
        campaign: Mapping[str, Any], record: Mapping[str, Any]
    ) -> tuple[str, dict[str, Any], dict[str, Any]]:
        attempts = record.get("attempt")
        claims = campaign.get("active_claims")
        allocations = campaign.get("allocations")
        if (
            not isinstance(attempts, Mapping)
            or not isinstance(claims, dict)
            or not isinstance(allocations, dict)
        ):
            raise ProxyCampaignAccountingError(
                "campaign attempt transition state is malformed"
            )
        matches = [
            (lease_id, claim)
            for lease_id, claim in claims.items()
            if isinstance(lease_id, str)
            and isinstance(claim, dict)
            and hmac.compare_digest(
                _state_token_hash(lease_id), str(attempts.get("lease_id_hash"))
            )
        ]
        if len(matches) != 1:
            raise ProxyCampaignAccountingError(
                "campaign attempt journal lead has no unique active claim"
            )
        lease_id, active = matches[0]
        allocation_id = record.get("allocation_id")
        allocation = allocations.get(allocation_id)
        reservation = attempts.get("unsettled_provider_reservation_bytes", 0)
        expected_expired = _utc_timestamp(
            attempts.get("finished_at"), "finished_at"
        ) >= _utc_timestamp(active.get("expires_at"), "claim.expires_at")
        if (
            not isinstance(allocation, dict)
            or active.get("allocation_id") != allocation_id
            or active.get("attempt_id_hash") != attempts.get("attempt_id_hash")
            or active.get("canonical_url_sha256")
            != attempts.get("canonical_url_sha256")
            or int(active.get("spent_provider_bytes", -1))
            != attempts.get("provider_billed_bytes")
            or int(active.get("requests_used", -1)) != attempts.get("provider_requests")
            or int(active.get("reserved_provider_bytes", -1)) != reservation
            or attempts.get("expired") is not expected_expired
            or (
                record.get("retain_active_claim") is True
                and (
                    attempts.get("completed") is not False
                    or attempts.get("expired") is not True
                )
            )
            or "terminal_attempt_sequence" in active
        ):
            raise ProxyCampaignAccountingError(
                "campaign attempt journal lead differs from its active claim"
            )
        return lease_id, allocation, active

    def _apply_attempt_record(
        self,
        campaign: dict[str, Any],
        record: Mapping[str, Any],
        *,
        new_offset: int,
    ) -> None:
        lease_id, allocation, active = self._find_transition_claim(campaign, record)
        attempt = record["attempt"]
        if record["retain_active_claim"]:
            active["terminal_attempt_sequence"] = record["sequence"]
            active["terminal_attempt_sha256"] = record["record_sha256"]
            campaign["status"] = "revoked"
            campaign["revocation_reason"] = (
                "unsettled provider-byte reservation after lease expiry"
            )
            campaign["awaiting_reason"] = ""
        else:
            del campaign["active_claims"][lease_id]
        if attempt["completed"]:
            allocation["completed"] = True
        if campaign.get("status") != "revoked" and all(
            item.get("completed") is True for item in campaign["allocations"].values()
        ):
            campaign["status"] = "complete"
            campaign["awaiting_reason"] = ""
        state = self._journal_state(campaign)
        if state is None:
            raise ProxyCampaignAccountingError(
                "campaign attempt journal state is missing"
            )
        state["count"] = record["sequence"]
        state["offset"] = new_offset
        state["tail_sha256"] = record["record_sha256"]
        campaign["updated_at"] = attempt["finished_at"]

    def _sync_campaign_journal(self, campaign: dict[str, Any]) -> bool:
        state = self._journal_state(campaign)
        if state is None:
            seal = campaign.get("reconciliation_seal")
            if (
                campaign.get("status") == "sealed"
                and isinstance(seal, Mapping)
                and seal.get("schema_version") == 1
            ):
                return False
            raise ProxyCampaignAccountingError(
                "unsealed inline campaign evidence predates the append-only journal"
            )
        allocations = campaign.get("allocations")
        if not isinstance(allocations, Mapping) or any(
            not isinstance(allocation, Mapping) or allocation.get("attempts") != []
            for allocation in allocations.values()
        ):
            raise ProxyCampaignAccountingError(
                "journal-backed campaign contains inline terminal attempts"
            )
        path = self.attempt_journal_path(str(campaign["campaign_id"]))
        try:
            descriptor = self._journal_descriptor(path, os.O_RDWR)
        except ProxyCampaignAccountingError as exc:
            if not os.path.lexists(path):
                if int(state["count"]) == 0 and int(state["offset"]) == 0:
                    return False
                raise ProxyCampaignAccountingError(
                    "campaign attempt journal is missing"
                ) from exc
            raise
        try:
            size = os.fstat(descriptor).st_size
            offset = int(state["offset"])
            if size < offset:
                raise ProxyCampaignAccountingError(
                    "campaign attempt journal is shorter than signed state"
                )
            self._validate_committed_journal_tail(descriptor, campaign, state)
            if size == offset:
                return False
            suffix_size = size - offset
            if suffix_size > MAX_ATTEMPT_JOURNAL_RECORD_BYTES:
                raise ProxyCampaignAccountingError(
                    "campaign attempt journal has multiple or oversized leads"
                )
            suffix = os.pread(descriptor, suffix_size, offset)
            if len(suffix) != suffix_size:
                raise ProxyCampaignAccountingError(
                    "campaign attempt journal lead cannot be read atomically"
                )
            if b"\n" not in suffix:
                # The signed main state proves that these bytes were never
                # committed. A process may die during write(2), before fsync.
                os.ftruncate(descriptor, offset)
                os.fsync(descriptor)
                return False
            if not suffix.endswith(b"\n") or suffix.count(b"\n") != 1:
                raise ProxyCampaignAccountingError(
                    "campaign attempt journal has an invalid complete lead"
                )
            record = self._decode_journal_record(
                suffix,
                campaign,
                expected_sequence=int(state["count"]) + 1,
                expected_previous=str(state["tail_sha256"]),
            )
            self._find_transition_claim(campaign, record)
            self._apply_attempt_record(
                campaign,
                record,
                new_offset=offset + len(suffix),
            )
            return True
        finally:
            os.close(descriptor)

    def _read_full_journal(self, campaign: Mapping[str, Any]) -> list[dict[str, Any]]:
        state = self._journal_state(campaign)
        if state is None:
            raise ProxyCampaignAccountingError(
                "campaign has no append-only attempt journal"
            )
        count = int(state["count"])
        offset = int(state["offset"])
        if count == 0:
            path = self.attempt_journal_path(str(campaign["campaign_id"]))
            try:
                descriptor = self._journal_descriptor(path, os.O_RDONLY)
            except ProxyCampaignAccountingError:
                if not os.path.lexists(path):
                    return []
                raise
            try:
                size = os.fstat(descriptor).st_size
            finally:
                os.close(descriptor)
            if size != 0:
                raise ProxyCampaignAccountingError(
                    "empty campaign attempt journal has unexpected bytes"
                )
            return []
        path = self.attempt_journal_path(str(campaign["campaign_id"]))
        descriptor = self._journal_descriptor(path, os.O_RDONLY)
        records: list[dict[str, Any]] = []
        seen_leases: set[str] = set()
        previous = self._journal_seed(
            str(campaign["campaign_id"]), str(campaign["approval_sha256"])
        )
        consumed = 0
        try:
            if os.fstat(descriptor).st_size != offset:
                raise ProxyCampaignAccountingError(
                    "campaign attempt journal size differs from signed state"
                )
            with os.fdopen(os.dup(descriptor), "rb") as stream:
                for sequence, raw in enumerate(stream, start=1):
                    record = self._decode_journal_record(
                        raw,
                        campaign,
                        expected_sequence=sequence,
                        expected_previous=previous,
                    )
                    lease_hash = str(record["attempt"]["lease_id_hash"])
                    if lease_hash in seen_leases:
                        raise ProxyCampaignAccountingError(
                            "campaign attempt journal contains a duplicate lease"
                        )
                    seen_leases.add(lease_hash)
                    records.append(record)
                    previous = str(record["record_sha256"])
                    consumed += len(raw)
        finally:
            os.close(descriptor)
        if (
            len(records) != count
            or consumed != offset
            or not hmac.compare_digest(previous, str(state["tail_sha256"]))
        ):
            raise ProxyCampaignAccountingError(
                "campaign attempt journal chain differs from signed state"
            )
        return records

    def _after_attempt_journal_fsync(self, record: Mapping[str, Any]) -> None:
        """Crash-injection point after evidence durability, before state removal."""

    def _append_attempt_record(
        self,
        body: dict[str, Any],
        campaign: dict[str, Any],
        *,
        allocation_id: str,
        attempt: Mapping[str, Any],
        retain_active_claim: bool,
    ) -> dict[str, Any]:
        state = self._journal_state(campaign)
        if state is None:
            raise ProxyCampaignAccountingError(
                "campaign attempt journal state is missing"
            )
        record = self._seal_journal_record(
            {
                "schema_version": PROXY_CAMPAIGN_ATTEMPT_JOURNAL_SCHEMA_VERSION,
                "campaign_id": campaign["campaign_id"],
                "approval_sha256": campaign["approval_sha256"],
                "sequence": int(state["count"]) + 1,
                "previous_record_sha256": state["tail_sha256"],
                "allocation_id": allocation_id,
                "retain_active_claim": retain_active_claim,
                "attempt": dict(attempt),
            }
        )
        raw = canonical_json_bytes(record) + b"\n"
        if len(raw) > MAX_ATTEMPT_JOURNAL_RECORD_BYTES:
            raise ProxyCampaignAccountingError(
                "campaign attempt journal record is oversized"
            )
        decoded = self._decode_journal_record(
            raw,
            campaign,
            expected_sequence=int(state["count"]) + 1,
            expected_previous=str(state["tail_sha256"]),
        )
        self._find_transition_claim(campaign, decoded)
        path = self.attempt_journal_path(str(campaign["campaign_id"]))
        path.parent.mkdir(parents=True, exist_ok=True)
        try:
            descriptor = self._journal_descriptor(
                path,
                os.O_WRONLY | os.O_APPEND | os.O_CREAT | os.O_EXCL,
            )
        except ProxyCampaignAccountingError:
            if not os.path.lexists(path):
                raise
            descriptor = self._journal_descriptor(
                path,
                os.O_WRONLY | os.O_APPEND,
            )
        try:
            if os.fstat(descriptor).st_size != int(state["offset"]):
                raise ProxyCampaignAccountingError(
                    "campaign attempt journal changed before append"
                )
            if int(state["count"]) == 0 and int(state["offset"]) == 0:
                # Prove the first journal dentry durable before any record can
                # reach fsync. Do this for a pre-existing empty journal too:
                # it may be the residue of a power loss between O_EXCL and the
                # original parent-directory fsync.
                directory = os.open(
                    path.parent,
                    os.O_RDONLY | os.O_DIRECTORY | os.O_CLOEXEC | os.O_NOFOLLOW,
                )
                try:
                    os.fsync(directory)
                finally:
                    os.close(directory)
            position = 0
            while position < len(raw):
                written = os.write(descriptor, raw[position:])
                if written <= 0:
                    raise ProxyCampaignAccountingError(
                        "campaign attempt journal append made no progress"
                    )
                position += written
            os.fsync(descriptor)
        finally:
            os.close(descriptor)
        self._after_attempt_journal_fsync(decoded)
        self._apply_attempt_record(
            campaign,
            decoded,
            new_offset=int(state["offset"]) + len(raw),
        )
        self._write(body)
        return decoded

    def _locked(self):
        try:
            self.lock_path.parent.mkdir(parents=True, exist_ok=True)
            directory = os.open(
                self.lock_path.parent,
                os.O_RDONLY | os.O_DIRECTORY | os.O_CLOEXEC | os.O_NOFOLLOW,
            )
        except OSError as exc:
            raise ProxyCampaignAccountingError(
                "campaign ledger lock directory is unavailable"
            ) from exc

        descriptor = -1
        handle = None
        try:
            descriptor = os.open(
                self.lock_path.name,
                os.O_RDWR | os.O_CREAT | os.O_CLOEXEC | os.O_NOFOLLOW,
                0o600,
                dir_fd=directory,
            )
            metadata = os.fstat(descriptor)
            if (
                not stat.S_ISREG(metadata.st_mode)
                or stat.S_IMODE(metadata.st_mode) != 0o600
                or metadata.st_uid != os.geteuid()
                or metadata.st_nlink != 1
            ):
                raise ProxyCampaignAccountingError(
                    "campaign ledger lock file is unsafe"
                )
            handle = os.fdopen(descriptor, "r+", encoding="utf-8")
            descriptor = -1
            fcntl.flock(handle.fileno(), fcntl.LOCK_EX)
            locked_handle = handle
            handle = None
            return locked_handle
        except ProxyCampaignAccountingError:
            raise
        except OSError as exc:
            raise ProxyCampaignAccountingError(
                "campaign ledger lock is unavailable"
            ) from exc
        finally:
            if descriptor >= 0:
                os.close(descriptor)
            if handle is not None and handle.closed is False:
                handle.close()
            os.close(directory)

    def _empty(self) -> dict[str, object]:
        return {
            "schema_version": PROXY_CAMPAIGN_LEDGER_SCHEMA_VERSION,
            "campaigns": {},
        }

    def _seal(self, body: Mapping[str, object]) -> dict[str, object]:
        digest = hashlib.sha256(canonical_json_bytes(body)).hexdigest()
        signature = hmac.new(
            self._secret,
            canonical_json_bytes({**body, "ledger_sha256": digest}),
            hashlib.sha256,
        ).hexdigest()
        return {**body, "ledger_sha256": digest, "signature": signature}

    def _read(self) -> dict[str, Any]:
        if not self.path.exists():
            if self._require_existing:
                raise ProxyCampaignAccountingError("campaign ledger state is missing")
            return self._empty()
        try:
            value = strict_json_loads(self.path.read_text(encoding="utf-8"))
        except (
            OSError,
            UnicodeDecodeError,
            json.JSONDecodeError,
            ProxyCampaignValidationError,
        ) as exc:
            raise ProxyCampaignAccountingError("campaign ledger is corrupt") from exc
        if not isinstance(value, dict) or set(value) != {
            "schema_version",
            "campaigns",
            "ledger_sha256",
            "signature",
        }:
            raise ProxyCampaignAccountingError("unsupported campaign ledger")
        body = {
            "schema_version": value.get("schema_version"),
            "campaigns": value.get("campaigns"),
        }
        if body[
            "schema_version"
        ] != PROXY_CAMPAIGN_LEDGER_SCHEMA_VERSION or not isinstance(
            body["campaigns"], dict
        ):
            raise ProxyCampaignAccountingError("unsupported campaign ledger")
        expected_digest = hashlib.sha256(canonical_json_bytes(body)).hexdigest()
        expected_signature = hmac.new(
            self._secret,
            canonical_json_bytes({**body, "ledger_sha256": expected_digest}),
            hashlib.sha256,
        ).hexdigest()
        if not hmac.compare_digest(
            str(value.get("ledger_sha256")), expected_digest
        ) or not hmac.compare_digest(str(value.get("signature")), expected_signature):
            raise ProxyCampaignAccountingError("campaign ledger authentication failed")
        recovered = False
        for campaign in body["campaigns"].values():
            if not isinstance(campaign, dict):
                raise ProxyCampaignAccountingError("persisted campaign is malformed")
            recovered = self._sync_campaign_journal(campaign) or recovered
        if recovered:
            # A terminal record reached fsync before the preceding process
            # could atomically publish the bounded main state. Replay exactly
            # that transition before exposing the ledger to another caller.
            self._write(body)
        return body  # type: ignore[return-value]

    def _write(self, body: Mapping[str, object]) -> None:
        self.path.parent.mkdir(parents=True, exist_ok=True)
        temporary = self.path.with_name(
            f"{self.path.name}.tmp-{os.getpid()}-{uuid.uuid4().hex}"
        )
        sealed = self._seal(body)
        try:
            descriptor = os.open(temporary, os.O_CREAT | os.O_EXCL | os.O_WRONLY, 0o600)
            try:
                with os.fdopen(descriptor, "w", encoding="utf-8") as stream:
                    json.dump(
                        sealed,
                        stream,
                        ensure_ascii=False,
                        indent=2,
                        sort_keys=True,
                    )
                    stream.write("\n")
                    stream.flush()
                    os.fsync(stream.fileno())
            except BaseException:
                try:
                    os.close(descriptor)
                except OSError:
                    pass
                raise
            os.replace(temporary, self.path)
            directory = os.open(self.path.parent, os.O_RDONLY | os.O_DIRECTORY)
            try:
                os.fsync(directory)
            finally:
                os.close(directory)
        finally:
            try:
                temporary.unlink()
            except FileNotFoundError:
                pass

    @classmethod
    def _new_campaign(cls, approval: ProxyCampaignApproval) -> dict[str, object]:
        return {
            "campaign_id": approval.campaign_id,
            "approval": approval.to_dict(),
            "approval_id": approval.approval_id,
            "approval_sha256": approval.approval_sha256,
            "status": "active",
            "awaiting_reason": "",
            "revocation_reason": "",
            "spent_provider_bytes": 0,
            "phase_spend": {phase: 0 for phase in PROXY_CAMPAIGN_PHASES},
            "daily_spend": {},
            "requests_used": 0,
            "leases_used": 0,
            "run_id_hash": _state_token_hash(approval.run_id),
            "dag_id_hash": _state_token_hash(approval.allowed_dag_ids[0]),
            "active_claims": {},
            "attempt_journal": cls._empty_journal_state(
                approval.campaign_id, approval.approval_sha256
            ),
            "allocations": {
                item.allocation_id: {
                    **item.to_dict(),
                    "spent_provider_bytes": 0,
                    "requests_used": 0,
                    "leases_used": 0,
                    "completed": False,
                    "completed_run_id_hash": "",
                    "completed_dag_id_hash": "",
                    "completed_task_id": "",
                    "completed_attempt_id_hash": "",
                    "report_sha256": "",
                    "request_ledger_sha256": "",
                    "completed_at": "",
                    "attempts": [],
                }
                for item in approval.allocations
            },
            "created_at": datetime.now(timezone.utc).isoformat(),
            "updated_at": datetime.now(timezone.utc).isoformat(),
        }

    def _register(
        self,
        body: dict[str, Any],
        approval: ProxyCampaignApproval,
        *,
        allow_sealed: bool = False,
    ) -> dict[str, Any]:
        campaigns = body["campaigns"]
        campaign = campaigns.get(approval.campaign_id)
        if campaign is None:
            campaign = self._new_campaign(approval)
            campaigns[approval.campaign_id] = campaign
            return campaign
        if not isinstance(campaign, dict):
            raise ProxyCampaignAccountingError("persisted campaign is malformed")
        if campaign.get("status") == "sealed" and not allow_sealed:
            raise ProxyCampaignAccountingError(
                "sealed proxy campaign evidence is terminal and immutable"
            )
        if campaign.get("status") == "revoked":
            raise ProxyCampaignRevoked("proxy campaign is revoked")
        if campaign.get("approval_sha256") == approval.approval_sha256:
            return campaign
        raise ProxyCampaignAccountingError(
            "proxy campaign approval is immutable; issue a new campaign_id "
            "instead of topping up an existing campaign"
        )

    def _reap_expired(
        self,
        body: dict[str, Any],
        campaign: dict[str, Any],
        now: datetime,
    ) -> int:
        claims = campaign.get("active_claims")
        if not isinstance(claims, dict):
            raise ProxyCampaignAccountingError("campaign active claims are malformed")
        reaped = 0
        for lease_id, claim in list(claims.items()):
            if not isinstance(claim, dict):
                raise ProxyCampaignAccountingError("campaign claim is malformed")
            if "terminal_attempt_sequence" in claim:
                continue
            if _utc_timestamp(claim.get("expires_at"), "claim.expires_at") <= now:
                allocation = campaign["allocations"].get(claim.get("allocation_id"))
                if not isinstance(allocation, dict):
                    raise ProxyCampaignAccountingError(
                        "expired claim allocation is missing"
                    )
                reservation = int(claim.get("reserved_provider_bytes", 0))
                attempt = {
                    "lease_id_hash": _state_token_hash(str(lease_id)),
                    "attempt_id_hash": claim.get("attempt_id_hash"),
                    "canonical_url_sha256": claim.get("canonical_url_sha256"),
                    "provider_billed_bytes": int(claim.get("spent_provider_bytes", 0)),
                    "provider_requests": int(claim.get("requests_used", 0)),
                    "unsettled_provider_reservation_bytes": reservation,
                    "completed": False,
                    "expired": True,
                    "finished_at": now.isoformat(),
                }
                self._append_attempt_record(
                    body,
                    campaign,
                    allocation_id=str(claim.get("allocation_id")),
                    attempt=attempt,
                    retain_active_claim=reservation > 0,
                )
                reaped += 1
                if reservation > 0:
                    # The proxy process disappeared before it could prove how
                    # much of the pre-I/O escrow reached the provider. Keep the
                    # bounded live-claim evidence after journal archival.
                    break
        return reaped

    @staticmethod
    def _reserved_provider_bytes(
        campaign: Mapping[str, Any],
        *,
        allocation_id: str | None = None,
        phase: str | None = None,
        day: str | None = None,
    ) -> int:
        claims = campaign.get("active_claims")
        if not isinstance(claims, Mapping):
            raise ProxyCampaignAccountingError("campaign active claims are malformed")
        total = 0
        for claim in claims.values():
            if not isinstance(claim, Mapping):
                raise ProxyCampaignAccountingError("campaign claim is malformed")
            if (
                allocation_id is not None
                and claim.get("allocation_id") != allocation_id
            ):
                continue
            if phase is not None and claim.get("phase") != phase:
                continue
            if day is not None and claim.get("reservation_day") != day:
                continue
            amount = claim.get("reserved_provider_bytes", 0)
            if isinstance(amount, bool) or not isinstance(amount, int) or amount < 0:
                raise ProxyCampaignAccountingError(
                    "campaign provider-byte reservation is malformed"
                )
            total += amount
        return total

    @staticmethod
    def _remaining(
        approval: ProxyCampaignApproval,
        campaign: Mapping[str, Any],
        allocation: Mapping[str, Any],
        *,
        day: str,
    ) -> tuple[int, int, int, int]:
        allocation_remaining = (
            int(allocation["budget_bytes"])
            - int(allocation.get("spent_provider_bytes", 0))
            - ProxyCampaignLedger._reserved_provider_bytes(
                campaign,
                allocation_id=str(allocation["allocation_id"]),
            )
        )
        total_remaining = (
            approval.caps.total_provider_bytes
            - int(campaign.get("spent_provider_bytes", 0))
            - ProxyCampaignLedger._reserved_provider_bytes(campaign)
        )
        phase = str(allocation["phase"])
        phase_spend = campaign.get("phase_spend")
        daily_spend = campaign.get("daily_spend")
        if not isinstance(phase_spend, Mapping) or not isinstance(daily_spend, Mapping):
            raise ProxyCampaignAccountingError("campaign byte counters are malformed")
        phase_remaining = (
            approval.caps.phase_cap(phase)
            - int(phase_spend.get(phase, 0))
            - ProxyCampaignLedger._reserved_provider_bytes(
                campaign,
                phase=phase,
            )
        )
        daily_remaining = (
            approval.caps.daily_provider_bytes
            - int(daily_spend.get(day, 0))
            - ProxyCampaignLedger._reserved_provider_bytes(
                campaign,
                day=day,
            )
        )
        return (
            allocation_remaining,
            total_remaining,
            phase_remaining,
            daily_remaining,
        )

    def claim(
        self,
        approval: ProxyCampaignApproval,
        allocation_id: str,
        *,
        dag_id: str,
        run_id: str,
        task_id: str,
        attempt_id: str,
        lease_id: str,
        expires_at: datetime,
        canonical_url: str,
        claim_token: str | None = None,
        now: datetime | None = None,
    ) -> ProxyCampaignClaim:
        current = _now(now)
        approval.verify(self._approval_secret, now=current)
        normalized_dag_id = _token(dag_id, "dag_id")
        if normalized_dag_id != approval.allowed_dag_ids[0]:
            raise ProxyCampaignValidationError("DAG differs from the paid approval")
        normalized_run_id = _identity(run_id, "run_id")
        if normalized_run_id != approval.run_id:
            raise ProxyCampaignValidationError(
                "lease run_id differs from the signed campaign run_id"
            )
        _token(task_id, "task_id")
        _token(attempt_id, "attempt_id")
        _token(lease_id, "lease_id")
        if expires_at.tzinfo is None or expires_at <= current:
            raise ProxyCampaignValidationError("lease expiry must be in the future")
        if expires_at.astimezone(timezone.utc) > _utc_timestamp(
            approval.expires_at, "expires_at"
        ):
            raise ProxyCampaignValidationError(
                "lease expiry may not exceed the signed approval expiry"
            )
        if expires_at.astimezone(timezone.utc).date() != current.date():
            raise ProxyCampaignValidationError(
                "lease may not carry provider-byte escrow across a UTC day"
            )
        allocation_spec = approval.allocation(allocation_id)
        if task_id != allocation_spec.task_id:
            raise ProxyCampaignValidationError(
                "lease task_id differs from the signed allocation"
            )
        if not approval.allows_url(canonical_url, allocation_id=allocation_id):
            raise ProxyCampaignValidationError(
                "canonical_url is outside the signed host/path families"
            )
        token = claim_token or uuid.uuid4().hex
        if not isinstance(token, str) or len(token) < 32:
            raise ProxyCampaignAccountingError(
                "claim_token must contain at least 32 characters"
            )
        handle = self._locked()
        try:
            body = self._read()
            campaign = self._register(body, approval)
            # Reaping can revoke the campaign while retaining unknown escrow.
            # Persist that forensic state before any fail-closed exception below;
            # otherwise every retry would rediscover the same orphan only in
            # memory and the signed ledger would still claim it was active.
            self._reap_expired(body, campaign, current)
            status = campaign.get("status")
            if status == "revoked":
                raise ProxyCampaignRevoked("proxy campaign is revoked")
            if status in {"sealed", "complete"}:
                raise ProxyCampaignAccountingError(
                    "proxy campaign is terminal and cannot mint another claim"
                )
            run_id_hash = _state_token_hash(approval.run_id)
            dag_id_hash = _state_token_hash(normalized_dag_id)
            persisted_run_id_hash = campaign.get("run_id_hash", "")
            if (
                persisted_run_id_hash != run_id_hash
                or campaign.get("dag_id_hash") != dag_id_hash
            ):
                raise ProxyCampaignValidationError(
                    "campaign ledger DAG/run differs from the signed authority"
                )
            claims = campaign.get("active_claims")
            allocations = campaign.get("allocations")
            if not isinstance(claims, dict) or not isinstance(allocations, dict):
                raise ProxyCampaignAccountingError("campaign state is malformed")
            if lease_id in claims:
                raise ProxyCampaignAccountingError("lease_id already has a claim")
            if len(claims) >= approval.limits.concurrency:
                raise ProxyCampaignConcurrencyLimited(
                    "proxy campaign concurrency limit reached"
                )
            if int(campaign.get("leases_used", 0)) >= approval.limits.leases:
                campaign["status"] = "awaiting_approval"
                campaign["awaiting_reason"] = "lease_limit"
                self._write(body)
                raise ProxyCampaignBudgetExceeded("campaign lease limit exhausted")
            if int(campaign.get("requests_used", 0)) >= approval.limits.requests:
                campaign["status"] = "awaiting_approval"
                campaign["awaiting_reason"] = "request_limit"
                self._write(body)
                raise ProxyCampaignBudgetExceeded("campaign request limit exhausted")
            allocation = allocations.get(allocation_id)
            if not isinstance(allocation, dict):
                raise ProxyCampaignAccountingError("signed allocation is not persisted")
            if allocation.get("completed") is True:
                raise ProxyCampaignAccountingError(
                    "work allocation is already complete"
                )
            if int(allocation.get("leases_used", 0)) >= allocation_spec.lease_limit:
                raise ProxyCampaignBudgetExceeded("allocation lease limit exhausted")
            if int(allocation.get("requests_used", 0)) >= allocation_spec.request_limit:
                raise ProxyCampaignBudgetExceeded("allocation request limit exhausted")
            remaining = self._remaining(
                approval, campaign, allocation, day=current.date().isoformat()
            )
            if min(remaining) <= 0:
                campaign["status"] = "awaiting_approval"
                campaign["awaiting_reason"] = "byte_cap"
                self._write(body)
                raise ProxyCampaignBudgetExceeded(
                    "campaign/allocation provider byte cap exhausted"
                )
            claims[lease_id] = {
                "allocation_id": allocation_id,
                "approval_id": approval.approval_id,
                "approval_sha256": approval.approval_sha256,
                "claim_token_hash": _state_token_hash(token),
                "attempt_id_hash": _state_token_hash(attempt_id),
                "dag_id": normalized_dag_id,
                "run_id_hash": run_id_hash,
                "task_id": task_id,
                "canonical_url_sha256": hashlib.sha256(
                    canonical_url.encode("utf-8")
                ).hexdigest(),
                "started_at": current.isoformat(),
                "expires_at": expires_at.astimezone(timezone.utc).isoformat(),
                "phase": allocation_spec.phase,
                "spent_provider_bytes": 0,
                "escrow_provider_bytes": 0,
                "reserved_provider_bytes": 0,
                "reservation_day": current.date().isoformat(),
                "requests_used": 0,
            }
            campaign["leases_used"] = int(campaign.get("leases_used", 0)) + 1
            allocation["leases_used"] = int(allocation.get("leases_used", 0)) + 1
            campaign["status"] = "active"
            campaign["awaiting_reason"] = ""
            campaign["updated_at"] = current.isoformat()
            self._write(body)
            return ProxyCampaignClaim(
                campaign_id=approval.campaign_id,
                approval_id=approval.approval_id,
                approval_sha256=approval.approval_sha256,
                allocation_id=allocation_id,
                phase=allocation_spec.phase,
                lease_id=lease_id,
                claim_token=token,
                allocation_budget_bytes=allocation_spec.budget_bytes,
                allocation_spent_provider_bytes=int(
                    allocation.get("spent_provider_bytes", 0)
                ),
                remaining_provider_bytes=min(remaining),
                total_remaining_provider_bytes=remaining[1],
                phase_remaining_provider_bytes=remaining[2],
                daily_remaining_provider_bytes=remaining[3],
            )
        finally:
            fcntl.flock(handle.fileno(), fcntl.LOCK_UN)
            handle.close()

    def record_request(
        self,
        approval: ProxyCampaignApproval,
        claim: ProxyCampaignClaim,
        *,
        now: datetime | None = None,
    ) -> int:
        """Charge one provider-bound CONNECT/request before the upstream dial."""

        current = _now(now)
        approval.verify(self._approval_secret, now=current)
        handle = self._locked()
        try:
            body = self._read()
            campaign, allocation, active = self._active(body, claim)
            if int(campaign.get("requests_used", 0)) >= approval.limits.requests:
                campaign["status"] = "awaiting_approval"
                campaign["awaiting_reason"] = "request_limit"
                campaign["updated_at"] = current.isoformat()
                self._write(body)
                raise ProxyCampaignBudgetExceeded("campaign request limit exhausted")
            request_limit = int(allocation.get("request_limit", 0))
            if int(allocation.get("requests_used", 0)) >= request_limit:
                raise ProxyCampaignBudgetExceeded("allocation request limit exhausted")
            campaign["requests_used"] = int(campaign.get("requests_used", 0)) + 1
            allocation["requests_used"] = int(allocation.get("requests_used", 0)) + 1
            active["requests_used"] = int(active.get("requests_used", 0)) + 1
            campaign["updated_at"] = current.isoformat()
            if int(campaign["requests_used"]) >= approval.limits.requests:
                campaign["status"] = "awaiting_approval"
                campaign["awaiting_reason"] = "request_limit"
            self._write(body)
            return int(active["requests_used"])
        finally:
            fcntl.flock(handle.fileno(), fcntl.LOCK_UN)
            handle.close()

    def _active(
        self,
        body: dict[str, Any],
        claim: ProxyCampaignClaim,
        *,
        allow_revoked: bool = False,
    ) -> tuple[dict[str, Any], dict[str, Any], dict[str, Any]]:
        campaign = body["campaigns"].get(claim.campaign_id)
        if not isinstance(campaign, dict):
            raise ProxyCampaignAccountingError("campaign claim is unknown")
        if campaign.get("status") == "revoked" and not allow_revoked:
            raise ProxyCampaignRevoked("proxy campaign is revoked")
        active = campaign.get("active_claims", {}).get(claim.lease_id)
        if not isinstance(active, dict) or not hmac.compare_digest(
            str(active.get("claim_token_hash")), _state_token_hash(claim.claim_token)
        ):
            raise ProxyCampaignAccountingError("campaign claim is stale or invalid")
        allocation = campaign.get("allocations", {}).get(claim.allocation_id)
        if not isinstance(allocation, dict):
            raise ProxyCampaignAccountingError("campaign allocation is missing")
        return campaign, allocation, active

    def _settlement_active(
        self,
        body: dict[str, Any],
        approval: ProxyCampaignApproval,
        claim: ProxyCampaignClaim,
    ) -> tuple[dict[str, Any], dict[str, Any], dict[str, Any]]:
        """Authenticate an already-issued claim without reopening authority."""

        if (
            claim.campaign_id != approval.campaign_id
            or claim.approval_id != approval.approval_id
            or not hmac.compare_digest(claim.approval_sha256, approval.approval_sha256)
        ):
            raise ProxyCampaignAccountingError(
                "settlement approval differs from the persisted claim"
            )
        campaign, allocation, active = self._active(
            body,
            claim,
            allow_revoked=True,
        )
        if active.get("approval_id") != approval.approval_id or not hmac.compare_digest(
            str(active.get("approval_sha256")), approval.approval_sha256
        ):
            raise ProxyCampaignAccountingError(
                "persisted claim has another approval authority"
            )
        started = _utc_timestamp(active.get("started_at"), "claim.started_at")
        claim_expires = _utc_timestamp(active.get("expires_at"), "claim.expires_at")
        issued = _utc_timestamp(approval.issued_at, "issued_at")
        authority_expires = _utc_timestamp(approval.expires_at, "expires_at")
        if (
            not issued <= started < authority_expires
            or claim_expires > authority_expires
        ):
            raise ProxyCampaignAccountingError(
                "persisted claim was not issued inside the signed validity window"
            )
        return campaign, allocation, active

    def reserve_provider_bytes(
        self,
        approval: ProxyCampaignApproval,
        claim: ProxyCampaignClaim,
        provider_bytes: int,
        *,
        provider_order_cap_bytes: int | None = None,
        global_daily_cap_bytes: int | None = None,
        now: datetime | None = None,
    ) -> int:
        """Durably escrow one complete lease before any provider socket I/O.

        The escrow is part of every signed byte ceiling.  The filtering proxy
        also supplies its code-owned order/day caps so the same ledger lock
        atomically prevents two processes or approvals from oversubscribing
        the provider order.  Normal accounting converts escrow to exact spend;
        a crash leaves the unknown remainder unavailable.
        """

        amount = _integer(provider_bytes, "provider_bytes")
        order_cap = (
            None
            if provider_order_cap_bytes is None
            else _integer(provider_order_cap_bytes, "provider_order_cap_bytes")
        )
        daily_cap = (
            None
            if global_daily_cap_bytes is None
            else _integer(global_daily_cap_bytes, "global_daily_cap_bytes")
        )
        current = _now(now)
        approval.verify(self._approval_secret, now=current)
        handle = self._locked()
        try:
            body = self._read()
            campaign, allocation, active = self._active(body, claim)
            if active.get("reservation_day") != current.date().isoformat():
                raise ProxyCampaignAccountingError(
                    "provider-byte escrow crossed a UTC day"
                )
            if int(active.get("reserved_provider_bytes", 0)) != 0:
                raise ProxyCampaignAccountingError(
                    "campaign claim already has provider-byte escrow"
                )
            remaining = self._remaining(
                approval,
                campaign,
                allocation,
                day=current.date().isoformat(),
            )
            if amount > min(remaining):
                campaign["status"] = "awaiting_approval"
                campaign["awaiting_reason"] = "byte_cap"
                campaign["updated_at"] = current.isoformat()
                self._write(body)
                raise ProxyCampaignBudgetExceeded(
                    "lease escrow would exceed a signed campaign byte cap"
                )
            order = self._provider_order_accounting_from_body(
                body,
                current_day=current.date().isoformat(),
            )
            if order_cap is not None and (
                int(order["exposure_provider_bytes"]) + amount > order_cap
            ):
                raise ProxyCampaignBudgetExceeded(
                    "lease escrow would exceed the provider-order lifetime cap"
                )
            if daily_cap is not None and (
                int(order["current_day_spent_provider_bytes"])
                + int(order["current_day_reserved_provider_bytes"])
                + amount
                > daily_cap
            ):
                raise ProxyCampaignBudgetExceeded(
                    "lease escrow would exceed the global daily cap"
                )
            active["reserved_provider_bytes"] = amount
            active["escrow_provider_bytes"] = amount
            campaign["updated_at"] = current.isoformat()
            self._write(body)
            return amount
        finally:
            fcntl.flock(handle.fileno(), fcntl.LOCK_UN)
            handle.close()

    def release_provider_reservation(
        self,
        approval: ProxyCampaignApproval,
        claim: ProxyCampaignClaim,
        *,
        now: datetime | None = None,
    ) -> int:
        """Return proven-unused escrow after every provider tunnel has drained."""

        current = _now(now)
        approval.verify_signature(self._approval_secret)
        handle = self._locked()
        try:
            body = self._read()
            campaign, _allocation, active = self._settlement_active(
                body, approval, claim
            )
            amount = active.get("reserved_provider_bytes", 0)
            if isinstance(amount, bool) or not isinstance(amount, int) or amount < 0:
                raise ProxyCampaignAccountingError(
                    "campaign provider-byte reservation is malformed"
                )
            active["reserved_provider_bytes"] = 0
            campaign["updated_at"] = current.isoformat()
            self._write(body)
            return amount
        finally:
            fcntl.flock(handle.fileno(), fcntl.LOCK_UN)
            handle.close()

    def remaining(
        self,
        approval: ProxyCampaignApproval,
        claim: ProxyCampaignClaim,
        *,
        now: datetime | None = None,
    ) -> int:
        current = _now(now)
        approval.verify(self._approval_secret, now=current)
        handle = self._locked()
        try:
            body = self._read()
            campaign, allocation, active = self._active(body, claim)
            reserved = int(active.get("reserved_provider_bytes", 0))
            if int(active.get("escrow_provider_bytes", 0)) > 0:
                return reserved
            return max(
                0,
                min(
                    self._remaining(
                        approval,
                        campaign,
                        allocation,
                        day=current.date().isoformat(),
                    )
                ),
            )
        finally:
            fcntl.flock(handle.fileno(), fcntl.LOCK_UN)
            handle.close()

    def remaining_limits(
        self,
        approval: ProxyCampaignApproval,
        claim: ProxyCampaignClaim,
        *,
        now: datetime | None = None,
    ) -> Mapping[str, int]:
        """Return each independent remaining cap for pre-read reservations."""

        current = _now(now)
        approval.verify(self._approval_secret, now=current)
        handle = self._locked()
        try:
            body = self._read()
            campaign, allocation, active = self._active(body, claim)
            reserved = int(active.get("reserved_provider_bytes", 0))
            if int(active.get("escrow_provider_bytes", 0)) > 0:
                return {
                    "allocation_provider_bytes": reserved,
                    "total_provider_bytes": reserved,
                    "phase_provider_bytes": reserved,
                    "daily_provider_bytes": reserved,
                }
            values = self._remaining(
                approval,
                campaign,
                allocation,
                day=current.date().isoformat(),
            )
            return {
                "allocation_provider_bytes": max(0, values[0]),
                "total_provider_bytes": max(0, values[1]),
                "phase_provider_bytes": max(0, values[2]),
                "daily_provider_bytes": max(0, values[3]),
            }
        finally:
            fcntl.flock(handle.fileno(), fcntl.LOCK_UN)
            handle.close()

    def consume(
        self,
        approval: ProxyCampaignApproval,
        claim: ProxyCampaignClaim,
        provider_billed_bytes: int,
        *,
        now: datetime | None = None,
    ) -> None:
        amount = _integer(
            provider_billed_bytes,
            "provider_billed_bytes",
            allow_zero=True,
        )
        if amount == 0:
            return
        current = _now(now)
        approval.verify_signature(self._approval_secret)
        handle = self._locked()
        try:
            body = self._read()
            campaign, allocation, active = self._settlement_active(
                body, approval, claim
            )
            if int(active.get("requests_used", 0)) <= 0:
                raise ProxyCampaignAccountingError(
                    "provider bytes have no pre-dial request charge"
                )
            reserved = int(active.get("reserved_provider_bytes", 0))
            escrowed = int(active.get("escrow_provider_bytes", 0)) > 0
            if not escrowed:
                # Legacy direct callers have no pre-I/O escrow.  They still
                # need live authority because this branch can consume fresh
                # campaign allowance.  Production proxy leases always use the
                # bounded escrow branch below.
                approval.verify(self._approval_secret, now=current)
                if campaign.get("status") == "revoked":
                    raise ProxyCampaignRevoked("proxy campaign is revoked")
            if escrowed:
                if amount > reserved:
                    campaign["status"] = "revoked"
                    campaign["revocation_reason"] = (
                        "provider bytes exceeded durable lease escrow"
                    )
                    campaign["updated_at"] = current.isoformat()
                    self._write(body)
                    raise ProxyCampaignBudgetExceeded(
                        "provider chunk would exceed durable lease escrow"
                    )
                active["reserved_provider_bytes"] = reserved - amount
            else:
                # Compatibility for callers which do not use the filtering
                # proxy. Production WhoScored leases always escrow first.
                remaining = self._remaining(
                    approval, campaign, allocation, day=current.date().isoformat()
                )
                if amount > min(remaining):
                    campaign["status"] = "awaiting_approval"
                    campaign["awaiting_reason"] = "byte_cap"
                    campaign["updated_at"] = current.isoformat()
                    self._write(body)
                    raise ProxyCampaignBudgetExceeded(
                        "provider chunk would exceed a signed campaign byte cap"
                    )
            allocation["spent_provider_bytes"] = (
                int(allocation.get("spent_provider_bytes", 0)) + amount
            )
            campaign["spent_provider_bytes"] = (
                int(campaign.get("spent_provider_bytes", 0)) + amount
            )
            phase_spend = campaign["phase_spend"]
            phase_spend[claim.phase] = int(phase_spend.get(claim.phase, 0)) + amount
            day = current.date().isoformat()
            daily_spend = campaign["daily_spend"]
            daily_spend[day] = int(daily_spend.get(day, 0)) + amount
            active["spent_provider_bytes"] = (
                int(active.get("spent_provider_bytes", 0)) + amount
            )
            campaign["updated_at"] = current.isoformat()
            after = self._remaining(approval, campaign, allocation, day=day)
            if min(after) == 0:
                campaign["status"] = "awaiting_approval"
                campaign["awaiting_reason"] = "byte_cap"
            self._write(body)
        finally:
            fcntl.flock(handle.fileno(), fcntl.LOCK_UN)
            handle.close()

    def finish(
        self,
        approval: ProxyCampaignApproval,
        claim: ProxyCampaignClaim,
        *,
        provider_billed_bytes: int,
        completed: bool,
        now: datetime | None = None,
    ) -> Mapping[str, object]:
        if not isinstance(completed, bool):
            raise ProxyCampaignAccountingError("completed must be boolean")
        reported = _integer(
            provider_billed_bytes,
            "provider_billed_bytes",
            allow_zero=True,
        )
        current = _now(now)
        approval.verify_signature(self._approval_secret)
        handle = self._locked()
        try:
            body = self._read()
            campaign, _allocation, active = self._settlement_active(
                body, approval, claim
            )
            if int(active.get("reserved_provider_bytes", 0)) != 0:
                raise ProxyCampaignAccountingError(
                    "lease cannot finish with unsettled provider-byte escrow"
                )
            charged = int(active.get("spent_provider_bytes", 0))
            if reported != charged:
                raise ProxyCampaignAccountingError(
                    "lease report does not equal exact provider billed bytes"
                )
            attempt = {
                "lease_id_hash": _state_token_hash(claim.lease_id),
                "attempt_id_hash": active.get("attempt_id_hash"),
                "canonical_url_sha256": active.get("canonical_url_sha256"),
                "provider_billed_bytes": charged,
                "provider_requests": int(active.get("requests_used", 0)),
                "completed": completed,
                "expired": current
                >= _utc_timestamp(active.get("expires_at"), "claim.expires_at"),
                "finished_at": current.isoformat(),
            }
            self._append_attempt_record(
                body,
                campaign,
                allocation_id=claim.allocation_id,
                attempt=attempt,
                retain_active_claim=False,
            )
            return json.loads(json.dumps(attempt, sort_keys=True))
        finally:
            fcntl.flock(handle.fileno(), fcntl.LOCK_UN)
            handle.close()

    def complete_allocation(
        self,
        approval: ProxyCampaignApproval,
        allocation_id: str,
        *,
        dag_id: str,
        run_id: str,
        task_id: str,
        attempt_id: str,
        report_sha256: str,
        request_ledger_sha256: str,
        now: datetime | None = None,
    ) -> Mapping[str, object]:
        """Durably close one successful work allocation for its sole DagRun."""

        current = _now(now)
        approval.verify(self._approval_secret, now=current)
        normalized_allocation_id = _token(allocation_id, "allocation_id")
        normalized_dag_id = _token(dag_id, "dag_id")
        normalized_run_id = _identity(run_id, "run_id")
        normalized_task_id = _token(task_id, "task_id")
        normalized_attempt_id = _token(attempt_id, "attempt_id")
        normalized_report_sha256 = _digest(report_sha256, "report_sha256")
        normalized_request_sha256 = _digest(
            request_ledger_sha256, "request_ledger_sha256"
        )
        allocation_spec = approval.allocation(normalized_allocation_id)
        if normalized_dag_id != approval.allowed_dag_ids[0]:
            raise ProxyCampaignValidationError(
                "allocation completion DAG differs from the signed approval"
            )
        if normalized_task_id != allocation_spec.task_id:
            raise ProxyCampaignValidationError(
                "allocation completion task_id differs from the signed allocation"
            )
        if approval.is_exact_canary and normalized_run_id != whoscored_canary_run_id(
            approval.campaign_id
        ):
            raise ProxyCampaignValidationError(
                "exact canary approval belongs to another Airflow run_id"
            )
        run_id_hash = _state_token_hash(normalized_run_id)
        dag_id_hash = _state_token_hash(normalized_dag_id)
        attempt_id_hash = _state_token_hash(normalized_attempt_id)
        handle = self._locked()
        try:
            body = self._read()
            campaign = self._register(body, approval)
            # As in ``claim``, an orphaned reservation must be durably recorded
            # before the revoked status is surfaced to the caller.
            self._reap_expired(body, campaign, current)
            if campaign.get("status") == "revoked":
                raise ProxyCampaignRevoked("proxy campaign is revoked")
            persisted_run_id_hash = campaign.get("run_id_hash", "")
            if (
                persisted_run_id_hash != run_id_hash
                or campaign.get("dag_id_hash") != dag_id_hash
            ):
                raise ProxyCampaignValidationError(
                    "proxy campaign is bound to another Airflow DAG/run"
                )
            allocations = campaign.get("allocations")
            claims = campaign.get("active_claims")
            if not isinstance(allocations, dict) or not isinstance(claims, dict):
                raise ProxyCampaignAccountingError("campaign state is malformed")
            allocation = allocations.get(normalized_allocation_id)
            if not isinstance(allocation, dict):
                raise ProxyCampaignAccountingError("signed allocation is not persisted")
            if allocation.get("completed") is True:
                if (
                    allocation.get("completed_run_id_hash") != run_id_hash
                    or allocation.get("completed_dag_id_hash") != dag_id_hash
                    or allocation.get("completed_task_id") != normalized_task_id
                    or allocation.get("completed_attempt_id_hash") != attempt_id_hash
                    or allocation.get("report_sha256") != normalized_report_sha256
                    or allocation.get("request_ledger_sha256")
                    != normalized_request_sha256
                ):
                    raise ProxyCampaignAccountingError(
                        "allocation was completed by another execution identity"
                    )
                return json.loads(json.dumps(allocation, sort_keys=True))
            if any(
                isinstance(claim, Mapping)
                and claim.get("allocation_id") == normalized_allocation_id
                for claim in claims.values()
            ):
                raise ProxyCampaignAccountingError(
                    "allocation cannot complete with active provider-byte claims"
                )
            allocation["completed"] = True
            allocation["completed_run_id_hash"] = run_id_hash
            allocation["completed_dag_id_hash"] = dag_id_hash
            allocation["completed_task_id"] = normalized_task_id
            allocation["completed_attempt_id_hash"] = attempt_id_hash
            allocation["report_sha256"] = normalized_report_sha256
            allocation["request_ledger_sha256"] = normalized_request_sha256
            allocation["completed_at"] = current.isoformat()
            if all(item.get("completed") is True for item in allocations.values()):
                campaign["status"] = "complete"
                campaign["awaiting_reason"] = ""
            campaign["updated_at"] = current.isoformat()
            self._write(body)
            return json.loads(json.dumps(allocation, sort_keys=True))
        finally:
            fcntl.flock(handle.fileno(), fcntl.LOCK_UN)
            handle.close()

    @staticmethod
    def _counter(value: object, field: str) -> int:
        if isinstance(value, bool) or not isinstance(value, int) or value < 0:
            raise ProxyCampaignAccountingError(
                f"persisted campaign {field} is malformed"
            )
        return value

    def _validate_journal_accounting(
        self,
        campaign: Mapping[str, Any],
        records: Sequence[Mapping[str, Any]],
    ) -> None:
        allocations = campaign.get("allocations")
        claims = campaign.get("active_claims")
        if not isinstance(allocations, Mapping) or not isinstance(claims, Mapping):
            raise ProxyCampaignAccountingError(
                "persisted campaign accounting state is malformed"
            )
        totals = {"leases": 0, "requests": 0, "bytes": 0}
        allocation_totals: dict[str, dict[str, int]] = {
            str(allocation_id): {"leases": 0, "requests": 0, "bytes": 0}
            for allocation_id in allocations
        }
        retained_sequences: set[int] = set()
        records_by_sequence: dict[int, Mapping[str, Any]] = {}
        for record in records:
            allocation_id = str(record["allocation_id"])
            attempt = record["attempt"]
            values = allocation_totals.get(allocation_id)
            if values is None or not isinstance(attempt, Mapping):
                raise ProxyCampaignAccountingError(
                    "campaign attempt journal names an unknown allocation"
                )
            billed = self._counter(
                attempt.get("provider_billed_bytes"), "attempt bytes"
            )
            requests = self._counter(
                attempt.get("provider_requests"), "attempt requests"
            )
            totals["leases"] += 1
            totals["requests"] += requests
            totals["bytes"] += billed
            values["leases"] += 1
            values["requests"] += requests
            values["bytes"] += billed
            sequence = int(record["sequence"])
            records_by_sequence[sequence] = record
            if record["retain_active_claim"]:
                retained_sequences.add(sequence)

        matched_retained: set[int] = set()
        for lease_id, claim in claims.items():
            if not isinstance(lease_id, str) or not isinstance(claim, Mapping):
                raise ProxyCampaignAccountingError(
                    "persisted campaign active claim is malformed"
                )
            allocation_id = str(claim.get("allocation_id"))
            values = allocation_totals.get(allocation_id)
            if values is None:
                raise ProxyCampaignAccountingError(
                    "persisted campaign claim allocation is unknown"
                )
            terminal_sequence = claim.get("terminal_attempt_sequence")
            if terminal_sequence is not None:
                if (
                    isinstance(terminal_sequence, bool)
                    or not isinstance(terminal_sequence, int)
                    or terminal_sequence <= 0
                ):
                    raise ProxyCampaignAccountingError(
                        "persisted terminal claim marker is malformed"
                    )
                record = records_by_sequence.get(terminal_sequence)
                if (
                    record is None
                    or record.get("retain_active_claim") is not True
                    or record.get("allocation_id") != allocation_id
                    or record.get("record_sha256")
                    != claim.get("terminal_attempt_sha256")
                    or record["attempt"].get("lease_id_hash")
                    != _state_token_hash(lease_id)
                ):
                    raise ProxyCampaignAccountingError(
                        "persisted terminal claim marker differs from journal"
                    )
                matched_retained.add(terminal_sequence)
                continue
            billed = self._counter(
                claim.get("spent_provider_bytes"), "active claim bytes"
            )
            requests = self._counter(
                claim.get("requests_used"), "active claim requests"
            )
            totals["leases"] += 1
            totals["requests"] += requests
            totals["bytes"] += billed
            values["leases"] += 1
            values["requests"] += requests
            values["bytes"] += billed
        if matched_retained != retained_sequences:
            raise ProxyCampaignAccountingError(
                "campaign attempt journal retained claim evidence is incomplete"
            )

        expected_campaign = {
            "leases": self._counter(campaign.get("leases_used"), "leases_used"),
            "requests": self._counter(campaign.get("requests_used"), "requests_used"),
            "bytes": self._counter(
                campaign.get("spent_provider_bytes"), "spent_provider_bytes"
            ),
        }
        if totals != expected_campaign:
            raise ProxyCampaignAccountingError(
                "campaign journal attempts differ from exact campaign counters"
            )
        for allocation_id, allocation in allocations.items():
            if not isinstance(allocation, Mapping):
                raise ProxyCampaignAccountingError(
                    "persisted campaign allocation is malformed"
                )
            expected_allocation = {
                "leases": self._counter(
                    allocation.get("leases_used"), "allocation leases_used"
                ),
                "requests": self._counter(
                    allocation.get("requests_used"), "allocation requests_used"
                ),
                "bytes": self._counter(
                    allocation.get("spent_provider_bytes"),
                    "allocation spent_provider_bytes",
                ),
            }
            if allocation_totals[str(allocation_id)] != expected_allocation:
                raise ProxyCampaignAccountingError(
                    "campaign journal attempts differ from allocation counters"
                )

    def _materialize_campaign(self, campaign: Mapping[str, Any]) -> dict[str, Any]:
        materialized = json.loads(json.dumps(campaign, sort_keys=True))
        state = self._journal_state(campaign)
        if state is None:
            # Compatibility is intentionally limited to immutable v1 evidence.
            # The enclosing ledger HMAC protects every inline attempt.
            return materialized
        records = self._read_full_journal(campaign)
        self._validate_journal_accounting(campaign, records)
        allocations = materialized.get("allocations")
        if not isinstance(allocations, dict):
            raise ProxyCampaignAccountingError(
                "persisted campaign allocations are malformed"
            )
        for allocation in allocations.values():
            allocation["attempts"] = []
        for record in records:
            allocations[str(record["allocation_id"])]["attempts"].append(
                json.loads(json.dumps(record["attempt"], sort_keys=True))
            )
        return materialized

    def snapshot(
        self,
        approval: ProxyCampaignApproval,
        *,
        now: datetime | None = None,
    ) -> Mapping[str, object]:
        current = _now(now)
        approval.verify(self._approval_secret, now=current)
        handle = self._locked()
        try:
            body = self._read()
            campaign = self._register(body, approval)
            self._reap_expired(body, campaign, current)
            campaign["updated_at"] = current.isoformat()
            self._write(body)
            return self._materialize_campaign(campaign)
        finally:
            fcntl.flock(handle.fileno(), fcntl.LOCK_UN)
            handle.close()

    @staticmethod
    def _attempt_accounting_sha256(campaign: Mapping[str, Any]) -> str:
        allocations = campaign.get("allocations")
        if not isinstance(allocations, Mapping):
            raise ProxyCampaignAccountingError("persisted allocations are malformed")
        values: list[dict[str, object]] = []
        for allocation_id, allocation in sorted(allocations.items()):
            attempts = (
                allocation.get("attempts") if isinstance(allocation, Mapping) else None
            )
            if not isinstance(allocation_id, str) or not isinstance(attempts, list):
                raise ProxyCampaignAccountingError(
                    "persisted campaign attempts are malformed"
                )
            for attempt in attempts:
                if not isinstance(attempt, Mapping):
                    raise ProxyCampaignAccountingError(
                        "persisted campaign attempt is malformed"
                    )
                values.append(
                    {
                        "allocation_id": allocation_id,
                        "attempt_id_hash": attempt.get("attempt_id_hash"),
                        "lease_id_hash": attempt.get("lease_id_hash"),
                        "canonical_url_sha256": attempt.get("canonical_url_sha256"),
                        "provider_billed_bytes": attempt.get("provider_billed_bytes"),
                    }
                )
        values.sort(
            key=lambda item: (
                str(item["allocation_id"]),
                str(item["attempt_id_hash"]),
                str(item["lease_id_hash"]),
            )
        )
        return hashlib.sha256(canonical_json_bytes(values)).hexdigest()

    def seal_for_reconciliation(
        self,
        approval: ProxyCampaignApproval,
        *,
        dag_id: str,
        run_id: str,
        provider_billed_bytes: int,
        attempt_accounting_sha256: str,
    ) -> Mapping[str, object]:
        """Atomically prohibit future claims and bind terminal DQ accounting."""

        approval.verify_signature(self._approval_secret)
        normalized_dag_id = _token(dag_id, "dag_id")
        normalized_run_id = _identity(run_id, "run_id")
        expected_bytes = _integer(
            provider_billed_bytes,
            "provider_billed_bytes",
            allow_zero=True,
        )
        expected_attempts = _digest(
            attempt_accounting_sha256, "attempt_accounting_sha256"
        )
        handle = self._locked()
        try:
            body = self._read()
            campaign = self._register(body, approval, allow_sealed=True)
            journal_state = self._journal_state(campaign)
            if journal_state is None:
                raise ProxyCampaignAccountingError(
                    "only journal-backed campaigns can create a new seal"
                )
            seal = {
                "schema_version": PROXY_CAMPAIGN_RECONCILIATION_SEAL_SCHEMA_VERSION,
                "approval_sha256": approval.approval_sha256,
                "dag_id_hash": _state_token_hash(normalized_dag_id),
                "run_id_hash": _state_token_hash(normalized_run_id),
                "provider_billed_bytes": expected_bytes,
                "attempt_accounting_sha256": expected_attempts,
                "attempt_journal_count": journal_state["count"],
                "attempt_journal_offset": journal_state["offset"],
                "attempt_journal_tail_sha256": journal_state["tail_sha256"],
            }
            if (
                campaign.get("dag_id_hash") != seal["dag_id_hash"]
                or campaign.get("run_id_hash") != seal["run_id_hash"]
            ):
                raise ProxyCampaignAccountingError(
                    "proxy campaign seal belongs to another Airflow DagRun"
                )
            if campaign.get("status") == "revoked":
                raise ProxyCampaignRevoked("proxy campaign is revoked")
            claims = campaign.get("active_claims")
            if not isinstance(claims, Mapping) or claims:
                raise ProxyCampaignAccountingError(
                    "proxy campaign cannot seal with active claims"
                )
            materialized = self._materialize_campaign(campaign)
            if (
                campaign.get("spent_provider_bytes") != expected_bytes
                or self._attempt_accounting_sha256(materialized) != expected_attempts
            ):
                raise ProxyCampaignAccountingError(
                    "proxy campaign seal accounting differs"
                )
            persisted = campaign.get("reconciliation_seal")
            if persisted is not None and persisted != seal:
                raise ProxyCampaignAccountingError(
                    "proxy campaign has another reconciliation seal"
                )
            if campaign.get("status") == "sealed":
                # Idempotent reconciliation retries may reread an identical
                # seal, but terminal evidence never gets another write.
                return materialized
            campaign["reconciliation_seal"] = seal
            campaign["status"] = "sealed"
            campaign["awaiting_reason"] = ""
            campaign["updated_at"] = datetime.now(timezone.utc).isoformat()
            self._write(body)
            return self._materialize_campaign(campaign)
        finally:
            fcntl.flock(handle.fileno(), fcntl.LOCK_UN)
            handle.close()

    def sealed_snapshot(self, approval: ProxyCampaignApproval) -> Mapping[str, object]:
        """Read a terminal campaign after approval expiry without minting authority."""

        approval.verify_signature(self._approval_secret)
        handle = self._locked()
        try:
            body = self._read()
            campaign = body["campaigns"].get(approval.campaign_id)
            if (
                not isinstance(campaign, dict)
                or campaign.get("approval_sha256") != approval.approval_sha256
                or campaign.get("status") != "sealed"
                or not isinstance(campaign.get("reconciliation_seal"), Mapping)
            ):
                raise ProxyCampaignAccountingError(
                    "proxy campaign has no matching terminal reconciliation seal"
                )
            materialized = self._materialize_campaign(campaign)
            seal = campaign["reconciliation_seal"]
            schema_version = seal.get("schema_version")
            if schema_version == 1:
                if self._journal_state(campaign) is not None or frozenset(seal) != {
                    "schema_version",
                    "approval_sha256",
                    "dag_id_hash",
                    "run_id_hash",
                    "provider_billed_bytes",
                    "attempt_accounting_sha256",
                }:
                    raise ProxyCampaignAccountingError(
                        "legacy reconciliation seal is malformed"
                    )
            elif schema_version == PROXY_CAMPAIGN_RECONCILIATION_SEAL_SCHEMA_VERSION:
                state = self._journal_state(campaign)
                if (
                    state is None
                    or frozenset(seal)
                    != {
                        "schema_version",
                        "approval_sha256",
                        "dag_id_hash",
                        "run_id_hash",
                        "provider_billed_bytes",
                        "attempt_accounting_sha256",
                        "attempt_journal_count",
                        "attempt_journal_offset",
                        "attempt_journal_tail_sha256",
                    }
                    or (
                        seal.get("attempt_journal_count") != state["count"]
                        or seal.get("attempt_journal_offset") != state["offset"]
                        or seal.get("attempt_journal_tail_sha256")
                        != state["tail_sha256"]
                    )
                ):
                    raise ProxyCampaignAccountingError(
                        "reconciliation seal differs from attempt journal"
                    )
            else:
                raise ProxyCampaignAccountingError("unsupported reconciliation seal")
            try:
                seal_approval = _digest(seal.get("approval_sha256"), "approval_sha256")
                seal_bytes = _integer(
                    seal.get("provider_billed_bytes"),
                    "provider_billed_bytes",
                    allow_zero=True,
                )
                seal_attempts = _digest(
                    seal.get("attempt_accounting_sha256"),
                    "attempt_accounting_sha256",
                )
            except ProxyCampaignValidationError as exc:
                raise ProxyCampaignAccountingError(
                    "reconciliation seal accounting is malformed"
                ) from exc
            if (
                seal_approval != approval.approval_sha256
                or seal_bytes != campaign.get("spent_provider_bytes")
                or seal_attempts != self._attempt_accounting_sha256(materialized)
            ):
                raise ProxyCampaignAccountingError(
                    "reconciliation seal exact accounting differs"
                )
            return materialized
        finally:
            fcntl.flock(handle.fileno(), fcntl.LOCK_UN)
            handle.close()

    def revoke(self, campaign_id: str, *, reason: str) -> None:
        campaign_id = _token(campaign_id, "campaign_id")
        if not isinstance(reason, str) or not reason.strip() or len(reason) > 500:
            raise ProxyCampaignValidationError(
                "revocation reason must be non-empty and bounded"
            )
        handle = self._locked()
        try:
            body = self._read()
            campaign = body["campaigns"].get(campaign_id)
            if not isinstance(campaign, dict):
                raise ProxyCampaignAccountingError("campaign is unknown")
            if campaign.get("status") == "sealed":
                raise ProxyCampaignAccountingError(
                    "sealed proxy campaign evidence is terminal and immutable"
                )
            campaign["status"] = "revoked"
            campaign["revocation_reason"] = reason.strip()
            # Preserve every live claim and its escrow as authenticated
            # forensic evidence. Revocation blocks new work; it must never
            # erase bytes that may already have reached the provider.
            campaign["updated_at"] = datetime.now(timezone.utc).isoformat()
            self._write(body)
        finally:
            fcntl.flock(handle.fileno(), fcntl.LOCK_UN)
            handle.close()

    def assert_exact_accounting(
        self,
        approval: ProxyCampaignApproval,
        *,
        task_report_provider_bytes: int,
        request_ledger_provider_bytes: int,
        proxy_ledger_provider_bytes: int,
        require_complete: bool = False,
        now: datetime | None = None,
    ) -> int:
        """Require equality across all four ledgers; never hide drift with max()."""

        counters = (
            _integer(
                task_report_provider_bytes,
                "task_report_provider_bytes",
                allow_zero=True,
            ),
            _integer(
                request_ledger_provider_bytes,
                "request_ledger_provider_bytes",
                allow_zero=True,
            ),
            _integer(
                proxy_ledger_provider_bytes,
                "proxy_ledger_provider_bytes",
                allow_zero=True,
            ),
        )
        snapshot = self.snapshot(approval, now=now)
        active_claims = snapshot.get("active_claims")
        if not isinstance(active_claims, Mapping) or active_claims:
            raise ProxyCampaignAccountingError(
                "campaign has unsettled provider-byte claims"
            )
        if snapshot.get("status") == "revoked":
            raise ProxyCampaignAccountingError(
                "revoked campaign cannot produce exact accounting evidence"
            )
        if require_complete and snapshot.get("status") != "complete":
            raise ProxyCampaignAccountingError(
                "campaign allocations are not durably complete"
            )
        campaign_bytes = int(snapshot.get("spent_provider_bytes", -1))
        if len(set((*counters, campaign_bytes))) != 1:
            raise ProxyCampaignAccountingError(
                "task/request/proxy/campaign provider billed bytes differ"
            )
        return campaign_bytes


def _approval_from_context(
    metadata: Mapping[str, object],
    *,
    secret: str | bytes | None,
    now: datetime | None = None,
) -> tuple[ProxyCampaignApproval, ProxyWorkAllocation, str]:
    """Validate mirrored metadata, optionally authenticating its HMAC."""

    approval = ProxyCampaignApproval.from_dict(metadata.get("proxy_campaign_approval"))
    if secret is None:
        approval.verify_digest()
        approval.verify_validity(now)
    else:
        approval.verify(secret, now=now)
    campaign_id = _token(metadata.get("proxy_campaign_id"), "proxy_campaign_id")
    approval_id = _token(metadata.get("proxy_approval_id"), "proxy_approval_id")
    approval_sha256 = _digest(
        metadata.get("proxy_approval_sha256"), "proxy_approval_sha256"
    )
    allocation_id = _token(metadata.get("proxy_allocation_id"), "proxy_allocation_id")
    attempt_id = _token(metadata.get("proxy_attempt_id"), "proxy_attempt_id")
    allocation = approval.allocation(allocation_id)
    dag_id = _token(metadata.get("dag_id"), "dag_id")
    run_id = _identity(metadata.get("run_id"), "run_id")
    task_id = _token(metadata.get("task_id"), "task_id")
    map_index = metadata.get("map_index")
    try_number = metadata.get("try_number")
    if (
        isinstance(map_index, bool)
        or not isinstance(map_index, int)
        or map_index < -1
        or isinstance(try_number, bool)
        or not isinstance(try_number, int)
        or try_number < 1
    ):
        raise ProxyCampaignValidationError(
            "map_index/try_number must identify one Airflow task attempt"
        )
    expected_attempt_id = deterministic_proxy_attempt_id(
        dag_id=dag_id,
        run_id=run_id,
        task_id=task_id,
        map_index=map_index,
        try_number=try_number,
    )
    if (
        metadata.get("transport_policy") != approval.transport_policy
        or run_id != approval.run_id
        or dag_id != approval.allowed_dag_ids[0]
        or task_id != allocation.task_id
        or attempt_id != expected_attempt_id
        or campaign_id != approval.campaign_id
        or approval_id != approval.approval_id
        or approval_sha256 != approval.approval_sha256
        or metadata.get("proxy_allocation") != allocation.to_dict()
        or metadata.get("proxy_work_item_id") != allocation.work_item_id
    ):
        raise ProxyCampaignValidationError(
            "mirrored campaign approval/allocation metadata does not match"
        )
    return approval, allocation, attempt_id


def approval_from_context(
    metadata: Mapping[str, object],
    *,
    secret: str | bytes,
    now: datetime | None = None,
) -> tuple[ProxyCampaignApproval, ProxyWorkAllocation, str]:
    """Authoritatively verify mirrored lease metadata for gateway/filter use."""

    return _approval_from_context(metadata, secret=secret, now=now)


def approval_structure_from_context(
    metadata: Mapping[str, object],
    *,
    now: datetime | None = None,
) -> tuple[ProxyCampaignApproval, ProxyWorkAllocation, str]:
    """Validate runner metadata without possessing approval-signing authority."""

    return _approval_from_context(metadata, secret=None, now=now)


def proxy_campaign_authority_context(
    approval: ProxyCampaignApproval,
) -> dict[str, object]:
    """Build the allocation-free context used by control and alert RPCs."""

    return {
        "dag_id": approval.allowed_dag_ids[0],
        "run_id": approval.run_id,
        "transport_policy": approval.transport_policy,
        "proxy_campaign_approval": approval.to_dict(),
        "proxy_campaign_id": approval.campaign_id,
        "proxy_approval_id": approval.approval_id,
        "proxy_approval_sha256": approval.approval_sha256,
    }


def approval_from_campaign_authority_context(
    metadata: Mapping[str, object],
    *,
    secret: str | bytes | None,
    require_active: bool = True,
    now: datetime | None = None,
) -> ProxyCampaignApproval:
    """Verify one exact signed campaign context without an allocation identity.

    Passing ``secret=None`` performs only public digest/time validation and is
    safe for a scheduler.  Gateway/filter callers pass their approval HMAC.
    ``require_active=False`` is reserved for terminal reconciliation reads and
    seals after an approval has expired; the HMAC and every identity pin still
    remain mandatory there.
    """

    if not isinstance(metadata, Mapping):
        raise ProxyCampaignValidationError(
            "campaign authority context must be an object"
        )
    _strict_fields(
        metadata,
        PROXY_CAMPAIGN_AUTHORITY_CONTEXT_FIELDS,
        "campaign authority context",
    )
    approval = ProxyCampaignApproval.from_dict(metadata.get("proxy_campaign_approval"))
    if secret is None:
        approval.verify_digest()
        if require_active:
            approval.verify_validity(now)
    else:
        approval.verify_signature(secret)
        if require_active:
            approval.verify_validity(now)
    dag_id = _token(metadata.get("dag_id"), "dag_id")
    run_id = _identity(metadata.get("run_id"), "run_id")
    if (
        dag_id != approval.allowed_dag_ids[0]
        or run_id != approval.run_id
        or metadata.get("transport_policy") != approval.transport_policy
        or metadata.get("proxy_campaign_id") != approval.campaign_id
        or metadata.get("proxy_approval_id") != approval.approval_id
        or metadata.get("proxy_approval_sha256") != approval.approval_sha256
    ):
        raise ProxyCampaignValidationError(
            "campaign authority metadata does not match the signed approval"
        )
    return approval


def _approval_secret_from_environment(environ: Mapping[str, str]) -> str:
    return str(environ.get(PROXY_APPROVAL_HMAC_SECRET_ENV, "")).strip()


def _ledger_secret_from_environment(environ: Mapping[str, str]) -> str:
    return str(environ.get(PROXY_LEDGER_HMAC_SECRET_ENV, "")).strip()


def _assert_paid_release_gates(
    approval: ProxyCampaignApproval,
) -> None:
    """Enforce immutable release sentinels after structural/auth checks."""

    if not WHOSCORED_PROVIDER_INVOICE_HARD_CAP_AVAILABLE:
        raise ProxyCampaignValidationError(
            "WhoScored paid traffic has no provider-side invoice hard cap"
        )
    if not WHOSCORED_PAID_APPLICATION_GATEWAY_AVAILABLE:
        raise ProxyCampaignValidationError(
            "WhoScored paid traffic has no authenticated isolated application gateway"
        )
    if approval.allowed_dag_ids == (WHOSCORED_CANARY_DAG_ID,):
        if not approval.is_exact_canary:
            raise ProxyCampaignValidationError(
                "WhoScored canary approval must match the exact 1 GB contract"
            )
    elif not WHOSCORED_FULL_PAID_CRAWL_AVAILABLE:
        raise ProxyCampaignValidationError(
            "WhoScored full paid crawl is disabled pending exact reconciliation"
        )


def assert_paid_runtime_available(
    metadata: Mapping[str, object],
    *,
    secret: str | bytes | None = None,
    environ: Mapping[str, str] | None = None,
    now: datetime | None = None,
) -> tuple[ProxyCampaignApproval, ProxyWorkAllocation, str]:
    """Validate a runner attempt and release sentinels without an authority key.

    ``secret`` remains an explicit compatibility/test seam: when supplied it
    also authenticates the HMAC.  The default path deliberately reads no
    approval, ledger or control secret; the isolated gateway repeats this check
    authoritatively before any network side effect.
    """

    del environ  # structural runner validation must never consult environment keys
    if secret is None:
        approval, allocation, attempt_id = approval_structure_from_context(
            metadata, now=now
        )
    else:
        approval, allocation, attempt_id = approval_from_context(
            metadata, secret=secret, now=now
        )
    _assert_paid_release_gates(approval)
    return approval, allocation, attempt_id


def assert_paid_runtime_authority_available(
    metadata: Mapping[str, object],
    *,
    secret: str | bytes | None = None,
    environ: Mapping[str, str] | None = None,
    now: datetime | None = None,
) -> tuple[ProxyCampaignApproval, ProxyWorkAllocation, str]:
    """Authenticate one paid attempt at the isolated gateway authority."""

    source = os.environ if environ is None else environ
    resolved = (
        secret if secret is not None else _approval_secret_from_environment(source)
    )
    approval, allocation, attempt_id = approval_from_context(
        metadata, secret=resolved, now=now
    )
    _assert_paid_release_gates(approval)
    return approval, allocation, attempt_id


def assert_paid_campaign_authority_available(
    metadata: Mapping[str, object],
    *,
    require_active: bool = True,
    enforce_release_gates: bool = False,
    secret: str | bytes | None = None,
    environ: Mapping[str, str] | None = None,
    now: datetime | None = None,
) -> ProxyCampaignApproval:
    """Authenticate allocation-free gateway alert/control authority."""

    source = os.environ if environ is None else environ
    resolved = (
        secret if secret is not None else _approval_secret_from_environment(source)
    )
    approval = approval_from_campaign_authority_context(
        metadata,
        secret=resolved,
        require_active=require_active,
        now=now,
    )
    if enforce_release_gates:
        _assert_paid_release_gates(approval)
    return approval


def load_proxy_campaign_approval(
    path: os.PathLike[str] | str,
    *,
    secret: str | bytes,
    expected_approval_id: str,
    expected_approval_sha256: str,
    now: datetime | None = None,
) -> ProxyCampaignApproval:
    """Read one bounded immutable approval and pin it to DagRun trigger values."""

    approval_path = Path(path)
    try:
        size = approval_path.stat().st_size
        if size <= 0 or size > MAX_APPROVAL_DOCUMENT_BYTES:
            raise ProxyCampaignValidationError(
                "campaign approval document has an invalid size"
            )
        raw = approval_path.read_bytes()
        value = strict_json_loads(raw.decode("utf-8"))
    except ProxyCampaignValidationError:
        raise
    except (OSError, UnicodeDecodeError, json.JSONDecodeError) as exc:
        raise ProxyCampaignValidationError(
            "campaign approval document is unreadable"
        ) from exc
    approval = ProxyCampaignApproval.from_dict(value)
    approval.verify(secret, now=now)
    if approval.approval_id != _token(
        expected_approval_id, "expected_approval_id"
    ) or approval.approval_sha256 != _digest(
        expected_approval_sha256, "expected_approval_sha256"
    ):
        raise ProxyCampaignSignatureError(
            "campaign approval does not match the pinned ID/SHA-256"
        )
    return approval


def load_proxy_campaign_approval_structure(
    path: os.PathLike[str] | str,
    *,
    expected_approval_id: str,
    expected_approval_sha256: str,
    now: datetime | None = None,
) -> ProxyCampaignApproval:
    """Load a pinned approval without possessing its HMAC authority key."""

    approval_path = Path(path)
    try:
        size = approval_path.stat().st_size
        if size <= 0 or size > MAX_APPROVAL_DOCUMENT_BYTES:
            raise ProxyCampaignValidationError(
                "campaign approval document has an invalid size"
            )
        approval = ProxyCampaignApproval.from_dict(
            strict_json_loads(approval_path.read_bytes().decode("utf-8"))
        )
    except ProxyCampaignValidationError:
        raise
    except (OSError, UnicodeDecodeError, json.JSONDecodeError) as exc:
        raise ProxyCampaignValidationError(
            "campaign approval document is unreadable"
        ) from exc
    approval.verify_digest()
    approval.verify_validity(now)
    if approval.approval_id != _token(
        expected_approval_id, "expected_approval_id"
    ) or approval.approval_sha256 != _digest(
        expected_approval_sha256, "expected_approval_sha256"
    ):
        raise ProxyCampaignSignatureError(
            "campaign approval does not match the pinned ID/SHA-256"
        )
    return approval


def load_proxy_campaign_approval_from_env(
    path: os.PathLike[str] | str,
    *,
    expected_approval_id: str,
    expected_approval_sha256: str,
    environ: Mapping[str, str] | None = None,
    now: datetime | None = None,
) -> ProxyCampaignApproval:
    """Load a pinned approval using the dedicated WhoScored runtime secret."""

    source = os.environ if environ is None else environ
    return load_proxy_campaign_approval(
        path,
        secret=_approval_secret_from_environment(source),
        expected_approval_id=expected_approval_id,
        expected_approval_sha256=expected_approval_sha256,
        now=now,
    )


def proxy_campaign_ledger_from_env(
    path: os.PathLike[str] | str,
    *,
    environ: Mapping[str, str] | None = None,
) -> ProxyCampaignLedger:
    """Open the authenticated campaign ledger with the deployment key.

    Airflow reconciliation uses this factory instead of duplicating dedicated
    secret lookup.  ``ProxyCampaignLedger`` performs
    the minimum-key-length check and authenticates the complete persisted
    document before exposing a snapshot.
    """

    source = os.environ if environ is None else environ
    return ProxyCampaignLedger(
        path,
        secret=_ledger_secret_from_environment(source),
        approval_secret=_approval_secret_from_environment(source),
    )


def deterministic_proxy_attempt_id(
    *,
    dag_id: str,
    run_id: str,
    task_id: str,
    map_index: int,
    try_number: int,
) -> str:
    """Derive a non-secret stable claim identity from one Airflow task attempt."""

    if isinstance(map_index, bool) or not isinstance(map_index, int) or map_index < -1:
        raise ProxyCampaignValidationError("map_index must be an integer >= -1")
    if (
        isinstance(try_number, bool)
        or not isinstance(try_number, int)
        or try_number < 1
    ):
        raise ProxyCampaignValidationError("try_number must be an integer >= 1")
    identity = {
        "dag_id": _token(dag_id, "dag_id"),
        "run_id": _identity(run_id, "run_id"),
        "task_id": _token(task_id, "task_id"),
        "map_index": map_index,
        "try_number": try_number,
    }
    return "attempt-" + hashlib.sha256(canonical_json_bytes(identity)).hexdigest()


def _context_payload(
    approval: ProxyCampaignApproval,
    allocation: ProxyWorkAllocation,
    attempt_id: str,
) -> dict[str, object]:
    return {
        "transport_policy": approval.transport_policy,
        "proxy_campaign_approval": approval.to_dict(),
        "proxy_campaign_id": approval.campaign_id,
        "proxy_approval_id": approval.approval_id,
        "proxy_approval_sha256": approval.approval_sha256,
        "proxy_allocation": allocation.to_dict(),
        "proxy_allocation_id": allocation.allocation_id,
        "proxy_work_item_id": allocation.work_item_id,
        "proxy_attempt_id": _token(attempt_id, "proxy_attempt_id"),
    }


def load_proxy_campaign_context(
    path: os.PathLike[str] | str,
    *,
    expected_approval_id: str,
    expected_approval_sha256: str,
    run_id: str,
    task_id: str,
    work_item_id: str,
    attempt_id: str,
    secret: str | bytes | None = None,
    environ: Mapping[str, str] | None = None,
    now: datetime | None = None,
) -> dict[str, object]:
    """Select exactly one signed task/work-item allocation and build metadata."""

    if secret is None:
        approval = load_proxy_campaign_approval_structure(
            path,
            expected_approval_id=expected_approval_id,
            expected_approval_sha256=expected_approval_sha256,
            now=now,
        )
    else:
        approval = load_proxy_campaign_approval(
            path,
            secret=secret,
            expected_approval_id=expected_approval_id,
            expected_approval_sha256=expected_approval_sha256,
            now=now,
        )
    if _identity(run_id, "run_id") != approval.run_id:
        raise ProxyCampaignValidationError(
            "runtime run_id differs from the signed campaign run_id"
        )
    normalized_task = _token(task_id, "task_id")
    normalized_item = _token(work_item_id, "work_item_id")
    matches = tuple(
        item
        for item in approval.allocations
        if item.task_id == normalized_task and item.work_item_id == normalized_item
    )
    if len(matches) != 1:
        raise ProxyCampaignValidationError(
            "approval must contain exactly one allocation for task_id/work_item_id"
        )
    return _context_payload(approval, matches[0], attempt_id)


def load_proxy_campaign_context_from_env(
    *,
    secret: str | bytes | None = None,
    environ: Mapping[str, str] | None = None,
    now: datetime | None = None,
) -> dict[str, object]:
    """Build verified ``TransportContext``/lease metadata without crypto duplication.

    An entirely absent campaign environment returns an empty mapping (the
    direct-only default).  Any partial environment fails closed.  The returned
    keys can be merged verbatim into ``TransportContext.as_dict()``.
    """

    source = os.environ if environ is None else environ
    names = (
        PROXY_APPROVAL_PATH_ENV,
        PROXY_APPROVAL_ID_ENV,
        PROXY_APPROVAL_SHA256_ENV,
        PROXY_ALLOCATION_ID_ENV,
        PROXY_ATTEMPT_ID_ENV,
    )
    values = {name: str(source.get(name, "")).strip() for name in names}
    if not any(values.values()):
        return {}
    if not all(values.values()):
        missing = ", ".join(name for name, value in values.items() if not value)
        raise ProxyCampaignValidationError(
            f"paid campaign environment is incomplete: {missing}"
        )
    if secret is None:
        approval = load_proxy_campaign_approval_structure(
            values[PROXY_APPROVAL_PATH_ENV],
            expected_approval_id=values[PROXY_APPROVAL_ID_ENV],
            expected_approval_sha256=values[PROXY_APPROVAL_SHA256_ENV],
            now=now,
        )
    else:
        approval = load_proxy_campaign_approval(
            values[PROXY_APPROVAL_PATH_ENV],
            secret=secret,
            expected_approval_id=values[PROXY_APPROVAL_ID_ENV],
            expected_approval_sha256=values[PROXY_APPROVAL_SHA256_ENV],
            now=now,
        )
    runtime_run_id = str(source.get("AIRFLOW_CTX_DAG_RUN_ID", "")).strip()
    if _identity(runtime_run_id, "AIRFLOW_CTX_DAG_RUN_ID") != approval.run_id:
        raise ProxyCampaignValidationError(
            "runtime run_id differs from the signed campaign run_id"
        )
    allocation = approval.allocation(values[PROXY_ALLOCATION_ID_ENV])
    attempt_id = _token(values[PROXY_ATTEMPT_ID_ENV], "proxy_attempt_id")
    return _context_payload(approval, allocation, attempt_id)
