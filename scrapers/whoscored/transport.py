"""Network transport for WhoScored with an explicitly bounded paid fallback.

The module deliberately owns *transport only*.  Parsers decide what valid
WhoScored content looks like and may pass a validator to :meth:`fetch`; a raw
store may be plugged in through :class:`RawCacheHook`, but this module does not
choose a storage backend.

The route order is fixed and fail-closed::

    raw cache -> direct curl (or an exact-bootstrap gate circuit) ->
    reusable direct FlareSolverr -> fresh direct curl recheck ->
    filtered paid curl -> filtered paid FlareSolverr

Paid traffic is reached only after the direct HTTP response and every direct
browser attempt were independently classified as Cloudflare challenges.  A
normal HTTP error, timeout, parser/validator rejection or browser crash never
turns a proxy on.
"""

from __future__ import annotations

import base64
import binascii
import hashlib
import hmac
import fcntl
import json
import logging
import math
import os
import random
import re
import stat
import sys
import threading
import time
import uuid
from collections import Counter, OrderedDict
from dataclasses import dataclass, field
from enum import Enum
from datetime import datetime, timezone
from pathlib import Path
from typing import (
    Any,
    Callable,
    Mapping,
    Optional,
    Protocol,
    Sequence,
    runtime_checkable,
)
from urllib.parse import parse_qsl, quote, urlencode, urljoin, urlsplit, urlunsplit

import requests

from scrapers.base.flaresolverr_client import (
    FlareSolverrCFChallengeFailed,
    FlareSolverrClient,
    FlareSolverrError,
    FlareSolverrErrorPage,
    FlareSolverrResponseTooLarge,
    FlareSolverrRuntimeIdentityError,
    FlareSolverrTabCrashed,
    FlareSolverrTimeout,
    MAX_XHR_BATCH_URLS,
    is_chromium_error_page,
)
from scrapers.whoscored.proxy_campaign import (
    PROXY_CAMPAIGN_AUTHORITY_CONTEXT_FIELDS,
    PROXY_CAMPAIGN_CONTROL_ARGUMENT_FIELDS,
    PROXY_CAMPAIGN_CONTROL_RESULT_FIELDS,
    PROXY_CAMPAIGN_CONTROL_SCHEMA_VERSION,
    ProxyCampaignApproval,
    ProxyCampaignValidationError,
    approval_from_campaign_authority_context,
    assert_paid_runtime_available,
    canonical_json_bytes,
    proxy_campaign_authority_context,
    strict_json_loads,
)
from scrapers.whoscored.runtime_contract import (
    RuntimeContractError,
    attested_runtime_file_sha256,
    require_production_runtime_class,
)
from scrapers.whoscored.source_circuit import (
    CircuitPermit,
    SharedSourceCircuit,
    SourceCircuitError,
    SourceCircuitOpen,
)

logger = logging.getLogger(__name__)

DEFAULT_DIRECT_BROWSER_ATTEMPTS = 4
DEFAULT_DIRECT_HTTP_RETRY_BACKOFF_SECONDS = 2.0
DEFAULT_BROWSER_RETRY_BACKOFF_SECONDS = 2.0
DEFAULT_BROWSER_RETRY_JITTER_SECONDS = 2.0
MAX_BROWSER_RETRY_BACKOFF_SECONDS = 30.0
SOURCE_CIRCUIT_PATH_ENV = "WHOSCORED_SOURCE_CIRCUIT_PATH"
SOURCE_CIRCUIT_WAIT_ENV = "WHOSCORED_SOURCE_CIRCUIT_WAIT"
SUPERVISOR_SESSION_OWNER_ENV = "WHOSCORED_SUPERVISOR_SESSION_OWNER"
SUPERVISOR_RESOURCE_LEDGER_ENV = "WHOSCORED_SUPERVISOR_RESOURCE_LEDGER_PATH"
PINNED_FLARESOLVERR_VERSION = "3.4.6"
FLARESOLVERR_EXTENSION_RUNTIME_PATH = "scripts/flaresolverr_extended.py"
CAPACITY_BROWSER_SESSION_OWNER_PATTERN = r"[a-z0-9]{16,32}"
_CAPACITY_BROWSER_SESSION_OWNER_RE = re.compile(
    rf"\A{CAPACITY_BROWSER_SESSION_OWNER_PATTERN}\Z",
    re.ASCII,
)


def _attested_flaresolverr_identity() -> Optional[tuple[str, str]]:
    """Resolve the browser runtime only from the completed production barrier."""

    try:
        extension_sha256 = attested_runtime_file_sha256(
            FLARESOLVERR_EXTENSION_RUNTIME_PATH
        )
    except RuntimeContractError:
        # Unit/development callers do not install the image-baked production
        # barrier.  A production marker, however, is emitted only after that
        # barrier succeeds, so losing its cached hash is a hard configuration
        # failure and can never silently disable response attestation.
        if getattr(sys, "_whoscored_runtime_class", None) == "production-v1":
            raise RuntimeContractError(
                "production WhoScored runtime has no attested FlareSolverr "
                "extension identity"
            ) from None
        return None
    return PINNED_FLARESOLVERR_VERSION, extension_sha256


def assert_paid_alert_runtime_available(context: "TransportContext") -> None:
    """Compatibility no-op: alert authority now lives only in the gateway.

    Source runners deliberately receive neither receipt-state mounts nor alert
    HMAC/bot secrets.  ``/v1/fetch`` independently checks gateway-owned state.
    """

    del context


def capacity_browser_session_prefix(owner: str) -> str:
    """Return the isolated capacity-session prefix for one validated owner."""

    if (
        type(owner) is not str
        or _CAPACITY_BROWSER_SESSION_OWNER_RE.fullmatch(owner) is None
    ):
        raise ValueError(
            "browser_session_owner must be an exact str of 16..32 "
            "lowercase ASCII letters or digits"
        )
    return f"ws-cap-{owner}-"


class TransportRoute(str, Enum):
    RAW_CACHE = "raw_cache"
    DIRECT_HTTP = "direct_http"
    DIRECT_FLARESOLVERR = "direct_flaresolverr"
    PAID_HTTP = "paid_http"
    PAID_FLARESOLVERR = "paid_flaresolverr"
    PAID_LEASE = "paid_lease"


_PAID_ROUTES = frozenset(
    {
        TransportRoute.PAID_HTTP,
        TransportRoute.PAID_FLARESOLVERR,
        TransportRoute.PAID_LEASE,
    }
)


class FailureKind(str, Enum):
    CLOUDFLARE = "cloudflare"
    HTTP_STATUS = "http_status"
    TIMEOUT = "timeout"
    BROWSER = "browser"
    CONTENT = "content"
    BUDGET = "budget"
    PROXY = "proxy"
    CACHE = "cache"
    CONFIG = "config"


class TransportPolicy(str, Enum):
    """Explicit authority boundary for routes that can incur paid traffic."""

    DIRECT_ONLY = "direct_only"
    DIRECT_THEN_PAID = "direct_then_paid"

    @classmethod
    def parse(cls, value: object) -> "TransportPolicy":
        if isinstance(value, cls):
            return value
        try:
            return cls(str(value).strip())
        except ValueError as exc:
            choices = ", ".join(policy.value for policy in cls)
            raise ValueError(f"transport_policy must be one of: {choices}") from exc


class WhoScoredTransportError(RuntimeError):
    """Base typed transport failure surfaced to the ingest state machine."""

    def __init__(
        self,
        message: str,
        *,
        kind: FailureKind,
        url: str,
        route: Optional[TransportRoute] = None,
        status_code: Optional[int] = None,
        retryable: bool = False,
    ) -> None:
        super().__init__(message)
        self.kind = kind
        self.url = url
        self.route = route
        self.status_code = status_code
        self.retryable = retryable


class CloudflareChallenge(WhoScoredTransportError):
    def __init__(
        self,
        message: str,
        *,
        url: str,
        route: TransportRoute,
        status_code: Optional[int] = None,
        source_wide: bool = False,
    ) -> None:
        super().__init__(
            message,
            kind=FailureKind.CLOUDFLARE,
            url=url,
            route=route,
            status_code=status_code,
            retryable=True,
        )
        self.source_wide = bool(source_wide)


class TransportBudgetExceeded(WhoScoredTransportError):
    def __init__(self, message: str, *, url: str, route: TransportRoute) -> None:
        super().__init__(
            message,
            kind=FailureKind.BUDGET,
            url=url,
            route=route,
            retryable=False,
        )


DEFAULT_PAID_BYTES_PER_DAGRUN = 8_000_000
DEFAULT_PAID_BYTES_PER_URL = 2_000_000
PAID_GATEWAY_SCHEMA_VERSION = 1
PAID_GATEWAY_TOKEN_ENV = "WHOSCORED_PAID_GATEWAY_TOKEN"
MAX_PAID_GATEWAY_RESPONSE_BYTES = 4 * 1024 * 1024
MAX_PAID_GATEWAY_BATCH_URLS = 8
MAX_PAID_GATEWAY_RESPONSE_DOCUMENT_BYTES = (
    (MAX_PAID_GATEWAY_RESPONSE_BYTES * 4 // 3) + 128 * 1024
)
MAX_PAID_GATEWAY_BATCH_RESPONSE_DOCUMENT_BYTES = (
    MAX_PAID_GATEWAY_BATCH_URLS * MAX_PAID_GATEWAY_RESPONSE_DOCUMENT_BYTES
    + 256 * 1024
)
MAX_PAID_GATEWAY_CONTROL_DOCUMENT_BYTES = 5 * 1024 * 1024
PAID_GATEWAY_CLEANUP_GRACE_SECONDS = 15.0
PAID_GATEWAY_CAMPAIGN_OPERATIONS = frozenset(
    PROXY_CAMPAIGN_CONTROL_ARGUMENT_FIELDS
)
_PAID_GATEWAY_RESPONSE_FIELDS = frozenset(
    {
        "schema_version",
        "url",
        "status_code",
        "headers",
        "body_base64",
        "body_sha256",
        "route",
        "receipt",
    }
)
_PAID_GATEWAY_BATCH_RESPONSE_FIELDS = frozenset(
    {"schema_version", "target_manifest_sha256", "results", "route", "receipt"}
)
_PAID_GATEWAY_BATCH_ITEM_FIELDS = frozenset(
    {"url", "status_code", "headers", "body_base64", "body_sha256"}
)
_PAID_GATEWAY_RECEIPT_FIELDS = frozenset(
    {
        "schema_version",
        "campaign_id",
        "approval_id",
        "approval_sha256",
        "allocation_id",
        "attempt_id_hash",
        "canonical_url_sha256",
        "lease_id_hash",
        "route",
        "up_bytes",
        "down_bytes",
        "total_bytes",
        "provider_billed_bytes",
        "close_complete",
        "cleanup_complete",
    }
)
_PAID_GATEWAY_BATCH_RECEIPT_FIELDS = frozenset(
    {
        "schema_version",
        "campaign_id",
        "approval_id",
        "approval_sha256",
        "allocation_id",
        "attempt_id_hash",
        "target_manifest_sha256",
        "lease_id_hash",
        "route",
        "up_bytes",
        "down_bytes",
        "total_bytes",
        "provider_billed_bytes",
        "bootstrap_provider_billed_bytes",
        "endpoint_provider_billed_bytes",
        "close_complete",
        "cleanup_complete",
    }
)
_PAID_GATEWAY_ERROR_FIELDS = frozenset({"schema_version", "error"})
_PAID_GATEWAY_SETTLED_ERROR_FIELDS = _PAID_GATEWAY_ERROR_FIELDS | {"receipt"}
_LOWER_SHA256_RE = re.compile(r"\A[0-9a-f]{64}\Z", re.ASCII)
_PAID_GATEWAY_ERROR_CODE_RE = re.compile(r"\A[a-z][a-z0-9_]{0,63}\Z", re.ASCII)


def _paid_gateway_target_manifest_sha256(
    urls: Sequence[str], *, browser_bootstrap_url: Optional[str] = None
) -> str:
    canonical_urls = [_canonical_url_key(url) for url in urls]
    document: dict[str, object] = {
        "schema_version": PAID_GATEWAY_SCHEMA_VERSION,
        "urls": canonical_urls,
    }
    if browser_bootstrap_url is not None:
        document["browser_bootstrap_url"] = _canonical_url_key(browser_bootstrap_url)
    return hashlib.sha256(
        canonical_json_bytes(document)
    ).hexdigest()


@dataclass(frozen=True)
class TransportContext:
    """Stable request identity shared by Airflow, transport and proxy filter."""

    dag_id: str = ""
    run_id: str = ""
    task_id: str = ""
    map_index: int = -1
    try_number: int = 0
    scope: str = ""
    entity: str = ""
    transport_policy: str = TransportPolicy.DIRECT_ONLY.value
    proxy_campaign: Mapping[str, object] = field(default_factory=dict, repr=False)

    @classmethod
    def from_env(cls) -> "TransportContext":
        from scrapers.whoscored.proxy_campaign import (
            load_proxy_campaign_context_from_env,
        )

        def _integer(name: str, default: int) -> int:
            try:
                return int(os.environ.get(name, default))
            except (TypeError, ValueError):
                return default

        campaign = load_proxy_campaign_context_from_env()
        configured_policy = os.environ.get("WHOSCORED_TRANSPORT_POLICY", "").strip()
        campaign_policy = str(campaign.get("transport_policy", "")).strip()
        if campaign_policy:
            if configured_policy and configured_policy != campaign_policy:
                raise ValueError(
                    "WHOSCORED_TRANSPORT_POLICY differs from the signed approval"
                )
            resolved_policy = campaign_policy
        else:
            resolved_policy = configured_policy or TransportPolicy.DIRECT_ONLY.value
            if resolved_policy != TransportPolicy.DIRECT_ONLY.value:
                raise ValueError(
                    "direct_then_paid requires a complete signed proxy campaign"
                )
        TransportPolicy.parse(resolved_policy)
        return cls(
            dag_id=os.environ.get("AIRFLOW_CTX_DAG_ID", ""),
            run_id=os.environ.get("AIRFLOW_CTX_DAG_RUN_ID", ""),
            task_id=os.environ.get("AIRFLOW_CTX_TASK_ID", ""),
            map_index=_integer("AIRFLOW_CTX_MAP_INDEX", -1),
            try_number=_integer("AIRFLOW_CTX_TRY_NUMBER", 0),
            scope=os.environ.get("WHOSCORED_SCOPE", ""),
            entity=os.environ.get("WHOSCORED_ENTITY", ""),
            transport_policy=resolved_policy,
            proxy_campaign=campaign,
        )

    def request_context(
        self, *, scope: Optional[str] = None, entity: Optional[str] = None
    ) -> "TransportContext":
        return TransportContext(
            dag_id=self.dag_id,
            run_id=self.run_id,
            task_id=self.task_id,
            map_index=self.map_index,
            try_number=self.try_number,
            scope=self.scope if scope is None else scope,
            entity=self.entity if entity is None else entity,
            transport_policy=self.transport_policy,
            proxy_campaign=self.proxy_campaign,
        )

    @property
    def dagrun_key(self) -> str:
        if self.dag_id and self.run_id:
            return f"{self.dag_id}/{self.run_id}"
        return ""

    def as_dict(self) -> dict[str, Any]:
        result: dict[str, Any] = {
            "dag_id": self.dag_id,
            "run_id": self.run_id,
            "task_id": self.task_id,
            "map_index": self.map_index,
            "try_number": self.try_number,
            "scope": self.scope,
            "entity": self.entity,
            "transport_policy": self.transport_policy,
        }
        result.update(dict(self.proxy_campaign))
        return result


@dataclass(frozen=True)
class PaidCampaignContext:
    """Allocation-free signed identity for alert and campaign-control RPCs."""

    document: Mapping[str, object] = field(repr=False)

    def __post_init__(self) -> None:
        normalized = dict(self.document)
        approval_from_campaign_authority_context(
            normalized,
            secret=None,
            require_active=False,
        )
        object.__setattr__(self, "document", normalized)

    @classmethod
    def from_transport_context(
        cls, context: TransportContext
    ) -> "PaidCampaignContext":
        full = context.as_dict()
        return cls(
            {
                name: full[name]
                for name in PROXY_CAMPAIGN_AUTHORITY_CONTEXT_FIELDS
            }
        )

    @classmethod
    def from_approval(
        cls, approval: ProxyCampaignApproval
    ) -> "PaidCampaignContext":
        return cls(proxy_campaign_authority_context(approval))

    def as_dict(self) -> dict[str, object]:
        return dict(self.document)


@runtime_checkable
class RequestLedger(Protocol):
    """Append-only sink; implementations must make one append durable."""

    def append(self, event: Mapping[str, Any]) -> None:
        """Persist one complete request/accounting event."""


class JsonlRequestLedger:
    """Process-safe append-only JSONL request ledger with an fsync commit."""

    def __init__(self, path: str) -> None:
        if not path:
            raise ValueError("request ledger path must not be empty")
        self.path = Path(path)
        self._lock = threading.Lock()

    def append(self, event: Mapping[str, Any]) -> None:
        rendered = (
            json.dumps(
                dict(event), ensure_ascii=False, sort_keys=True, separators=(",", ":")
            ).encode("utf-8")
            + b"\n"
        )
        self.path.parent.mkdir(parents=True, exist_ok=True)
        flags = (
            os.O_APPEND
            | os.O_CREAT
            | os.O_WRONLY
            | getattr(os, "O_CLOEXEC", 0)
            | getattr(os, "O_NOFOLLOW", 0)
        )
        with self._lock:
            descriptor = os.open(self.path, flags, 0o600)
            try:
                metadata = os.fstat(descriptor)
                if not stat.S_ISREG(metadata.st_mode):
                    raise OSError("request ledger is not a regular file")
                os.fchmod(descriptor, 0o600)
                fcntl.flock(descriptor, fcntl.LOCK_EX)
                pending = memoryview(rendered)
                while pending:
                    written = os.write(descriptor, pending)
                    if written <= 0:
                        raise OSError("request ledger write made no progress")
                    pending = pending[written:]
                os.fsync(descriptor)
            finally:
                fcntl.flock(descriptor, fcntl.LOCK_UN)
                os.close(descriptor)


class ProxyBudgetRejected(RuntimeError):
    """The shared filtering proxy rejected a lease at a hard byte ceiling."""


class ProxyConcurrencyLimited(RuntimeError):
    """The single paid slot is in use by another task in the DagRun."""


class ProxyCampaignControlRejected(RuntimeError):
    """The filter rejected a bounded campaign-ledger control operation."""


@dataclass(frozen=True)
class CachedPayload:
    """Raw cache value.  ``content`` must be the source response, not parsed data."""

    content: bytes
    status_code: int = 200
    headers: Mapping[str, str] = field(default_factory=dict)
    observed_at: Optional[str] = None


@runtime_checkable
class RawCacheHook(Protocol):
    """Minimal content-addressed raw-store hook used by the transport."""

    def load(self, key: str) -> Optional[CachedPayload]:
        """Return a cached successful source response, or ``None``."""

    def store(self, key: str, payload: CachedPayload, sha256: str) -> None:
        """Persist a validated response before a caller writes parsed rows."""


@dataclass(frozen=True)
class TransportResponse:
    url: str
    content: bytes
    status_code: int
    headers: Mapping[str, str]
    route: TransportRoute
    wire_bytes: int
    sha256: str
    request_bytes: int = 0
    response_bytes: int = 0
    resource_bytes: int = 0
    observed_at: str = ""

    @property
    def text(self) -> str:
        return self.content.decode("utf-8", errors="replace")


@dataclass(frozen=True)
class TransportBudgets:
    """Per-transport hard limits; the proxy also enforces each lease server-side."""

    max_response_bytes: int = 4 * 1024 * 1024
    max_paid_bytes_per_url: int = DEFAULT_PAID_BYTES_PER_URL
    max_paid_bytes_per_lease: int = DEFAULT_PAID_BYTES_PER_URL
    # Kept under the old name because the service tightens the existing
    # object between entity batches.  The filtering proxy enforces this value
    # across *all* tasks in the same dag_id/run_id, not merely this process.
    max_paid_bytes_per_task: int = DEFAULT_PAID_BYTES_PER_DAGRUN
    max_paid_urls: int = 20
    max_paid_browser_bootstraps: int = 2
    lease_ttl_seconds: int = 60

    @classmethod
    def for_eligible_urls(
        cls, eligible_urls: int, **overrides: int
    ) -> "TransportBudgets":
        paid_limit = (
            0 if eligible_urls <= 0 else min(20, max(3, (eligible_urls + 49) // 50))
        )
        paid_limit = int(overrides.pop("max_paid_urls", paid_limit))
        return cls(max_paid_urls=paid_limit, **overrides)


@dataclass
class TransportStats:
    route_requests: Counter[str] = field(default_factory=Counter)
    route_wire_bytes: Counter[str] = field(default_factory=Counter)
    failures: Counter[str] = field(default_factory=Counter)
    cache_hits: int = 0
    cache_invalid: int = 0
    browser_sessions: int = 0
    browser_batches: int = 0
    browser_batch_items: int = 0
    paid_urls: set[str] = field(default_factory=set)
    paid_proxy_up_bytes: int = 0
    paid_proxy_down_bytes: int = 0
    paid_proxy_bytes_by_url: dict[str, int] = field(default_factory=dict)

    @property
    def paid_proxy_bytes(self) -> int:
        return self.paid_proxy_up_bytes + self.paid_proxy_down_bytes

    def as_dict(self) -> dict[str, Any]:
        return {
            "route_requests": dict(self.route_requests),
            "route_wire_bytes": dict(self.route_wire_bytes),
            "failures": dict(self.failures),
            "cache_hits": self.cache_hits,
            "cache_invalid": self.cache_invalid,
            "browser_sessions": self.browser_sessions,
            "browser_batches": self.browser_batches,
            "browser_batch_items": self.browser_batch_items,
            "paid_urls": len(self.paid_urls),
            "paid_proxy_up_bytes": self.paid_proxy_up_bytes,
            "paid_proxy_down_bytes": self.paid_proxy_down_bytes,
            "paid_proxy_bytes": self.paid_proxy_bytes,
            "paid_proxy_bytes_by_url": dict(self.paid_proxy_bytes_by_url),
        }


@dataclass(frozen=True)
class ProxyLease:
    lease_id: str
    token: str = field(repr=False)
    proxy_url: str = field(repr=False)
    max_bytes: int
    expires_at: float


class ProxyFilterClient:
    """Small client for the filtering proxy's lease control plane.

    No retry adapter is installed: lease creation is stateful and a hidden
    retry could allocate multiple paid exits.
    """

    def __init__(
        self,
        proxy_url: str,
        *,
        control_url: Optional[str] = None,
        control_token: Optional[str] = None,
        timeout: float = 5.0,
        session: Optional[requests.Session] = None,
    ) -> None:
        # These endpoints cross a secret boundary: lease credentials are
        # added by this client and must never arrive embedded in configuration.
        # Normalize only credential-free HTTP(S) origins so downstream
        # exceptions cannot echo a configured username/password.
        self.proxy_url = _credential_free_proxy_origin(
            proxy_url, label="filtering proxy URL"
        )
        self.control_url = _credential_free_proxy_origin(
            control_url or proxy_url, label="filtering proxy control URL"
        )
        resolved_token = str(
            control_token
            if control_token is not None
            else _proxy_control_token_from_environment()
        ).strip()
        if len(resolved_token) < 32:
            raise ValueError(
                "WHOSCORED_PROXY_CONTROL_TOKEN must contain at least 32 characters "
                "when the paid WhoScored proxy is configured"
            )
        self._control_token = resolved_token
        self.timeout = timeout
        self.session = session or requests.Session()
        self.session.trust_env = False

    def create_lease(
        self,
        *,
        max_bytes: int,
        ttl_seconds: int,
        context: Optional[TransportContext] = None,
        canonical_url: str = "",
        target_manifest_sha256: str = "",
        logical_target_units: int = 1,
        expected_endpoint_labels: Sequence[str] = (),
    ) -> ProxyLease:
        request: dict[str, Any] = {
            "max_bytes": max_bytes,
            "ttl_seconds": ttl_seconds,
            "canonical_url": canonical_url,
        }
        if context is not None:
            request.update(context.as_dict())
        if not target_manifest_sha256 and (
            logical_target_units != 1 or expected_endpoint_labels
        ):
            raise ValueError(
                "multi-target lease requires a target manifest and endpoint labels"
            )
        if target_manifest_sha256:
            if (
                type(target_manifest_sha256) is not str
                or not re.fullmatch(r"[0-9a-f]{64}", target_manifest_sha256)
                or isinstance(logical_target_units, bool)
                or not isinstance(logical_target_units, int)
                or logical_target_units <= 0
                or len(expected_endpoint_labels) != logical_target_units + 1
            ):
                raise ValueError("batch lease binding is invalid")
            request.update(
                target_manifest_sha256=target_manifest_sha256,
                logical_target_units=logical_target_units,
                expected_endpoint_labels=list(expected_endpoint_labels),
            )
        response = self.session.post(
            f"{self.control_url}/v1/leases",
            json=request,
            headers={"X-Proxy-Control-Token": self._control_token},
            timeout=self.timeout,
        )
        if int(getattr(response, "status_code", 0) or 0) == 429:
            try:
                rejected = dict(response.json())
            except (TypeError, ValueError):
                rejected = {}
            message = str(rejected.get("error") or "paid proxy lease rejected")
            if rejected.get("code") == "budget_exceeded":
                raise ProxyBudgetRejected(message)
            if rejected.get("code") == "concurrency_limited":
                raise ProxyConcurrencyLimited(message)
        response.raise_for_status()
        data = response.json()
        lease_id = str(data["id"])
        token = str(data["token"])
        lease_proxy_url = str(data.get("proxy_url") or self.proxy_url)
        return ProxyLease(
            lease_id=lease_id,
            token=token,
            proxy_url=_proxy_url_with_lease(lease_proxy_url, token),
            max_bytes=int(data.get("max_bytes", max_bytes)),
            expires_at=float(data.get("expires_at", time.time() + ttl_seconds)),
        )

    def stats(self, lease: ProxyLease) -> dict[str, Any]:
        response = self.session.get(
            f"{self.control_url}/v1/leases/{lease.lease_id}/stats",
            headers={
                "X-Proxy-Control-Token": self._control_token,
                "Authorization": f"Bearer {lease.token}",
            },
            timeout=self.timeout,
        )
        response.raise_for_status()
        return dict(response.json())

    def close(self, lease: ProxyLease) -> dict[str, Any]:
        response = self.session.delete(
            f"{self.control_url}/v1/leases/{lease.lease_id}",
            headers={
                "X-Proxy-Control-Token": self._control_token,
                "Authorization": f"Bearer {lease.token}",
            },
            timeout=self.timeout,
        )
        response.raise_for_status()
        return dict(response.json())

    def begin_endpoint(self, lease: ProxyLease, endpoint: str) -> str:
        """Open one provider-byte attribution boundary for an active lease."""

        if type(endpoint) is not str or not endpoint or len(endpoint) > 200:
            raise ValueError("lease endpoint must be a non-empty bounded string")
        response = self.session.post(
            f"{self.control_url}/v1/leases/{lease.lease_id}/endpoints",
            json={"endpoint": endpoint},
            headers={
                "X-Proxy-Control-Token": self._control_token,
                "Authorization": f"Bearer {lease.token}",
            },
            timeout=self.timeout,
        )
        response.raise_for_status()
        data = response.json()
        request_id = data.get("request_id") if isinstance(data, Mapping) else None
        if type(request_id) is not str or not request_id:
            raise ValueError("filtering proxy endpoint receipt is invalid")
        return request_id

    def end_endpoint(self, lease: ProxyLease, request_id: str) -> dict[str, Any]:
        """Durably close the exact endpoint boundary opened above."""

        if type(request_id) is not str or not request_id:
            raise ValueError("lease endpoint request id is invalid")
        response = self.session.delete(
            f"{self.control_url}/v1/leases/{lease.lease_id}/endpoints/{request_id}",
            headers={
                "X-Proxy-Control-Token": self._control_token,
                "Authorization": f"Bearer {lease.token}",
            },
            timeout=self.timeout,
        )
        response.raise_for_status()
        data = response.json()
        if not isinstance(data, Mapping) or data.get("id") != lease.lease_id:
            raise ValueError("filtering proxy endpoint accounting is invalid")
        return dict(data)

    def switch_endpoint(
        self, lease: ProxyLease, request_id: str, endpoint: str
    ) -> str:
        """Atomically close one owner and install the next while the browser lives."""

        if type(request_id) is not str or not request_id:
            raise ValueError("lease endpoint request id is invalid")
        if type(endpoint) is not str or not endpoint or len(endpoint) > 200:
            raise ValueError("lease endpoint must be a non-empty bounded string")
        response = self.session.post(
            f"{self.control_url}/v1/leases/{lease.lease_id}/endpoints/{request_id}/switch",
            json={"endpoint": endpoint},
            headers={
                "X-Proxy-Control-Token": self._control_token,
                "Authorization": f"Bearer {lease.token}",
            },
            timeout=self.timeout,
        )
        response.raise_for_status()
        data = response.json()
        next_request_id = (
            data.get("request_id") if isinstance(data, Mapping) else None
        )
        if type(next_request_id) is not str or not next_request_id:
            raise ValueError("filtering proxy endpoint switch receipt is invalid")
        return next_request_id

    def campaign_control(
        self,
        operation: str,
        *,
        context: Any,
        arguments: Mapping[str, object],
    ) -> Mapping[str, object]:
        """Execute one exact WhoScored ledger operation through the filter."""

        expected_arguments = PROXY_CAMPAIGN_CONTROL_ARGUMENT_FIELDS.get(operation)
        if expected_arguments is None or frozenset(arguments) != expected_arguments:
            raise ValueError("campaign control operation or arguments are invalid")
        context_document = context.as_dict()
        approval_from_campaign_authority_context(
            context_document,
            secret=None,
            require_active=False,
        )
        request = {
            "schema_version": PROXY_CAMPAIGN_CONTROL_SCHEMA_VERSION,
            "operation": operation,
            "context": context_document,
            "arguments": dict(arguments),
        }
        response: object
        try:
            response = self.session.post(
                f"{self.control_url}/v1/whoscored/campaign-control",
                data=canonical_json_bytes(request),
                headers={
                    "Content-Type": "application/json",
                    "X-Proxy-Control-Token": self._control_token,
                },
                timeout=self.timeout,
                stream=True,
            )
        except Exception as exc:
            raise ProxyCampaignControlRejected(
                f"campaign control request failed: {type(exc).__name__}"
            ) from None
        try:
            chunks: list[bytes] = []
            size = 0
            for raw in response.iter_content(chunk_size=64 * 1024):
                chunk = bytes(raw or b"")
                size += len(chunk)
                if size > MAX_PAID_GATEWAY_CONTROL_DOCUMENT_BYTES:
                    raise ProxyCampaignControlRejected(
                        "campaign control response is oversized"
                    )
                chunks.append(chunk)
            body = b"".join(chunks)
            try:
                decoded = strict_json_loads(body.decode("utf-8"))
            except (
                UnicodeDecodeError,
                json.JSONDecodeError,
                ProxyCampaignValidationError,
            ):
                raise ProxyCampaignControlRejected(
                    "campaign control response is not strict JSON"
                ) from None
            status = int(getattr(response, "status_code", 0) or 0)
            if status != 200:
                raise ProxyCampaignControlRejected("campaign control was rejected")
            if (
                not isinstance(decoded, Mapping)
                or frozenset(decoded) != {
                    "schema_version",
                    "operation",
                    "result",
                }
                or decoded.get("schema_version")
                != PROXY_CAMPAIGN_CONTROL_SCHEMA_VERSION
                or decoded.get("operation") != operation
                or canonical_json_bytes(dict(decoded)) != body
            ):
                raise ProxyCampaignControlRejected(
                    "campaign control response envelope is invalid"
                )
            result = decoded.get("result")
            if (
                not isinstance(result, Mapping)
                or frozenset(result)
                != PROXY_CAMPAIGN_CONTROL_RESULT_FIELDS[operation]
            ):
                raise ProxyCampaignControlRejected(
                    "campaign control result fields are invalid"
                )
            if operation == "assert_exact_accounting":
                billed = result.get("provider_billed_bytes")
                if isinstance(billed, bool) or not isinstance(billed, int) or billed < 0:
                    raise ProxyCampaignControlRejected(
                        "campaign control accounting result is invalid"
                    )
            elif not isinstance(
                result.get(
                    "allocation" if operation == "complete_allocation" else "campaign"
                ),
                Mapping,
            ):
                raise ProxyCampaignControlRejected(
                    "campaign control snapshot result is invalid"
                )
            return dict(result)
        except ProxyCampaignControlRejected:
            raise
        except Exception as exc:
            raise ProxyCampaignControlRejected(
                f"campaign control response failed: {type(exc).__name__}"
            ) from None
        finally:
            close_response = getattr(response, "close", None)
            if callable(close_response):
                try:
                    close_response()
                except Exception:
                    pass

    def close_session(self) -> None:
        """Release the control-plane HTTP connection pool."""

        self.session.close()


class PaidGatewayError(RuntimeError):
    """A paid application-gateway request failed without exposing secrets."""


class PaidGatewayRejected(PaidGatewayError):
    """The gateway rejected signed authority or a bounded spend request."""

    def __init__(
        self,
        code: str,
        *,
        receipt: Optional["PaidGatewayReceipt | PaidGatewayBatchReceipt"] = None,
    ) -> None:
        super().__init__(f"paid gateway rejected fetch: {code}")
        self.code = code
        self.receipt = receipt


class PaidGatewayProtocolError(PaidGatewayError):
    """The gateway returned a malformed or incomplete cleanup receipt."""


@dataclass(frozen=True)
class PaidGatewayReceipt:
    """Credential-free accounting returned only after gateway cleanup."""

    campaign_id: str
    approval_id: str
    approval_sha256: str
    allocation_id: str
    attempt_id_hash: str
    canonical_url_sha256: str
    lease_id_hash: str
    route: TransportRoute
    up_bytes: int
    down_bytes: int
    total_bytes: int
    provider_billed_bytes: int
    close_complete: bool
    cleanup_complete: bool

    @classmethod
    def from_dict(
        cls,
        value: object,
        *,
        context: TransportContext,
        url: str,
        max_provider_bytes: Optional[int] = None,
    ) -> "PaidGatewayReceipt":
        if not isinstance(value, Mapping) or frozenset(value) != (
            _PAID_GATEWAY_RECEIPT_FIELDS
        ):
            raise PaidGatewayProtocolError("paid gateway receipt fields are invalid")
        if value.get("schema_version") != PAID_GATEWAY_SCHEMA_VERSION:
            raise PaidGatewayProtocolError("paid gateway receipt schema is invalid")

        def digest(field: str) -> str:
            item = value.get(field)
            if type(item) is not str or _LOWER_SHA256_RE.fullmatch(item) is None:
                raise PaidGatewayProtocolError(
                    f"paid gateway receipt {field} is invalid"
                )
            return item

        def counter(field: str) -> int:
            item = value.get(field)
            if isinstance(item, bool) or not isinstance(item, int) or item < 0:
                raise PaidGatewayProtocolError(
                    f"paid gateway receipt {field} is invalid"
                )
            return item

        campaign = context.proxy_campaign
        expected_campaign_id = campaign.get("proxy_campaign_id")
        expected_approval_id = campaign.get("proxy_approval_id")
        expected_approval_sha256 = campaign.get("proxy_approval_sha256")
        expected_allocation_id = campaign.get("proxy_allocation_id")
        route_value = value.get("route")
        try:
            route = TransportRoute(str(route_value))
        except ValueError as exc:
            raise PaidGatewayProtocolError(
                "paid gateway receipt route is invalid"
            ) from exc
        if route not in {
            TransportRoute.PAID_HTTP,
            TransportRoute.PAID_FLARESOLVERR,
        }:
            raise PaidGatewayProtocolError("paid gateway receipt route is not paid")

        up = counter("up_bytes")
        down = counter("down_bytes")
        total = counter("total_bytes")
        provider_billed = counter("provider_billed_bytes")
        if total != up + down or provider_billed != total:
            raise PaidGatewayProtocolError(
                "paid gateway receipt byte accounting is inconsistent"
            )
        if (
            max_provider_bytes is not None
            and (
                isinstance(max_provider_bytes, bool)
                or not isinstance(max_provider_bytes, int)
                or max_provider_bytes <= 0
                or total > max_provider_bytes
            )
        ):
            raise PaidGatewayProtocolError(
                "paid gateway receipt exceeds the requested provider-byte cap"
            )
        if (
            value.get("close_complete") is not True
            or value.get("cleanup_complete") is not True
        ):
            raise PaidGatewayProtocolError(
                "paid gateway did not complete lease and browser cleanup"
            )
        canonical_url_sha256 = digest("canonical_url_sha256")
        if canonical_url_sha256 != hashlib.sha256(
            _canonical_url_key(url).encode("utf-8")
        ).hexdigest():
            raise PaidGatewayProtocolError(
                "paid gateway receipt belongs to another URL"
            )
        attempt_id = campaign.get("proxy_attempt_id")
        attempt_id_hash = digest("attempt_id_hash")
        if type(attempt_id) is not str or attempt_id_hash != hashlib.sha256(
            attempt_id.encode("utf-8")
        ).hexdigest():
            raise PaidGatewayProtocolError(
                "paid gateway receipt belongs to another task attempt"
            )
        if (
            value.get("campaign_id") != expected_campaign_id
            or value.get("approval_id") != expected_approval_id
            or digest("approval_sha256") != expected_approval_sha256
            or value.get("allocation_id") != expected_allocation_id
        ):
            raise PaidGatewayProtocolError(
                "paid gateway receipt belongs to another signed campaign"
            )
        return cls(
            campaign_id=str(expected_campaign_id),
            approval_id=str(expected_approval_id),
            approval_sha256=str(expected_approval_sha256),
            allocation_id=str(expected_allocation_id),
            attempt_id_hash=attempt_id_hash,
            canonical_url_sha256=canonical_url_sha256,
            lease_id_hash=digest("lease_id_hash"),
            route=route,
            up_bytes=up,
            down_bytes=down,
            total_bytes=total,
            provider_billed_bytes=provider_billed,
            close_complete=True,
            cleanup_complete=True,
        )

    def to_dict(self) -> dict[str, object]:
        return {
            "schema_version": PAID_GATEWAY_SCHEMA_VERSION,
            "campaign_id": self.campaign_id,
            "approval_id": self.approval_id,
            "approval_sha256": self.approval_sha256,
            "allocation_id": self.allocation_id,
            "attempt_id_hash": self.attempt_id_hash,
            "canonical_url_sha256": self.canonical_url_sha256,
            "lease_id_hash": self.lease_id_hash,
            "route": self.route.value,
            "up_bytes": self.up_bytes,
            "down_bytes": self.down_bytes,
            "total_bytes": self.total_bytes,
            "provider_billed_bytes": self.provider_billed_bytes,
            "close_complete": self.close_complete,
            "cleanup_complete": self.cleanup_complete,
        }


@dataclass(frozen=True)
class PaidGatewayResponse:
    """One high-level paid fetch result; contains no reusable capability."""

    url: str
    content: bytes
    status_code: int
    headers: Mapping[str, str]
    route: TransportRoute
    receipt: PaidGatewayReceipt


@dataclass(frozen=True)
class PaidGatewayBatchItem:
    """One credential-free body inside an atomic paid batch response."""

    url: str
    content: bytes
    status_code: int
    headers: Mapping[str, str]


@dataclass(frozen=True)
class PaidGatewayBatchReceipt:
    """Aggregate and per-endpoint accounting for one cleaned batch lease."""

    campaign_id: str
    approval_id: str
    approval_sha256: str
    allocation_id: str
    attempt_id_hash: str
    target_manifest_sha256: str
    lease_id_hash: str
    route: TransportRoute
    up_bytes: int
    down_bytes: int
    total_bytes: int
    provider_billed_bytes: int
    bootstrap_provider_billed_bytes: int
    endpoint_provider_billed_bytes: Mapping[str, int]
    close_complete: bool
    cleanup_complete: bool

    @classmethod
    def from_dict(
        cls,
        value: object,
        *,
        context: TransportContext,
        urls: Sequence[str],
        browser_bootstrap_url: str,
        max_provider_bytes: int,
    ) -> "PaidGatewayBatchReceipt":
        if not isinstance(value, Mapping) or frozenset(value) != (
            _PAID_GATEWAY_BATCH_RECEIPT_FIELDS
        ):
            raise PaidGatewayProtocolError(
                "paid gateway batch receipt fields are invalid"
            )
        if value.get("schema_version") != PAID_GATEWAY_SCHEMA_VERSION:
            raise PaidGatewayProtocolError(
                "paid gateway batch receipt schema is invalid"
            )

        def digest(field: str) -> str:
            item = value.get(field)
            if type(item) is not str or _LOWER_SHA256_RE.fullmatch(item) is None:
                raise PaidGatewayProtocolError(
                    f"paid gateway batch receipt {field} is invalid"
                )
            return item

        def counter(field: str) -> int:
            item = value.get(field)
            if isinstance(item, bool) or not isinstance(item, int) or item < 0:
                raise PaidGatewayProtocolError(
                    f"paid gateway batch receipt {field} is invalid"
                )
            return item

        campaign = context.proxy_campaign
        attempt_id = campaign.get("proxy_attempt_id")
        attempt_hash = digest("attempt_id_hash")
        if type(attempt_id) is not str or attempt_hash != hashlib.sha256(
            attempt_id.encode("utf-8")
        ).hexdigest():
            raise PaidGatewayProtocolError(
                "paid gateway batch receipt belongs to another task attempt"
            )
        manifest = digest("target_manifest_sha256")
        if manifest != _paid_gateway_target_manifest_sha256(
            urls, browser_bootstrap_url=browser_bootstrap_url
        ):
            raise PaidGatewayProtocolError(
                "paid gateway batch receipt belongs to another target manifest"
            )
        route_value = value.get("route")
        if route_value != TransportRoute.PAID_FLARESOLVERR.value:
            raise PaidGatewayProtocolError(
                "paid gateway batch receipt route is invalid"
            )
        up = counter("up_bytes")
        down = counter("down_bytes")
        total = counter("total_bytes")
        billed = counter("provider_billed_bytes")
        bootstrap = counter("bootstrap_provider_billed_bytes")
        if total != up + down or billed != total or total > max_provider_bytes:
            raise PaidGatewayProtocolError(
                "paid gateway batch receipt byte accounting is inconsistent"
            )
        raw_endpoints = value.get("endpoint_provider_billed_bytes")
        expected_digests = {
            hashlib.sha256(_canonical_url_key(url).encode("utf-8")).hexdigest()
            for url in urls
        }
        if not isinstance(raw_endpoints, Mapping) or set(raw_endpoints) != expected_digests:
            raise PaidGatewayProtocolError(
                "paid gateway batch endpoint accounting is incomplete"
            )
        endpoints: dict[str, int] = {}
        for endpoint_digest, raw_amount in raw_endpoints.items():
            if (
                type(endpoint_digest) is not str
                or _LOWER_SHA256_RE.fullmatch(endpoint_digest) is None
                or isinstance(raw_amount, bool)
                or not isinstance(raw_amount, int)
                or raw_amount < 0
            ):
                raise PaidGatewayProtocolError(
                    "paid gateway batch endpoint accounting is invalid"
                )
            endpoints[endpoint_digest] = raw_amount
        if bootstrap + sum(endpoints.values()) != total:
            raise PaidGatewayProtocolError(
                "paid gateway batch attribution does not equal aggregate bytes"
            )
        if (
            value.get("campaign_id") != campaign.get("proxy_campaign_id")
            or value.get("approval_id") != campaign.get("proxy_approval_id")
            or digest("approval_sha256")
            != campaign.get("proxy_approval_sha256")
            or value.get("allocation_id") != campaign.get("proxy_allocation_id")
            or value.get("close_complete") is not True
            or value.get("cleanup_complete") is not True
        ):
            raise PaidGatewayProtocolError(
                "paid gateway batch receipt authority or cleanup is invalid"
            )
        return cls(
            campaign_id=str(value["campaign_id"]),
            approval_id=str(value["approval_id"]),
            approval_sha256=digest("approval_sha256"),
            allocation_id=str(value["allocation_id"]),
            attempt_id_hash=attempt_hash,
            target_manifest_sha256=manifest,
            lease_id_hash=digest("lease_id_hash"),
            route=TransportRoute.PAID_FLARESOLVERR,
            up_bytes=up,
            down_bytes=down,
            total_bytes=total,
            provider_billed_bytes=billed,
            bootstrap_provider_billed_bytes=bootstrap,
            endpoint_provider_billed_bytes=endpoints,
            close_complete=True,
            cleanup_complete=True,
        )

    def to_dict(self) -> dict[str, object]:
        return {
            "schema_version": PAID_GATEWAY_SCHEMA_VERSION,
            "campaign_id": self.campaign_id,
            "approval_id": self.approval_id,
            "approval_sha256": self.approval_sha256,
            "allocation_id": self.allocation_id,
            "attempt_id_hash": self.attempt_id_hash,
            "target_manifest_sha256": self.target_manifest_sha256,
            "lease_id_hash": self.lease_id_hash,
            "route": self.route.value,
            "up_bytes": self.up_bytes,
            "down_bytes": self.down_bytes,
            "total_bytes": self.total_bytes,
            "provider_billed_bytes": self.provider_billed_bytes,
            "bootstrap_provider_billed_bytes": self.bootstrap_provider_billed_bytes,
            "endpoint_provider_billed_bytes": dict(
                self.endpoint_provider_billed_bytes
            ),
            "close_complete": self.close_complete,
            "cleanup_complete": self.cleanup_complete,
        }


@dataclass(frozen=True)
class PaidGatewayBatchResponse:
    target_manifest_sha256: str
    results: tuple[PaidGatewayBatchItem, ...]
    route: TransportRoute
    receipt: PaidGatewayBatchReceipt


class PaidGatewayClient:
    """Runner-side client exposing exactly one bounded ``fetch`` operation.

    The request has no proxy URL, lease token, browser session identifier,
    method, arbitrary headers, cookies or JavaScript.  Those capabilities live
    only inside the isolated application gateway.
    """

    def __init__(
        self,
        gateway_url: str,
        *,
        token: Optional[str] = None,
        timeout: float = 75.0,
        session: Optional[requests.Session] = None,
    ) -> None:
        self.gateway_url = _credential_free_proxy_origin(
            gateway_url, label="paid gateway URL"
        )
        resolved_token = str(
            token if token is not None else os.environ.get(PAID_GATEWAY_TOKEN_ENV, "")
        ).strip()
        if len(resolved_token) < 32:
            raise ValueError(
                f"{PAID_GATEWAY_TOKEN_ENV} must contain at least 32 characters"
            )
        if (
            isinstance(timeout, bool)
            or not isinstance(timeout, (int, float))
            or not math.isfinite(float(timeout))
            or timeout <= 0
        ):
            raise ValueError("paid gateway timeout must be finite and positive")
        self._token = resolved_token
        self.timeout = float(timeout)
        self.session = session or requests.Session()
        self.session.trust_env = False
        self._receipt_lock = threading.Lock()
        self._receipt_bindings: OrderedDict[str, tuple[str, ...]] = OrderedDict()

    def _accept_receipt_once(self, receipt: PaidGatewayReceipt) -> None:
        """Reject replay or cross-context rebinding before local accounting."""

        binding = (
            "single",
            receipt.campaign_id,
            receipt.approval_id,
            receipt.approval_sha256,
            receipt.allocation_id,
            receipt.attempt_id_hash,
            receipt.canonical_url_sha256,
        )
        with self._receipt_lock:
            previous = self._receipt_bindings.get(receipt.lease_id_hash)
            if previous is not None:
                if previous == binding:
                    raise PaidGatewayProtocolError(
                        "paid gateway receipt was replayed"
                    )
                raise PaidGatewayProtocolError(
                    "paid gateway lease receipt was rebound"
                )
            self._receipt_bindings[receipt.lease_id_hash] = binding
            if len(self._receipt_bindings) > 4096:
                self._receipt_bindings.popitem(last=False)

    def _accept_batch_receipt_once(self, receipt: PaidGatewayBatchReceipt) -> None:
        binding = (
            "batch",
            receipt.campaign_id,
            receipt.approval_id,
            receipt.approval_sha256,
            receipt.allocation_id,
            receipt.attempt_id_hash,
            receipt.target_manifest_sha256,
        )
        with self._receipt_lock:
            previous = self._receipt_bindings.get(receipt.lease_id_hash)
            if previous is not None:
                if previous == binding:
                    raise PaidGatewayProtocolError(
                        "paid gateway batch receipt was replayed"
                    )
                raise PaidGatewayProtocolError(
                    "paid gateway lease receipt was rebound"
                )
            self._receipt_bindings[receipt.lease_id_hash] = binding
            if len(self._receipt_bindings) > 4096:
                self._receipt_bindings.popitem(last=False)

    @staticmethod
    def _bounded_response_body(response: object, limit: int) -> bytes:
        iterator = getattr(response, "iter_content", None)
        if callable(iterator):
            chunks: list[bytes] = []
            size = 0
            for raw_chunk in iterator(chunk_size=64 * 1024):
                chunk = bytes(raw_chunk or b"")
                size += len(chunk)
                if size > limit:
                    raise PaidGatewayProtocolError(
                        "paid gateway response document is oversized"
                    )
                chunks.append(chunk)
            return b"".join(chunks)
        body = bytes(getattr(response, "content", b"") or b"")
        if len(body) > limit:
            raise PaidGatewayProtocolError(
                "paid gateway response document is oversized"
            )
        return body

    def _request_timeout(self, timeout_ms: int) -> float:
        required = timeout_ms / 1000.0 + PAID_GATEWAY_CLEANUP_GRACE_SECONDS
        if self.timeout < required:
            raise ValueError(
                "paid gateway client timeout is shorter than operation deadline "
                "plus cleanup grace"
            )
        return required

    @staticmethod
    def _campaign_context_document(
        context: PaidCampaignContext | TransportContext,
    ) -> dict[str, object]:
        if isinstance(context, TransportContext):
            return PaidCampaignContext.from_transport_context(context).as_dict()
        if isinstance(context, PaidCampaignContext):
            return context.as_dict()
        raise TypeError("campaign context must be a PaidCampaignContext")

    def _post_gateway_control(
        self,
        *,
        path: str,
        document: Mapping[str, object],
        limit: int,
    ) -> Mapping[str, object]:
        response: object
        try:
            response = self.session.post(
                f"{self.gateway_url}{path}",
                data=canonical_json_bytes(dict(document)),
                headers={
                    "Authorization": f"Bearer {self._token}",
                    "Content-Type": "application/json",
                },
                timeout=self._request_timeout(60_000),
                stream=True,
            )
        except Exception as exc:
            raise PaidGatewayError(
                f"paid gateway control request failed: {type(exc).__name__}"
            ) from None
        try:
            body = self._bounded_response_body(response, limit)
            try:
                decoded = strict_json_loads(body.decode("utf-8"))
            except (
                UnicodeDecodeError,
                json.JSONDecodeError,
                ProxyCampaignValidationError,
            ):
                raise PaidGatewayProtocolError(
                    "paid gateway control response is not strict JSON"
                ) from None
            if not isinstance(decoded, Mapping):
                raise PaidGatewayProtocolError(
                    "paid gateway control response is not an object"
                )
            if canonical_json_bytes(dict(decoded)) != body:
                raise PaidGatewayProtocolError(
                    "paid gateway control response is not canonical JSON"
                )
            status = int(getattr(response, "status_code", 0) or 0)
            if status != 200:
                error = decoded.get("error")
                code = error.get("code") if isinstance(error, Mapping) else None
                if (
                    frozenset(decoded) != _PAID_GATEWAY_ERROR_FIELDS
                    or decoded.get("schema_version") != PAID_GATEWAY_SCHEMA_VERSION
                    or not isinstance(error, Mapping)
                    or frozenset(error) != {"code"}
                    or type(code) is not str
                    or _PAID_GATEWAY_ERROR_CODE_RE.fullmatch(code) is None
                ):
                    raise PaidGatewayProtocolError(
                        "paid gateway control rejection is invalid"
                    )
                raise PaidGatewayRejected(code)
            return dict(decoded)
        except (PaidGatewayRejected, PaidGatewayProtocolError):
            raise
        except Exception as exc:
            raise PaidGatewayError(
                f"paid gateway control response failed: {type(exc).__name__}"
            ) from None
        finally:
            close_response = getattr(response, "close", None)
            if callable(close_response):
                try:
                    close_response()
                except Exception:
                    pass

    def fetch(
        self,
        url: str,
        *,
        context: TransportContext,
        max_response_bytes: int,
        max_provider_bytes: int,
        timeout_ms: int,
        browser_bootstrap_url: Optional[str] = None,
    ) -> PaidGatewayResponse:
        for name, value, ceiling in (
            ("max_response_bytes", max_response_bytes, MAX_PAID_GATEWAY_RESPONSE_BYTES),
            ("max_provider_bytes", max_provider_bytes, 2_000_000),
            ("timeout_ms", timeout_ms, 60_000),
        ):
            if (
                isinstance(value, bool)
                or not isinstance(value, int)
                or not 1 <= value <= ceiling
            ):
                raise ValueError(f"{name} must be in 1..{ceiling}")
        request_document = {
            "schema_version": PAID_GATEWAY_SCHEMA_VERSION,
            "url": url,
            "browser_bootstrap_url": browser_bootstrap_url,
            "max_response_bytes": max_response_bytes,
            "max_provider_bytes": max_provider_bytes,
            "timeout_ms": timeout_ms,
            "context": context.as_dict(),
        }
        try:
            response = self.session.post(
                f"{self.gateway_url}/v1/fetch",
                json=request_document,
                headers={"Authorization": f"Bearer {self._token}"},
                timeout=self._request_timeout(timeout_ms),
                stream=True,
            )
        except Exception as exc:
            raise PaidGatewayError(
                f"paid gateway request failed: {type(exc).__name__}"
            ) from None
        try:
            return self._decode_fetch_response(
                response,
                url=url,
                context=context,
                max_response_bytes=max_response_bytes,
                max_provider_bytes=max_provider_bytes,
            )
        except (PaidGatewayRejected, PaidGatewayProtocolError):
            raise
        except Exception as exc:
            raise PaidGatewayError(
                f"paid gateway response failed: {type(exc).__name__}"
            ) from None
        finally:
            close_response = getattr(response, "close", None)
            if callable(close_response):
                try:
                    close_response()
                except Exception:
                    pass

    def fetch_batch(
        self,
        urls: Sequence[str],
        *,
        context: TransportContext,
        max_response_bytes: int,
        max_provider_bytes: int,
        timeout_ms: int,
        browser_bootstrap_url: str,
    ) -> PaidGatewayBatchResponse:
        """Fetch one atomic structured-feed manifest through one paid lease."""

        items = tuple(urls)
        if not 1 <= len(items) <= MAX_PAID_GATEWAY_BATCH_URLS:
            raise ValueError(
                f"paid gateway batch must contain 1..{MAX_PAID_GATEWAY_BATCH_URLS} URLs"
            )
        if any(
            type(url) is not str
            or url != _canonical_url_key(url)
            or not _is_whoscored_structured_feed_url(url)
            for url in items
        ) or len(set(items)) != len(items):
            raise ValueError(
                "paid gateway batch URLs must be unique canonical structured feeds"
            )
        if not _is_whoscored_stage_bootstrap_url(browser_bootstrap_url):
            raise ValueError("paid gateway batch bootstrap URL is invalid")
        for name, value, ceiling in (
            ("max_response_bytes", max_response_bytes, MAX_PAID_GATEWAY_RESPONSE_BYTES),
            (
                "max_provider_bytes",
                max_provider_bytes,
                2_000_000,
            ),
            ("timeout_ms", timeout_ms, 60_000),
        ):
            if (
                isinstance(value, bool)
                or not isinstance(value, int)
                or not 1 <= value <= ceiling
            ):
                raise ValueError(f"{name} must be in 1..{ceiling}")
        manifest = _paid_gateway_target_manifest_sha256(
            items, browser_bootstrap_url=browser_bootstrap_url
        )
        document = {
            "schema_version": PAID_GATEWAY_SCHEMA_VERSION,
            "urls": list(items),
            "browser_bootstrap_url": browser_bootstrap_url,
            "target_manifest_sha256": manifest,
            "max_response_bytes": max_response_bytes,
            "max_provider_bytes": max_provider_bytes,
            "timeout_ms": timeout_ms,
            "context": context.as_dict(),
        }
        try:
            response = self.session.post(
                f"{self.gateway_url}/v1/fetch-batch",
                json=document,
                headers={"Authorization": f"Bearer {self._token}"},
                timeout=self._request_timeout(timeout_ms),
                stream=True,
            )
        except Exception as exc:
            raise PaidGatewayError(
                f"paid gateway batch request failed: {type(exc).__name__}"
            ) from None
        try:
            return self._decode_fetch_batch_response(
                response,
                urls=items,
                context=context,
                max_response_bytes=max_response_bytes,
                max_provider_bytes=max_provider_bytes,
                target_manifest_sha256=manifest,
                browser_bootstrap_url=browser_bootstrap_url,
            )
        except (PaidGatewayRejected, PaidGatewayProtocolError):
            raise
        except Exception as exc:
            raise PaidGatewayError(
                f"paid gateway batch response failed: {type(exc).__name__}"
            ) from None
        finally:
            close_response = getattr(response, "close", None)
            if callable(close_response):
                try:
                    close_response()
                except Exception:
                    pass

    def _decode_fetch_batch_response(
        self,
        response: object,
        *,
        urls: Sequence[str],
        context: TransportContext,
        max_response_bytes: int,
        max_provider_bytes: int,
        target_manifest_sha256: str,
        browser_bootstrap_url: str,
    ) -> PaidGatewayBatchResponse:
        status = int(getattr(response, "status_code", 0) or 0)
        body = self._bounded_response_body(
            response, MAX_PAID_GATEWAY_BATCH_RESPONSE_DOCUMENT_BYTES
        )
        try:
            decoded = strict_json_loads(body.decode("utf-8"))
        except (UnicodeDecodeError, json.JSONDecodeError, ProxyCampaignValidationError):
            raise PaidGatewayProtocolError(
                "paid gateway batch response is not strict JSON"
            ) from None
        if status != 200:
            if not isinstance(decoded, Mapping) or frozenset(decoded) not in {
                _PAID_GATEWAY_ERROR_FIELDS,
                _PAID_GATEWAY_SETTLED_ERROR_FIELDS,
            } or decoded.get("schema_version") != PAID_GATEWAY_SCHEMA_VERSION:
                raise PaidGatewayProtocolError(
                    "paid gateway batch error response fields are invalid"
                )
            error = decoded.get("error")
            code = error.get("code") if isinstance(error, Mapping) else None
            if (
                not isinstance(error, Mapping)
                or frozenset(error) != {"code"}
                or type(code) is not str
                or _PAID_GATEWAY_ERROR_CODE_RE.fullmatch(code) is None
            ):
                raise PaidGatewayProtocolError(
                    "paid gateway batch error response is invalid"
                )
            receipt = (
                PaidGatewayBatchReceipt.from_dict(
                    decoded.get("receipt"),
                    context=context,
                    urls=urls,
                    browser_bootstrap_url=browser_bootstrap_url,
                    max_provider_bytes=max_provider_bytes,
                )
                if "receipt" in decoded
                else None
            )
            if receipt is not None:
                self._accept_batch_receipt_once(receipt)
            raise PaidGatewayRejected(code, receipt=receipt)
        if (
            not isinstance(decoded, Mapping)
            or frozenset(decoded) != _PAID_GATEWAY_BATCH_RESPONSE_FIELDS
            or decoded.get("schema_version") != PAID_GATEWAY_SCHEMA_VERSION
            or decoded.get("target_manifest_sha256") != target_manifest_sha256
            or decoded.get("route") != TransportRoute.PAID_FLARESOLVERR.value
        ):
            raise PaidGatewayProtocolError(
                "paid gateway batch response envelope is invalid"
            )
        raw_results = decoded.get("results")
        if not isinstance(raw_results, list) or len(raw_results) != len(urls):
            raise PaidGatewayProtocolError(
                "paid gateway batch result count is invalid"
            )
        results: list[PaidGatewayBatchItem] = []
        for expected_url, raw in zip(urls, raw_results):
            if (
                not isinstance(raw, Mapping)
                or frozenset(raw) != _PAID_GATEWAY_BATCH_ITEM_FIELDS
                or raw.get("url") != expected_url
            ):
                raise PaidGatewayProtocolError(
                    "paid gateway batch item identity is invalid"
                )
            status_code = raw.get("status_code")
            if (
                isinstance(status_code, bool)
                or not isinstance(status_code, int)
                or not 100 <= status_code <= 599
            ):
                raise PaidGatewayProtocolError(
                    "paid gateway batch item status is invalid"
                )
            encoded = raw.get("body_base64")
            try:
                content = base64.b64decode(encoded, validate=True)
            except (TypeError, binascii.Error, ValueError):
                raise PaidGatewayProtocolError(
                    "paid gateway batch item body is invalid"
                ) from None
            if len(content) > max_response_bytes or raw.get(
                "body_sha256"
            ) != hashlib.sha256(content).hexdigest():
                raise PaidGatewayProtocolError(
                    "paid gateway batch item body integrity is invalid"
                )
            raw_headers = raw.get("headers")
            if not isinstance(raw_headers, Mapping) or len(raw_headers) > 32:
                raise PaidGatewayProtocolError(
                    "paid gateway batch item headers are invalid"
                )
            headers: dict[str, str] = {}
            for key, value in raw_headers.items():
                if (
                    type(key) is not str
                    or type(value) is not str
                    or not key
                    or len(key) > 128
                    or len(value) > 4096
                    or any(character in key + value for character in "\r\n")
                ):
                    raise PaidGatewayProtocolError(
                        "paid gateway batch item headers are invalid"
                    )
                headers[key] = value
            results.append(
                PaidGatewayBatchItem(
                    url=expected_url,
                    content=content,
                    status_code=status_code,
                    headers=headers,
                )
            )
        receipt = PaidGatewayBatchReceipt.from_dict(
            decoded.get("receipt"),
            context=context,
            urls=urls,
            browser_bootstrap_url=browser_bootstrap_url,
            max_provider_bytes=max_provider_bytes,
        )
        if receipt.target_manifest_sha256 != target_manifest_sha256:
            raise PaidGatewayProtocolError(
                "paid gateway batch receipt and response manifest differ"
            )
        self._accept_batch_receipt_once(receipt)
        return PaidGatewayBatchResponse(
            target_manifest_sha256=target_manifest_sha256,
            results=tuple(results),
            route=TransportRoute.PAID_FLARESOLVERR,
            receipt=receipt,
        )

    def _decode_fetch_response(
        self,
        response: object,
        *,
        url: str,
        context: TransportContext,
        max_response_bytes: int,
        max_provider_bytes: int,
    ) -> PaidGatewayResponse:
        status = int(getattr(response, "status_code", 0) or 0)
        document_limit = max(
            64 * 1024,
            min(
                MAX_PAID_GATEWAY_RESPONSE_DOCUMENT_BYTES,
                (max_response_bytes * 4 // 3) + 128 * 1024,
            ),
        )
        body = self._bounded_response_body(response, document_limit)
        try:
            decoded = strict_json_loads(body.decode("utf-8"))
        except (UnicodeDecodeError, json.JSONDecodeError, ProxyCampaignValidationError):
            raise PaidGatewayProtocolError(
                "paid gateway response is not strict JSON"
            ) from None
        if status != 200:
            if not isinstance(decoded, Mapping) or frozenset(decoded) not in {
                _PAID_GATEWAY_ERROR_FIELDS,
                _PAID_GATEWAY_SETTLED_ERROR_FIELDS,
            } or decoded.get("schema_version") != PAID_GATEWAY_SCHEMA_VERSION:
                raise PaidGatewayProtocolError(
                    "paid gateway error response fields are invalid"
                )
            error = decoded.get("error")
            code = error.get("code") if isinstance(error, Mapping) else None
            if (
                not isinstance(error, Mapping)
                or frozenset(error) != {"code"}
                or type(code) is not str
                or _PAID_GATEWAY_ERROR_CODE_RE.fullmatch(code) is None
            ):
                raise PaidGatewayProtocolError(
                    "paid gateway error response is invalid"
                )
            receipt = (
                PaidGatewayReceipt.from_dict(
                    decoded.get("receipt"),
                    context=context,
                    url=url,
                    max_provider_bytes=max_provider_bytes,
                )
                if "receipt" in decoded
                else None
            )
            if receipt is not None:
                self._accept_receipt_once(receipt)
            raise PaidGatewayRejected(code, receipt=receipt)
        if not isinstance(decoded, Mapping) or frozenset(decoded) != (
            _PAID_GATEWAY_RESPONSE_FIELDS
        ):
            raise PaidGatewayProtocolError("paid gateway response fields are invalid")
        if decoded.get("schema_version") != PAID_GATEWAY_SCHEMA_VERSION:
            raise PaidGatewayProtocolError("paid gateway response schema is invalid")
        if decoded.get("url") != url:
            raise PaidGatewayProtocolError("paid gateway returned another URL")
        status_code = decoded.get("status_code")
        if (
            isinstance(status_code, bool)
            or not isinstance(status_code, int)
            or not 100 <= status_code <= 599
        ):
            raise PaidGatewayProtocolError(
                "paid gateway source status is invalid"
            )
        encoded = decoded.get("body_base64")
        if type(encoded) is not str:
            raise PaidGatewayProtocolError("paid gateway body is missing")
        try:
            content = base64.b64decode(encoded, validate=True)
        except (binascii.Error, ValueError):
            raise PaidGatewayProtocolError(
                "paid gateway body is not valid base64"
            ) from None
        if len(content) > max_response_bytes:
            raise PaidGatewayProtocolError("paid gateway source body is oversized")
        body_sha256 = decoded.get("body_sha256")
        if (
            type(body_sha256) is not str
            or _LOWER_SHA256_RE.fullmatch(body_sha256) is None
            or not hmac.compare_digest(
                body_sha256, hashlib.sha256(content).hexdigest()
            )
        ):
            raise PaidGatewayProtocolError("paid gateway body digest is invalid")
        raw_headers = decoded.get("headers")
        if not isinstance(raw_headers, Mapping) or len(raw_headers) > 32:
            raise PaidGatewayProtocolError("paid gateway headers are invalid")
        headers: dict[str, str] = {}
        for key, value in raw_headers.items():
            if (
                type(key) is not str
                or type(value) is not str
                or not key
                or len(key) > 128
                or len(value) > 4096
                or "\r" in key
                or "\n" in key
                or "\r" in value
                or "\n" in value
            ):
                raise PaidGatewayProtocolError("paid gateway headers are invalid")
            headers[key] = value
        receipt = PaidGatewayReceipt.from_dict(
            decoded.get("receipt"),
            context=context,
            url=url,
            max_provider_bytes=max_provider_bytes,
        )
        route_value = decoded.get("route")
        if route_value != receipt.route.value:
            raise PaidGatewayProtocolError(
                "paid gateway response and receipt routes differ"
            )
        self._accept_receipt_once(receipt)
        return PaidGatewayResponse(
            url=url,
            content=content,
            status_code=status_code,
            headers=headers,
            route=receipt.route,
            receipt=receipt,
        )

    def preflight_alert(
        self, *, context: PaidCampaignContext | TransportContext
    ) -> Mapping[str, str]:
        """Ask the gateway to deliver/deduplicate its own paid-alert proof."""

        campaign = self._campaign_context_document(context)
        document = {
            "schema_version": PAID_GATEWAY_SCHEMA_VERSION,
            "context": campaign,
        }
        decoded = self._post_gateway_control(
            path="/v1/preflight-alert",
            document=document,
            limit=64 * 1024,
        )
        expected = {
            "schema_version",
            "status",
            "campaign_id",
            "approval_id",
            "approval_sha256",
        }
        if (
            frozenset(decoded) != expected
            or decoded.get("schema_version") != PAID_GATEWAY_SCHEMA_VERSION
            or decoded.get("status") != "delivered"
            or decoded.get("campaign_id") != campaign.get("proxy_campaign_id")
            or decoded.get("approval_id") != campaign.get("proxy_approval_id")
            or decoded.get("approval_sha256")
            != campaign.get("proxy_approval_sha256")
        ):
            raise PaidGatewayProtocolError(
                "paid gateway alert response is invalid"
            )
        return {key: str(decoded[key]) for key in expected if key != "schema_version"}

    def _campaign_control(
        self,
        operation: str,
        *,
        context: PaidCampaignContext | TransportContext,
        arguments: Mapping[str, object],
    ) -> Mapping[str, object]:
        expected_arguments = PROXY_CAMPAIGN_CONTROL_ARGUMENT_FIELDS.get(operation)
        if expected_arguments is None or frozenset(arguments) != expected_arguments:
            raise ValueError("campaign control operation or arguments are invalid")
        document = {
            "schema_version": PROXY_CAMPAIGN_CONTROL_SCHEMA_VERSION,
            "operation": operation,
            "context": self._campaign_context_document(context),
            "arguments": dict(arguments),
        }
        decoded = self._post_gateway_control(
            path="/v1/campaign-control",
            document=document,
            limit=MAX_PAID_GATEWAY_CONTROL_DOCUMENT_BYTES,
        )
        if (
            frozenset(decoded) != {"schema_version", "operation", "result"}
            or decoded.get("schema_version")
            != PROXY_CAMPAIGN_CONTROL_SCHEMA_VERSION
            or decoded.get("operation") != operation
        ):
            raise PaidGatewayProtocolError(
                "paid gateway campaign control envelope is invalid"
            )
        result = decoded.get("result")
        if (
            not isinstance(result, Mapping)
            or frozenset(result) != PROXY_CAMPAIGN_CONTROL_RESULT_FIELDS[operation]
        ):
            raise PaidGatewayProtocolError(
                "paid gateway campaign control result is invalid"
            )
        return dict(result)

    def snapshot(
        self, *, context: PaidCampaignContext | TransportContext
    ) -> Mapping[str, object]:
        result = self._campaign_control("snapshot", context=context, arguments={})
        campaign = result.get("campaign")
        if not isinstance(campaign, Mapping):
            raise PaidGatewayProtocolError("campaign snapshot is invalid")
        return dict(campaign)

    def complete_allocation(
        self,
        *,
        context: PaidCampaignContext | TransportContext,
        allocation_id: str,
        dag_id: str,
        run_id: str,
        task_id: str,
        attempt_id: str,
        report_sha256: str,
        request_ledger_sha256: str,
    ) -> Mapping[str, object]:
        result = self._campaign_control(
            "complete_allocation",
            context=context,
            arguments={
                "allocation_id": allocation_id,
                "dag_id": dag_id,
                "run_id": run_id,
                "task_id": task_id,
                "attempt_id": attempt_id,
                "report_sha256": report_sha256,
                "request_ledger_sha256": request_ledger_sha256,
            },
        )
        allocation = result.get("allocation")
        if not isinstance(allocation, Mapping):
            raise PaidGatewayProtocolError("campaign allocation result is invalid")
        return dict(allocation)

    def assert_exact_accounting(
        self,
        *,
        context: PaidCampaignContext | TransportContext,
        task_report_provider_bytes: int,
        request_ledger_provider_bytes: int,
        proxy_ledger_provider_bytes: int,
        require_complete: bool = False,
    ) -> int:
        result = self._campaign_control(
            "assert_exact_accounting",
            context=context,
            arguments={
                "task_report_provider_bytes": task_report_provider_bytes,
                "request_ledger_provider_bytes": request_ledger_provider_bytes,
                "proxy_ledger_provider_bytes": proxy_ledger_provider_bytes,
                "require_complete": require_complete,
            },
        )
        billed = result.get("provider_billed_bytes")
        if isinstance(billed, bool) or not isinstance(billed, int) or billed < 0:
            raise PaidGatewayProtocolError("campaign accounting result is invalid")
        return billed

    def seal_for_reconciliation(
        self,
        *,
        context: PaidCampaignContext | TransportContext,
        dag_id: str,
        run_id: str,
        provider_billed_bytes: int,
        attempt_accounting_sha256: str,
    ) -> Mapping[str, object]:
        result = self._campaign_control(
            "seal_for_reconciliation",
            context=context,
            arguments={
                "dag_id": dag_id,
                "run_id": run_id,
                "provider_billed_bytes": provider_billed_bytes,
                "attempt_accounting_sha256": attempt_accounting_sha256,
            },
        )
        campaign = result.get("campaign")
        if not isinstance(campaign, Mapping):
            raise PaidGatewayProtocolError("campaign seal result is invalid")
        return dict(campaign)

    def sealed_snapshot(
        self, *, context: PaidCampaignContext | TransportContext
    ) -> Mapping[str, object]:
        result = self._campaign_control(
            "sealed_snapshot", context=context, arguments={}
        )
        campaign = result.get("campaign")
        if not isinstance(campaign, Mapping):
            raise PaidGatewayProtocolError("sealed campaign snapshot is invalid")
        return dict(campaign)

    def close(self) -> None:
        self.session.close()


def _proxy_control_token_from_environment() -> str:
    """Resolve only the dedicated WhoScored lease/HMAC runtime secret."""

    return str(os.environ.get("WHOSCORED_PROXY_CONTROL_TOKEN", "")).strip()


def _credential_free_proxy_origin(value: object, *, label: str) -> str:
    """Return one safe HTTP(S) proxy origin without ever reflecting its input."""

    error = f"invalid {label}"
    if type(value) is not str or not value or value != value.strip():
        raise ValueError(error)
    try:
        parts = urlsplit(value)
        hostname = parts.hostname
        port = parts.port
        username = parts.username
        password = parts.password
    except (TypeError, ValueError):
        raise ValueError(error) from None
    if (
        parts.scheme.lower() not in {"http", "https"}
        or not hostname
        or username is not None
        or password is not None
        or "@" in parts.netloc
        or "%" in parts.netloc
        or parts.path not in {"", "/"}
        or bool(parts.query)
        or bool(parts.fragment)
        or port is not None
        and not 1 <= port <= 65535
    ):
        raise ValueError(error)
    host = hostname
    if ":" in host and not host.startswith("["):
        host = f"[{host}]"
    if port is not None:
        host = f"{host}:{port}"
    return urlunsplit((parts.scheme.lower(), host, "", "", ""))


def _proxy_url_with_lease(proxy_url: str, token: str) -> str:
    origin = _credential_free_proxy_origin(
        proxy_url, label="filtering proxy lease URL"
    )
    parts = urlsplit(origin)
    host = parts.hostname
    assert host is not None
    if ":" in host and not host.startswith("["):
        host = f"[{host}]"
    if parts.port:
        host = f"{host}:{parts.port}"
    netloc = f"lease:{quote(token, safe='')}@{host}"
    return urlunsplit((parts.scheme, netloc, parts.path, parts.query, parts.fragment))


def _safe_route_exception_text(
    exc: BaseException, *, route: TransportRoute
) -> str:
    """Keep credentials out of every paid-route error string."""

    if route in _PAID_ROUTES:
        return type(exc).__name__
    return str(exc)


_CF_STRONG_BODY_MARKERS = (
    "<title>just a moment",
    "<title>attention required! | cloudflare</title>",
    "/cdn-cgi/challenge-platform/",
    "cf-browser-verification",
    "challenge-running",
    "cf_chl_",
    "cf-chl-",
)
_CF_ORIGIN_ERROR_TITLE = re.compile(
    r"<title>\s*whoscored\.com\s*\|\s*(50[0-9])\s*:[^<]*</title>",
    re.IGNORECASE,
)

_WHOSCORED_STRUCTURED_FEED_PATHS = (
    re.compile(r"\A/statisticsfeed/1/get(?:team|player)statistics\Z"),
    re.compile(r"\A/stagestatfeed/[1-9][0-9]*/stageteams/\Z"),
)
_WHOSCORED_STAGE_BOOTSTRAP_PATH = re.compile(
    r"\A/Regions/[1-9][0-9]*/Tournaments/[1-9][0-9]*/"
    r"Seasons/[1-9][0-9]*/Stages/[1-9][0-9]*/TeamStatistics\Z"
)


def _is_whoscored_structured_feed_url(url: str) -> bool:
    parts = urlsplit(url)
    return bool(
        parts.scheme.lower() == "https"
        and parts.hostname == "www.whoscored.com"
        and parts.port is None
        and parts.username is None
        and parts.password is None
        and not parts.fragment
        and any(
            pattern.fullmatch(parts.path)
            for pattern in _WHOSCORED_STRUCTURED_FEED_PATHS
        )
    )


def _is_whoscored_stage_bootstrap_url(url: str) -> bool:
    parts = urlsplit(url)
    return bool(
        parts.scheme.lower() == "https"
        and parts.hostname == "www.whoscored.com"
        and parts.port is None
        and parts.username is None
        and parts.password is None
        and not parts.query
        and not parts.fragment
        and _WHOSCORED_STAGE_BOOTSTRAP_PATH.fullmatch(parts.path)
    )


def is_whoscored_structured_feed_access_gate(
    url: str,
    status_code: int,
    content: bytes | str,
    headers: Optional[Mapping[str, str]] = None,
) -> bool:
    """Detect WhoScored's anti-automation shell, narrowly.

    The structured-feed origin returns a tiny generic 404 document instead of
    JSON when an XHR lacks the browser execution context. Depending on which
    source cookies are already present, the same shell is either returned as
    HTTP 200 or as a no-follow HTTP 302 to the exact same-origin ``404.html``
    path. It is eligible for direct FlareSolverr only when the source-specific
    body markers are present and, for 302, the location names the requested
    allow-listed feed path. An arbitrary redirect/parser rejection never
    enables browser or paid fallback.
    """

    if status_code not in {200, 302} or not _is_whoscored_structured_feed_url(url):
        return False
    body = (
        content.decode("utf-8", errors="ignore")
        if isinstance(content, bytes)
        else content
    )
    lowered = body[:100_000].lower()
    verify_marker = "verify-client.js" in lowered
    if status_code == 200:
        return verify_marker and "page you requested does not exist" in lowered
    if not verify_marker:
        return False

    lowered_headers = {
        str(key).lower(): str(value) for key, value in (headers or {}).items()
    }
    location = lowered_headers.get("location", "")
    if not location:
        return False
    requested = urlsplit(url)
    redirected = urlsplit(urljoin("https://www.whoscored.com", location))
    if (
        redirected.scheme.lower() != "https"
        or redirected.hostname != "www.whoscored.com"
        or redirected.port is not None
        or redirected.username is not None
        or redirected.password is not None
        or redirected.path != "/404.html"
        or redirected.fragment
    ):
        return False
    query = parse_qsl(redirected.query, keep_blank_values=True)
    return query == [("aspxerrorpath", requested.path)]


def is_cloudflare_response(
    status_code: int,
    headers: Mapping[str, str],
    content: bytes | str,
) -> bool:
    """Classify a response narrowly enough that ordinary errors never buy traffic."""

    lowered_headers = {str(k).lower(): str(v).lower() for k, v in headers.items()}
    body = (
        content.decode("utf-8", errors="ignore")
        if isinstance(content, bytes)
        else content
    )
    body = body[:250_000].lower()
    marker = any(value in body for value in _CF_STRONG_BODY_MARKERS)
    cf_header = bool(lowered_headers.get("cf-ray")) or (
        "cloudflare" in lowered_headers.get("server", "")
    )
    if status_code == 200:
        # FlareSolverr may return a rendered interstitial as HTTP 200 without
        # source headers. Only source-specific challenge markup is accepted.
        return marker
    # WhoScored is fronted by Cloudflare even for ordinary origin errors.
    # Paid fallback requires both independently useful signals: a supported
    # challenge status/header and the actual challenge document.
    return status_code in (403, 429, 503) and cf_header and marker


def _has_cloudflare_challenge_markup(content: bytes | str) -> bool:
    body = (
        content.decode("utf-8", errors="ignore")
        if isinstance(content, bytes)
        else content
    )
    body = body[:250_000].lower()
    return any(value in body for value in _CF_STRONG_BODY_MARKERS)


def _cloudflare_origin_error_status(content: bytes | str) -> Optional[int]:
    """Recover an origin 5xx that FlareSolverr reports as HTTP 200.

    FlareSolverr returns the rendered document status, while Cloudflare's
    branded origin-error document can therefore arrive as 200.  Match only
    WhoScored's exact error title so an application page containing an
    arbitrary ``502`` string is never reclassified.
    """

    body = (
        content.decode("utf-8", errors="ignore")
        if isinstance(content, bytes)
        else content
    )
    match = _CF_ORIGIN_ERROR_TITLE.search(body[:16_384])
    return int(match.group(1)) if match is not None else None


Validator = Callable[[TransportResponse], Optional[bool]]


@dataclass(frozen=True)
class FetchRequest:
    """One logical URL in a bounded structured-feed batch."""

    url: str
    cache_key: str
    validator: Optional[Validator] = None
    scope: Optional[str] = None
    entity: Optional[str] = None
    before_network: Optional[Callable[[], None]] = None
    browser_bootstrap_url: Optional[str] = None


@dataclass
class _BrowserSession:
    session_id: str
    proxy_url: Optional[str]
    created_at: float
    requests: int = 0
    bootstrap_url: str = ""


@dataclass(frozen=True)
class _BrowserBatchOutcome:
    response: Optional[TransportResponse] = None
    error: Optional[WhoScoredTransportError] = None

    def __post_init__(self) -> None:
        if (self.response is None) == (self.error is None):
            raise ValueError("browser batch outcome requires one response or error")


class WhoScoredTransport:
    """Direct-first WhoScored fetcher with typed failure and traffic accounting."""

    def __init__(
        self,
        *,
        flaresolverr_url: str = "http://flaresolverr:8191",
        paid_proxy_url: Optional[str] = None,
        proxy_control_url: Optional[str] = None,
        paid_gateway_url: Optional[str] = None,
        raw_cache: Optional[RawCacheHook] = None,
        budgets: Optional[TransportBudgets] = None,
        request_timeout: float = 30.0,
        browser_timeout_ms: int = 60_000,
        direct_http_attempts: int = 3,
        direct_http_retry_backoff_seconds: float = (
            DEFAULT_DIRECT_HTTP_RETRY_BACKOFF_SECONDS
        ),
        direct_browser_attempts: int = DEFAULT_DIRECT_BROWSER_ATTEMPTS,
        browser_retry_backoff_seconds: float = (DEFAULT_BROWSER_RETRY_BACKOFF_SECONDS),
        browser_retry_jitter_seconds: float = DEFAULT_BROWSER_RETRY_JITTER_SECONDS,
        impersonate: str = "chrome120",
        direct_http_session: Any = None,
        direct_fs_client: Optional[FlareSolverrClient] = None,
        paid_fs_client: Optional[FlareSolverrClient] = None,
        proxy_client: Optional[ProxyFilterClient] = None,
        paid_gateway_client: Optional[PaidGatewayClient] = None,
        http_session_factory: Optional[Callable[[Optional[str]], Any]] = None,
        context: Optional[TransportContext] = None,
        request_ledger: Optional[RequestLedger] = None,
        browser_session_ttl_seconds: int = 300,
        browser_session_max_requests: int = 96,
        browser_session_owner: Optional[str] = None,
        source_circuit: Optional[SharedSourceCircuit] = None,
        source_circuit_wait: Optional[bool] = None,
        transport_policy: Optional[TransportPolicy | str] = None,
        paid_batch_enabled: Optional[bool] = None,
    ) -> None:
        require_production_runtime_class(operation="WhoScored source transport")
        if direct_http_attempts < 1:
            raise ValueError("direct_http_attempts must be >= 1")
        if direct_browser_attempts < 1:
            raise ValueError("direct_browser_attempts must be >= 1")
        if (
            isinstance(direct_http_retry_backoff_seconds, bool)
            or not isinstance(direct_http_retry_backoff_seconds, (int, float))
            or not math.isfinite(float(direct_http_retry_backoff_seconds))
            or direct_http_retry_backoff_seconds < 0
        ):
            raise ValueError(
                "direct_http_retry_backoff_seconds must be finite and >= 0"
            )
        if (
            isinstance(browser_retry_backoff_seconds, bool)
            or not isinstance(browser_retry_backoff_seconds, (int, float))
            or not math.isfinite(float(browser_retry_backoff_seconds))
            or browser_retry_backoff_seconds < 0
        ):
            raise ValueError("browser_retry_backoff_seconds must be finite and >= 0")
        if (
            isinstance(browser_retry_jitter_seconds, bool)
            or not isinstance(browser_retry_jitter_seconds, (int, float))
            or not math.isfinite(float(browser_retry_jitter_seconds))
            or browser_retry_jitter_seconds < 0
        ):
            raise ValueError("browser_retry_jitter_seconds must be finite and >= 0")
        supervisor_owner = os.environ.get(SUPERVISOR_SESSION_OWNER_ENV, "").strip()
        supervisor_ledger_path = os.environ.get(
            SUPERVISOR_RESOURCE_LEDGER_ENV, ""
        ).strip()
        if bool(supervisor_owner) != bool(supervisor_ledger_path):
            raise ValueError(
                "supervised browser session owner and resource ledger must be "
                "configured together"
            )
        if supervisor_owner and browser_session_owner is not None:
            raise ValueError(
                "supervised and capacity browser session ownership cannot be mixed"
            )
        resolved_session_owner = supervisor_owner or browser_session_owner
        browser_session_prefix = (
            capacity_browser_session_prefix(resolved_session_owner)
            if resolved_session_owner is not None
            else None
        )
        if source_circuit_wait is None:
            wait_value = os.environ.get(SOURCE_CIRCUIT_WAIT_ENV, "0").strip()
            if wait_value not in {"0", "1"}:
                raise ValueError(f"{SOURCE_CIRCUIT_WAIT_ENV} must be 0 or 1")
            source_circuit_wait = wait_value == "1"
        elif type(source_circuit_wait) is not bool:
            raise ValueError("source_circuit_wait must be a boolean")
        circuit_path = os.environ.get(SOURCE_CIRCUIT_PATH_ENV, "").strip()
        if source_circuit is None and circuit_path:
            source_circuit = SharedSourceCircuit(circuit_path)
        if source_circuit_wait and source_circuit is None:
            raise ValueError(
                f"{SOURCE_CIRCUIT_WAIT_ENV}=1 requires {SOURCE_CIRCUIT_PATH_ENV}"
            )
        if paid_batch_enabled is None:
            paid_batch_value = os.environ.get(
                "WHOSCORED_PAID_BATCH_ENABLED", "0"
            ).strip()
            if paid_batch_value not in {"0", "1"}:
                raise ValueError("WHOSCORED_PAID_BATCH_ENABLED must be 0 or 1")
            paid_batch_enabled = paid_batch_value == "1"
        elif type(paid_batch_enabled) is not bool:
            raise ValueError("paid_batch_enabled must be a boolean")
        self.context = context or TransportContext.from_env()
        context_policy = TransportPolicy.parse(self.context.transport_policy)
        if transport_policy is not None:
            requested_policy = TransportPolicy.parse(transport_policy)
            if requested_policy is not context_policy:
                raise ValueError(
                    "transport_policy differs from the authenticated TransportContext"
                )
        resolved_policy = context_policy
        if resolved_policy is TransportPolicy.DIRECT_THEN_PAID:
            assert_paid_runtime_available(self.context.as_dict())
            if any(
                value is not None
                for value in (
                    paid_proxy_url,
                    proxy_control_url,
                    paid_fs_client,
                    proxy_client,
                )
            ):
                raise ValueError(
                    "WhoScored paid runners may configure only the isolated "
                    "paid application gateway"
                )
            if paid_gateway_client is None and not paid_gateway_url:
                raise ValueError(
                    "transport_policy=direct_then_paid requires the isolated "
                    "paid application gateway"
                )
        self.transport_policy = resolved_policy
        self.raw_cache = raw_cache
        self.budgets = budgets or TransportBudgets()
        signed_allocation = self.context.proxy_campaign.get("proxy_allocation")
        if budgets is None and isinstance(signed_allocation, Mapping):
            allocation_budget = signed_allocation.get("budget_bytes")
            request_limit = signed_allocation.get("request_limit")
            lease_limit = signed_allocation.get("lease_limit")
            if any(
                isinstance(value, bool) or not isinstance(value, int) or value <= 0
                for value in (allocation_budget, request_limit, lease_limit)
            ):
                raise ValueError("signed proxy allocation limits are invalid")
            assert isinstance(allocation_budget, int)
            assert isinstance(request_limit, int)
            assert isinstance(lease_limit, int)
            self.budgets = TransportBudgets(
                max_response_bytes=self.budgets.max_response_bytes,
                max_paid_bytes_per_url=self.budgets.max_paid_bytes_per_url,
                max_paid_bytes_per_lease=self.budgets.max_paid_bytes_per_lease,
                max_paid_bytes_per_task=allocation_budget,
                max_paid_urls=request_limit,
                max_paid_browser_bootstraps=lease_limit,
                lease_ttl_seconds=self.budgets.lease_ttl_seconds,
            )
        self.request_timeout = request_timeout
        self.browser_timeout_ms = browser_timeout_ms
        self.direct_http_attempts = direct_http_attempts
        self.direct_http_retry_backoff_seconds = float(
            direct_http_retry_backoff_seconds
        )
        self.direct_browser_attempts = direct_browser_attempts
        self.browser_retry_backoff_seconds = float(browser_retry_backoff_seconds)
        self.browser_retry_jitter_seconds = float(browser_retry_jitter_seconds)
        self.impersonate = impersonate
        # Runner processes never receive a filtering-proxy origin or any
        # short-lived lease/session capability.  The legacy constructor values
        # remain accepted for direct-only compatibility but cannot authorize a
        # WhoScored paid route.
        self.paid_proxy_url = None
        ledger_path = os.environ.get("WHOSCORED_REQUEST_LEDGER_PATH", "").strip()
        self.request_ledger = request_ledger or (
            JsonlRequestLedger(ledger_path) if ledger_path else None
        )
        self.browser_session_ttl_seconds = max(1, browser_session_ttl_seconds)
        self.browser_session_max_requests = max(1, browser_session_max_requests)
        self._capacity_browser_session_prefix = browser_session_prefix
        self._supervisor_session_owner = supervisor_owner
        self._supervisor_resource_ledger = (
            JsonlRequestLedger(supervisor_ledger_path)
            if supervisor_ledger_path
            else None
        )
        self._source_circuit = source_circuit
        self._source_circuit_wait = source_circuit_wait
        self._paid_batch_enabled = paid_batch_enabled
        self._source_circuit_permit: Optional[CircuitPermit] = None
        self._http_session_factory = http_session_factory
        self._pool_proxy_url = self._resolve_pool_proxy_url()
        self._direct_http = direct_http_session or self._new_http_session(
            self._pool_proxy_url
        )
        flaresolverr_identity = _attested_flaresolverr_identity()
        identity_kwargs = (
            {
                "expected_version": flaresolverr_identity[0],
                "expected_extension_sha256": flaresolverr_identity[1],
            }
            if flaresolverr_identity is not None
            else {}
        )
        self._direct_fs = direct_fs_client or FlareSolverrClient(
            url=flaresolverr_url,
            **identity_kwargs,
        )
        self._paid_fs = (
            paid_fs_client or FlareSolverrClient(
                url=flaresolverr_url,
                **identity_kwargs,
            )
            if resolved_policy is TransportPolicy.DIRECT_ONLY
            else None
        )
        self._proxy_client = None
        self._paid_gateway: Optional[PaidGatewayClient] = None
        if resolved_policy is TransportPolicy.DIRECT_THEN_PAID:
            self._paid_gateway = paid_gateway_client or PaidGatewayClient(
                str(paid_gateway_url)
            )
        self.stats = TransportStats()
        self._paid_browser_bootstraps = 0
        self._browser_sessions: dict[TransportRoute, _BrowserSession] = {}
        # A structured feed behind one validated stage bootstrap consistently
        # returns the same source-specific access gate to curl while its XHR
        # succeeds in the direct browser session.  Remember that evidence only
        # for the exact bootstrap and only for this transport lifetime.  The
        # raw cache is still consulted before this circuit on every request.
        self._direct_gate_circuits: set[str] = set()
        self._active_context = self.context
        self._active_cache_key = ""
        self._active_request_id = ""
        self._source_circuit_browser_blocked = False

    def _paid_fallback_enabled(self) -> bool:
        return bool(
            self.transport_policy is TransportPolicy.DIRECT_THEN_PAID
            and self._paid_gateway is not None
        )

    def _begin_source_operation(self) -> None:
        """Finish an unresolved prior probe before a new public operation."""

        self._source_circuit_browser_blocked = False
        if self._source_circuit is None or self._source_circuit_permit is None:
            return
        permit = self._source_circuit_permit
        self._source_circuit_permit = None
        try:
            self._source_circuit.inconclusive(permit)
        except SourceCircuitError as exc:
            raise WhoScoredTransportError(
                "WhoScored source circuit could not preserve an uncertain probe",
                kind=FailureKind.CONFIG,
                url="https://www.whoscored.com/",
                route=TransportRoute.DIRECT_FLARESOLVERR,
            ) from exc

    def _admit_structured_source(self, url: str) -> bool:
        if (
            self._source_circuit is None
            or self._source_circuit_permit is not None
            or not _is_whoscored_structured_feed_url(url)
        ):
            return True
        try:
            self._source_circuit_permit = self._source_circuit.admit(
                wait=self._source_circuit_wait
            )
        except SourceCircuitOpen as exc:
            if self._paid_fallback_enabled():
                # The shared circuit contains authoritative browser CF evidence.
                # Raw cache has already missed; allow only a fresh direct HTTP
                # recheck before the signed paid route, never another browser
                # stampede while the circuit is open.
                self._source_circuit_browser_blocked = True
                return False
            raise CloudflareChallenge(
                "WhoScored source cooldown is active",
                url=url,
                route=TransportRoute.DIRECT_FLARESOLVERR,
                source_wide=True,
            ) from exc
        except SourceCircuitError as exc:
            raise WhoScoredTransportError(
                "WhoScored source circuit state is unavailable",
                kind=FailureKind.CONFIG,
                url=url,
                route=TransportRoute.DIRECT_FLARESOLVERR,
            ) from exc
        return True

    def _source_probe_active(self) -> bool:
        return bool(
            self._source_circuit_permit is not None
            and self._source_circuit_permit.is_probe
        )

    def _source_succeeded(self, url: str) -> None:
        if (
            self._source_circuit is None
            or self._source_circuit_permit is None
            or not _is_whoscored_structured_feed_url(url)
        ):
            return
        permit = self._source_circuit_permit
        self._source_circuit_permit = None
        try:
            self._source_circuit.succeed(permit)
        except SourceCircuitError as exc:
            raise WhoScoredTransportError(
                "WhoScored source circuit could not record a successful probe",
                kind=FailureKind.CONFIG,
                url=url,
                route=TransportRoute.DIRECT_FLARESOLVERR,
            ) from exc

    def _source_tripped(self, exc: CloudflareChallenge) -> None:
        if (
            not exc.source_wide
            or self._source_circuit is None
            or self._source_circuit_permit is None
        ):
            return
        permit = self._source_circuit_permit
        self._source_circuit_permit = None
        try:
            self._source_circuit.trip(permit)
            if self._paid_fallback_enabled():
                self._source_circuit_browser_blocked = True
        except SourceCircuitError as circuit_exc:
            raise WhoScoredTransportError(
                "WhoScored source circuit could not record a source block",
                kind=FailureKind.CONFIG,
                url=exc.url,
                route=TransportRoute.DIRECT_FLARESOLVERR,
            ) from circuit_exc

    def _source_inconclusive(self, url: str) -> bool:
        """Reopen one uncertain half-open probe without escalating it."""

        if (
            self._source_circuit is None
            or self._source_circuit_permit is None
            or not self._source_circuit_permit.is_probe
            or not _is_whoscored_structured_feed_url(url)
        ):
            return False
        permit = self._source_circuit_permit
        self._source_circuit_permit = None
        try:
            self._source_circuit.inconclusive(permit)
        except SourceCircuitError as exc:
            raise WhoScoredTransportError(
                "WhoScored source circuit could not preserve an uncertain probe",
                kind=FailureKind.CONFIG,
                url=url,
                route=TransportRoute.DIRECT_FLARESOLVERR,
            ) from exc
        return True

    def _wait_before_browser_retry(self, attempt: int) -> None:
        """Apply one bounded exponential delay before a physical retry."""

        if self.browser_retry_backoff_seconds == 0:
            return
        multiplier = 2 ** min(max(0, int(attempt)), 10)
        base_delay = min(
            self.browser_retry_backoff_seconds * multiplier,
            MAX_BROWSER_RETRY_BACKOFF_SECONDS,
        )
        jitter = (
            random.uniform(
                0.0,
                min(
                    self.browser_retry_jitter_seconds,
                    MAX_BROWSER_RETRY_BACKOFF_SECONDS,
                ),
            )
            if self.browser_retry_jitter_seconds
            else 0.0
        )
        delay = min(base_delay + jitter, MAX_BROWSER_RETRY_BACKOFF_SECONDS)
        time.sleep(delay)

    def _wait_before_direct_http_retry(self, attempt: int) -> None:
        """Space transient origin retries so one short 5xx wave can clear."""

        if self.direct_http_retry_backoff_seconds == 0:
            return
        multiplier = 2 ** min(max(0, int(attempt)), 10)
        delay = min(
            self.direct_http_retry_backoff_seconds * multiplier,
            MAX_BROWSER_RETRY_BACKOFF_SECONDS,
        )
        time.sleep(delay)

    def _resolve_pool_proxy_url(self) -> Optional[str]:
        """Residential-pool egress for the direct route.

        WhoScored blocks the datacentre host IP at Cloudflare while the same
        region's residential pool passes.  When ``WHOSCORED_PROXY_FILE`` points
        at a ``host:port:user:pass`` pool the direct curl and direct
        FlareSolverr routes egress through one pool member chosen once per
        process (sticky residential, matching the platform's per-task proxy
        pattern).  Unset/empty keeps the legacy host-IP direct route.
        """
        path = os.environ.get("WHOSCORED_PROXY_FILE", "").strip()
        if not path:
            return None
        from scrapers.utils.proxy_manager import ProxyManager

        manager = ProxyManager(rotation_strategy="random")
        manager.load_from_file_custom_format(path)
        return manager.get_http_proxy_url()

    def _new_http_session(self, proxy_url: Optional[str]) -> Any:
        if self._http_session_factory is not None:
            return self._http_session_factory(proxy_url)
        try:
            from curl_cffi.requests import Session
        except ImportError as exc:  # pragma: no cover - production dependency
            raise RuntimeError("curl_cffi is required for WhoScoredTransport") from exc
        # A route labelled ``direct`` must never inherit HTTP(S)_PROXY from the
        # worker environment. Paid sessions receive their one explicit,
        # byte-bounded lease below; they must not merge it with ambient proxy
        # configuration either.
        session = Session(impersonate=self.impersonate, trust_env=False)
        session.headers.update(
            {
                "Accept": "text/html,application/xhtml+xml,application/json;q=0.9,*/*;q=0.8",
                "Accept-Language": "en-US,en;q=0.9",
                "Accept-Encoding": "gzip, deflate, br",
                "Sec-Fetch-Dest": "document",
                "Sec-Fetch-Mode": "navigate",
                "Sec-Fetch-Site": "none",
            }
        )
        if proxy_url:
            session.proxies = {"http": proxy_url, "https": proxy_url}
        return session

    def _record_supervisor_session(self, event: str, session_id: str) -> None:
        ledger = self._supervisor_resource_ledger
        if ledger is None:
            return
        prefix = capacity_browser_session_prefix(self._supervisor_session_owner)
        if event not in {"owned", "released"} or not session_id.startswith(prefix):
            raise RuntimeError("invalid supervised browser session lifecycle event")
        ledger.append(
            {
                "schema_version": 1,
                "event": event,
                "resource": "flaresolverr_session",
                "owner": self._supervisor_session_owner,
                "session_id": session_id,
                "dag_id": self.context.dag_id,
                "run_id": self.context.run_id,
                "task_id": self.context.task_id,
                "try_number": self.context.try_number,
                "recorded_at": datetime.now(timezone.utc).isoformat(),
            }
        )

    def _browser_session(
        self,
        client: FlareSolverrClient,
        *,
        route: TransportRoute,
        proxy_url: Optional[str],
        required_requests: int = 1,
    ) -> _BrowserSession:
        existing = self._browser_sessions.get(route)
        now = time.monotonic()
        if existing is not None and (
            existing.proxy_url != proxy_url
            or now - existing.created_at >= self.browser_session_ttl_seconds
            or (
                existing.requests > 0
                and existing.requests + max(1, required_requests)
                > self.browser_session_max_requests
            )
        ):
            self._drop_browser_session(client, route)
            existing = None
        if existing is not None:
            return existing
        suffix = f"{route.value}-{uuid.uuid4().hex[:10]}"
        session_id = (
            f"{self._capacity_browser_session_prefix}{suffix}"
            if self._capacity_browser_session_prefix is not None
            else f"ws-{suffix}"
        )
        created = _BrowserSession(
            session_id=session_id,
            proxy_url=proxy_url,
            created_at=now,
        )
        # Track ownership before the external side effect.  A signal can then
        # interrupt create or local accounting without losing the only id that
        # close() needs for an idempotent server-side destroy.
        self._browser_sessions[route] = created
        try:
            self._record_supervisor_session("owned", session_id)
        except BaseException:
            self._browser_sessions.pop(route, None)
            raise
        try:
            client.create_session(session_id, proxy_url=proxy_url)
            self.stats.browser_sessions += 1
        except BaseException:
            # Destroy even when create never reached the server; the endpoint
            # is idempotent.  A BaseException from cleanup deliberately keeps
            # tracking intact so the outer termination close can retry it.
            self._drop_browser_session(client, route)
            raise
        return created

    def _drop_browser_session(
        self, client: FlareSolverrClient, route: TransportRoute
    ) -> None:
        existing = self._browser_sessions.get(route)
        if existing is not None:
            if self._supervisor_resource_ledger is not None:
                try:
                    strict_destroy = getattr(client, "destroy_session_strict")
                    strict_destroy(existing.session_id)
                    self._record_supervisor_session("released", existing.session_id)
                except Exception as exc:
                    # Keep ownership durable and locally tracked. The parent
                    # process-group supervisor retries exact cleanup after the
                    # runner exits or is killed.
                    logger.error(
                        "Could not release supervised WhoScored browser session "
                        "for route %s (%s)",
                        route.value,
                        type(exc).__name__,
                    )
                    return
                self._browser_sessions.pop(route, None)
                return
            try:
                client.destroy_session(existing.session_id)
            except Exception as exc:  # destroy is best-effort and idempotent
                logger.debug(
                    "Could not destroy WhoScored browser session for route %s (%s)",
                    route.value,
                    type(exc).__name__,
                )
            # An ordinary destroy failure remains best-effort, but a
            # BaseException skips this line and keeps the id tracked so the
            # termination unwind can retry the idempotent destroy in close().
            if self._browser_sessions.get(route) is existing:
                self._browser_sessions.pop(route, None)

    def _replay_browser_identity(self, solution: Mapping[str, Any]) -> None:
        """Replay solved CF cookies into direct HTTP when the session supports it."""
        cookies = solution.get("cookies") or []
        jar = getattr(self._direct_http, "cookies", None)
        if jar is not None:
            for item in cookies:
                if not isinstance(item, Mapping) or not item.get("name"):
                    continue
                kwargs: dict[str, Any] = {}
                if item.get("domain"):
                    kwargs["domain"] = item["domain"]
                if item.get("path"):
                    kwargs["path"] = item["path"]
                try:
                    jar.set(str(item["name"]), str(item.get("value", "")), **kwargs)
                except Exception:
                    logger.debug("Could not replay browser cookie", exc_info=True)
        user_agent = str(solution.get("userAgent") or "").strip()
        headers = getattr(self._direct_http, "headers", None)
        if user_agent and headers is not None:
            headers["User-Agent"] = user_agent

    def _activate_request(
        self,
        *,
        cache_key: str,
        scope: Optional[str],
        entity: Optional[str],
        request_id: Optional[str] = None,
    ) -> None:
        """Select the exact URL identity used by ledger writes below."""

        self._active_cache_key = cache_key
        self._active_request_id = request_id or uuid.uuid4().hex
        self._active_context = self.context.request_context(
            scope=scope,
            entity=entity or self.context.entity or _entity_from_cache_key(cache_key),
        )

    def fetch(
        self,
        url: str,
        *,
        cache_key: Optional[str] = None,
        validator: Optional[Validator] = None,
        scope: Optional[str] = None,
        entity: Optional[str] = None,
        before_network: Optional[Callable[[], None]] = None,
        browser_bootstrap_url: Optional[str] = None,
    ) -> TransportResponse:
        """Fetch one logical URL through the fixed route state machine."""

        if browser_bootstrap_url is not None and (
            not _is_whoscored_structured_feed_url(url)
            or not _is_whoscored_stage_bootstrap_url(browser_bootstrap_url)
        ):
            raise WhoScoredTransportError(
                "browser XHR bootstrap is restricted to WhoScored stage statistics",
                kind=FailureKind.CONFIG,
                url=url,
                route=TransportRoute.DIRECT_FLARESOLVERR,
            )

        key = cache_key or url
        self._activate_request(
            cache_key=key,
            scope=scope,
            entity=entity,
        )
        cache_invalid_before = self.stats.cache_invalid
        cached = self._load_cached(key, url, validator)
        if cached is not None:
            return cached
        cache_entry_was_invalid = self.stats.cache_invalid > cache_invalid_before
        self._begin_source_operation()
        source_browser_admitted = self._admit_structured_source(url)
        if before_network is not None:
            # The gate belongs after cache validation, not before ``has``.
            # A parser-invalid but integrity-valid raw object intentionally
            # retries direct once and must consume the same rate-limit token
            # as an ordinary cache miss.
            before_network()

        direct_gate_key = (
            _canonical_url_key(browser_bootstrap_url)
            if browser_bootstrap_url is not None
            else None
        )

        def _direct_once() -> TransportResponse:
            response = self._http_fetch(
                url,
                session=self._direct_http,
                route=TransportRoute.DIRECT_HTTP,
                referer=browser_bootstrap_url,
            )
            try:
                self._validate(response, validator)
            except WhoScoredTransportError as exc:
                if exc.kind is FailureKind.CONTENT:
                    self._store_response(key, response)
                raise
            return response

        def _direct_with_retries(
            *, acquire_first_token: bool = False
        ) -> TransportResponse:
            if acquire_first_token:
                self._admit_structured_source(url)
                if before_network is not None:
                    before_network()
            for attempt in range(self.direct_http_attempts):
                try:
                    return _direct_once()
                except CloudflareChallenge:
                    # Cloudflare is route evidence, not a transient ordinary
                    # HTTP failure. Move to the direct browser immediately.
                    raise
                except WhoScoredTransportError as exc:
                    if self._source_inconclusive(url):
                        raise
                    retryable_direct = exc.retryable and exc.kind in {
                        FailureKind.HTTP_STATUS,
                        FailureKind.TIMEOUT,
                    }
                    if not retryable_direct or attempt + 1 >= self.direct_http_attempts:
                        raise
                    self._wait_before_direct_http_retry(attempt)
                    if before_network is not None:
                        # Every physical retry consumes a source-rate token.
                        before_network()
            raise AssertionError("direct HTTP retry loop returned no result")

        # Parser-invalid cache entries retain their existing direct-origin
        # retry contract.  Only an actual raw miss may use prior gate evidence.
        skip_direct = bool(
            direct_gate_key
            and not cache_entry_was_invalid
            and direct_gate_key in self._direct_gate_circuits
            and source_browser_admitted
        )
        initial_direct_cf: Optional[CloudflareChallenge] = None
        if not skip_direct:
            try:
                direct = _direct_with_retries()
            except CloudflareChallenge as exc:
                initial_direct_cf = exc
                self.stats.failures[FailureKind.CLOUDFLARE.value] += 1
                if direct_gate_key is not None:
                    self._direct_gate_circuits.add(direct_gate_key)
            except WhoScoredTransportError:
                if direct_gate_key is not None:
                    self._direct_gate_circuits.discard(direct_gate_key)
                raise
            else:
                if direct_gate_key is not None:
                    self._direct_gate_circuits.discard(direct_gate_key)
                stored = self._store_and_return(key, direct)
                self._source_succeeded(url)
                return stored

        direct_cf_failures = 0
        last_cf: Optional[CloudflareChallenge] = None
        last_transient: Optional[WhoScoredTransportError] = None
        # Shared circuit state is writable by ordinary Airflow tasks. It may
        # suppress a probe, but can never stand in for browser CF evidence when
        # authorising paid traffic.
        if not source_browser_admitted:
            assert initial_direct_cf is not None
            raise initial_direct_cf
        for attempt in range(self.direct_browser_attempts):
            try:
                browser = self._browser_fetch(
                    url,
                    client=self._direct_fs,
                    route=TransportRoute.DIRECT_FLARESOLVERR,
                    proxy_url=self._pool_proxy_url,
                    bootstrap_url=browser_bootstrap_url,
                )
                self._validate(browser, validator)
            except CloudflareChallenge as exc:
                self._drop_browser_session(
                    self._direct_fs, TransportRoute.DIRECT_FLARESOLVERR
                )
                direct_cf_failures += 1
                last_cf = exc
                self.stats.failures[FailureKind.CLOUDFLARE.value] += 1
                self._source_tripped(exc)
                if not exc.source_wide and self._source_inconclusive(url):
                    raise
                if (
                    exc.source_wide
                    and self._source_circuit is not None
                    and not self._paid_fallback_enabled()
                    and not self._source_circuit_wait
                ):
                    raise
                if attempt + 1 < self.direct_browser_attempts:
                    if (
                        exc.source_wide
                        and self._source_circuit is not None
                        and self._source_circuit_wait
                    ):
                        self._admit_structured_source(url)
                    else:
                        self._wait_before_browser_retry(attempt)
                    if before_network is not None:
                        before_network()
                continue
            except WhoScoredTransportError as exc:
                # A tab crash, timeout, ordinary browser error or parser
                # rejection must never silently buy residential bandwidth.
                if direct_gate_key is not None:
                    self._direct_gate_circuits.discard(direct_gate_key)
                if self._source_inconclusive(url):
                    self._drop_browser_session(
                        self._direct_fs, TransportRoute.DIRECT_FLARESOLVERR
                    )
                    if exc.kind is FailureKind.CONTENT:
                        self._store_response(key, browser)
                    raise
                if (
                    exc.retryable
                    and exc.kind
                    in {
                        FailureKind.HTTP_STATUS,
                        FailureKind.TIMEOUT,
                        FailureKind.BROWSER,
                    }
                    and attempt + 1 < self.direct_browser_attempts
                ):
                    last_transient = exc
                    self._drop_browser_session(
                        self._direct_fs, TransportRoute.DIRECT_FLARESOLVERR
                    )
                    self._wait_before_browser_retry(attempt)
                    if before_network is not None:
                        before_network()
                    continue
                if exc.kind is FailureKind.CONTENT:
                    self._store_response(key, browser)
                raise
            stored = self._store_and_return(key, browser)
            self._source_succeeded(url)
            return stored

        # Any non-CF browser outcome makes the evidence mixed.  A later
        # source-wide challenge may still trip the shared cooldown, but it
        # cannot erase that earlier timeout/error and authorize paid traffic.
        if last_transient is not None:
            raise last_transient
        if direct_cf_failures != self.direct_browser_attempts:
            assert last_cf is not None
            raise last_cf
        if self._paid_gateway is None:
            assert last_cf is not None
            raise last_cf

        # Circuit evidence is an optimisation, never authority to spend.
        # Browser CF failures invalidate the earlier direct observation: every
        # serial page and structured feed rechecks direct HTTP immediately
        # before creating a paid lease.
        if direct_gate_key is not None:
            self._direct_gate_circuits.discard(direct_gate_key)
        try:
            direct = _direct_with_retries(acquire_first_token=True)
        except CloudflareChallenge:
            self.stats.failures[FailureKind.CLOUDFLARE.value] += 1
            # The paid transition is now backed by fresh direct evidence.
            if direct_gate_key is not None:
                self._direct_gate_circuits.add(direct_gate_key)
        except WhoScoredTransportError:
            # Timeout, status and content failures are not CF evidence and
            # therefore stop before paid traffic with the circuit clear.
            raise
        else:
            stored = self._store_and_return(key, direct)
            self._source_succeeded(url)
            return stored

        paid = self._paid_fetch(
            url,
            validator,
            cache_key=key,
            browser_bootstrap_url=browser_bootstrap_url,
        )
        return self._store_and_return(key, paid)

    def fetch_many(self, requests: Sequence[FetchRequest]) -> list[TransportResponse]:
        """Fetch one structured-feed group with bounded direct-browser batches.

        Every item checks raw cache first. The first raw miss confirms the
        official direct-origin access gate; later misses under that exact
        bootstrap use the existing gate circuit instead of repeating a known-
        useless direct request. Only those URLs reach the restricted browser
        endpoint, in batches of at most eight. The method returns only after
        every validator succeeds, so callers cannot publish a partial scope.
        Valid raw objects may still be retained on failure so an Airflow retry
        is resumable without duplicate traffic.
        """

        items = tuple(requests)
        if not items:
            return []
        if len({item.cache_key for item in items}) != len(items):
            raise WhoScoredTransportError(
                "structured batch cache keys must be unique",
                kind=FailureKind.CONFIG,
                url=items[0].url,
            )
        if len({item.url for item in items}) != len(items):
            raise WhoScoredTransportError(
                "structured batch URLs must be unique",
                kind=FailureKind.CONFIG,
                url=items[0].url,
            )
        bootstrap_urls = {item.browser_bootstrap_url for item in items}
        if len(bootstrap_urls) != 1 or None in bootstrap_urls:
            raise WhoScoredTransportError(
                "structured batch requires one exact browser bootstrap URL",
                kind=FailureKind.CONFIG,
                url=items[0].url,
            )
        bootstrap_url = items[0].browser_bootstrap_url
        assert bootstrap_url is not None
        if not _is_whoscored_stage_bootstrap_url(bootstrap_url) or any(
            not _is_whoscored_structured_feed_url(item.url) for item in items
        ):
            raise WhoScoredTransportError(
                "browser XHR batch is restricted to WhoScored stage statistics",
                kind=FailureKind.CONFIG,
                url=items[0].url,
                route=TransportRoute.DIRECT_FLARESOLVERR,
            )

        results: list[Optional[TransportResponse]] = [None] * len(items)
        gated: list[tuple[int, FetchRequest, bool]] = []
        gate_key = _canonical_url_key(bootstrap_url)
        request_ids = {item.cache_key: uuid.uuid4().hex for item in items}

        # Raw cache remains first for every logical URL. The first fresh direct
        # response carrying the exact official access gate opens the same
        # bootstrap-scoped circuit as the serial path; later raw misses avoid
        # known-useless duplicate direct requests. Paid still requires a fresh
        # direct recheck below.
        misses: list[tuple[int, FetchRequest, bool]] = []
        for index, item in enumerate(items):
            self._activate_request(
                cache_key=item.cache_key,
                scope=item.scope,
                entity=item.entity,
                request_id=request_ids[item.cache_key],
            )
            cache_invalid_before = self.stats.cache_invalid
            cached = self._load_cached(item.cache_key, item.url, item.validator)
            if cached is not None:
                results[index] = cached
                continue
            cache_entry_was_invalid = self.stats.cache_invalid > cache_invalid_before
            misses.append((index, item, cache_entry_was_invalid))

        if not misses:
            return [response for response in results if response is not None]

        # An unresolved prior half-open probe is settled only after every raw
        # object has had its chance to satisfy this logical batch.  Circuit
        # state can therefore never block a complete warm-cache replay.
        self._begin_source_operation()
        for index, item, cache_entry_was_invalid in misses:
            self._activate_request(
                cache_key=item.cache_key,
                scope=item.scope,
                entity=item.entity,
                request_id=request_ids[item.cache_key],
            )
            source_browser_admitted = self._admit_structured_source(item.url)
            if (
                source_browser_admitted
                and not cache_entry_was_invalid
                and gate_key in self._direct_gate_circuits
            ):
                gated.append((index, item, False))
                continue
            if item.before_network is not None:
                item.before_network()
            for attempt in range(self.direct_http_attempts):
                direct: Optional[TransportResponse] = None
                try:
                    direct = self._http_fetch(
                        item.url,
                        session=self._direct_http,
                        route=TransportRoute.DIRECT_HTTP,
                        referer=bootstrap_url,
                    )
                    self._validate(direct, item.validator)
                except CloudflareChallenge:
                    self.stats.failures[FailureKind.CLOUDFLARE.value] += 1
                    self._direct_gate_circuits.add(gate_key)
                    if source_browser_admitted:
                        gated.append((index, item, True))
                    else:
                        # Shared circuit state is mutable scheduling metadata,
                        # never current-request browser evidence. A fresh
                        # direct CF response alone cannot authorise paid bytes.
                        raise
                    break
                except WhoScoredTransportError as exc:
                    if exc.kind is FailureKind.CONTENT:
                        assert direct is not None
                        self._store_response(item.cache_key, direct)
                    if self._source_inconclusive(item.url):
                        self._direct_gate_circuits.discard(gate_key)
                        raise
                    retryable_direct = exc.retryable and exc.kind in {
                        FailureKind.HTTP_STATUS,
                        FailureKind.TIMEOUT,
                    }
                    if retryable_direct and attempt + 1 < self.direct_http_attempts:
                        self._wait_before_direct_http_retry(attempt)
                        if item.before_network is not None:
                            item.before_network()
                        continue
                    self._direct_gate_circuits.discard(gate_key)
                    raise
                else:
                    assert direct is not None
                    self._direct_gate_circuits.discard(gate_key)
                    self._store_response(item.cache_key, direct)
                    results[index] = direct
                    self._source_succeeded(item.url)
                    break

        offset = 0
        while offset < len(gated):
            if (
                self._source_circuit_permit is None
                and not self._admit_structured_source(gated[offset][1].url)
            ):
                raise CloudflareChallenge(
                    "WhoScored source cooldown is active",
                    url=gated[offset][1].url,
                    route=TransportRoute.DIRECT_FLARESOLVERR,
                    source_wide=True,
                )
            batch_size = 1 if self._source_probe_active() else MAX_XHR_BATCH_URLS
            pending = gated[offset : offset + batch_size]
            offset += len(pending)
            last_cf: Optional[CloudflareChallenge] = None
            last_item_errors: dict[str, WhoScoredTransportError] = {}
            non_cf_evidence: dict[str, WhoScoredTransportError] = {}
            cf_failure_counts: Counter[str] = Counter()
            rechunk_after_cooldown: list[tuple[int, FetchRequest, bool]] = []
            browser_attempts = range(self.direct_browser_attempts)
            for attempt in browser_attempts:
                if not pending:
                    break
                rate_limited_pending: list[tuple[int, FetchRequest, bool]] = []
                for index, item, network_gate_acquired in pending:
                    if not network_gate_acquired and item.before_network is not None:
                        item.before_network()
                    rate_limited_pending.append((index, item, True))
                pending = rate_limited_pending
                try:
                    browser_outcomes = self._browser_fetch_many(
                        [item for _, item, _ in pending],
                        client=self._direct_fs,
                        route=TransportRoute.DIRECT_FLARESOLVERR,
                        proxy_url=None,
                        bootstrap_url=bootstrap_url,
                        request_ids=request_ids,
                    )
                except CloudflareChallenge as exc:
                    self._drop_browser_session(
                        self._direct_fs, TransportRoute.DIRECT_FLARESOLVERR
                    )
                    self.stats.failures[FailureKind.CLOUDFLARE.value] += len(pending)
                    last_cf = exc
                    for _, item, _ in pending:
                        last_item_errors[item.cache_key] = exc
                        cf_failure_counts[item.cache_key] += 1
                    pending = [(index, item, False) for index, item, _ in pending]
                    self._source_tripped(exc)
                    if not exc.source_wide and self._source_inconclusive(
                        pending[0][1].url
                    ):
                        raise
                    if (
                        exc.source_wide
                        and self._source_circuit is not None
                        and not self._paid_fallback_enabled()
                        and not self._source_circuit_wait
                    ):
                        raise
                    if (
                        exc.source_wide
                        and self._source_circuit is not None
                        and self._source_circuit_wait
                    ):
                        rechunk_after_cooldown = list(pending)
                        pending = []
                        break
                    if attempt + 1 < self.direct_browser_attempts:
                        if not (
                            exc.source_wide
                            and self._source_circuit is not None
                            and self._source_circuit_wait
                        ):
                            self._wait_before_browser_retry(attempt)
                    continue
                except WhoScoredTransportError as exc:
                    # Endpoint-level timeout/protocol failures do not produce
                    # per-item outcomes. Retry only explicitly transient
                    # browser/bootstrap failures; retain them as non-CF
                    # evidence so a later challenge can never authorize paid.
                    self._direct_gate_circuits.discard(gate_key)
                    self._drop_browser_session(
                        self._direct_fs, TransportRoute.DIRECT_FLARESOLVERR
                    )
                    if not exc.retryable:
                        self._source_inconclusive(pending[0][1].url)
                        raise
                    for _, item, _ in pending:
                        last_item_errors[item.cache_key] = exc
                        non_cf_evidence[item.cache_key] = exc
                    pending = [(index, item, False) for index, item, _ in pending]
                    if self._source_inconclusive(pending[0][1].url):
                        raise
                    if attempt + 1 < self.direct_browser_attempts:
                        self._wait_before_browser_retry(attempt)
                    continue

                retry: list[tuple[int, FetchRequest, bool]] = []
                terminal_error: Optional[WhoScoredTransportError] = None
                source_wide_cf: Optional[CloudflareChallenge] = None
                for (index, item, network_gate_acquired), outcome in zip(
                    pending, browser_outcomes
                ):
                    if outcome.error is not None:
                        last_item_errors[item.cache_key] = outcome.error
                        non_cf_evidence[item.cache_key] = outcome.error
                        if outcome.error.retryable:
                            retry.append((index, item, False))
                        else:
                            terminal_error = terminal_error or outcome.error
                        continue
                    browser = outcome.response
                    assert browser is not None
                    self._activate_request(
                        cache_key=item.cache_key,
                        scope=item.scope,
                        entity=item.entity,
                        request_id=request_ids[item.cache_key],
                    )
                    try:
                        self._validate(browser, item.validator)
                    except CloudflareChallenge as exc:
                        self.stats.failures[FailureKind.CLOUDFLARE.value] += 1
                        last_cf = exc
                        last_item_errors[item.cache_key] = exc
                        cf_failure_counts[item.cache_key] += 1
                        retry.append((index, item, False))
                        if exc.source_wide:
                            source_wide_cf = source_wide_cf or exc
                    except WhoScoredTransportError as exc:
                        if exc.kind is FailureKind.CONTENT:
                            self._store_response(item.cache_key, browser)
                        last_item_errors[item.cache_key] = exc
                        non_cf_evidence[item.cache_key] = exc
                        if exc.retryable:
                            retry.append((index, item, False))
                        else:
                            terminal_error = terminal_error or exc
                    else:
                        self._store_response(item.cache_key, browser)
                        results[index] = browser
                        last_item_errors.pop(item.cache_key, None)
                        non_cf_evidence.pop(item.cache_key, None)
                if source_wide_cf is not None:
                    # Record authoritative source evidence before any sibling
                    # item error can win exception precedence.  The blocked
                    # browser session is never reused after that evidence.
                    self._drop_browser_session(
                        self._direct_fs, TransportRoute.DIRECT_FLARESOLVERR
                    )
                    self._source_tripped(source_wide_cf)
                    if (
                        self._source_circuit is not None
                        and not self._paid_fallback_enabled()
                        and not self._source_circuit_wait
                    ):
                        raise source_wide_cf
                    if self._source_circuit is not None and self._source_circuit_wait:
                        if terminal_error is not None:
                            self._direct_gate_circuits.discard(gate_key)
                            raise terminal_error
                        rechunk_after_cooldown = list(retry)
                        pending = []
                        break
                if self._source_probe_active() and (terminal_error or retry):
                    uncertain = (
                        terminal_error or last_item_errors[retry[0][1].cache_key]
                    )
                    self._drop_browser_session(
                        self._direct_fs, TransportRoute.DIRECT_FLARESOLVERR
                    )
                    self._direct_gate_circuits.discard(gate_key)
                    self._source_inconclusive(pending[0][1].url)
                    raise uncertain
                if terminal_error is not None:
                    self._direct_gate_circuits.discard(gate_key)
                    raise terminal_error
                pending = retry
                if pending:
                    self._drop_browser_session(
                        self._direct_fs, TransportRoute.DIRECT_FLARESOLVERR
                    )
                    if attempt + 1 < self.direct_browser_attempts:
                        if not (
                            source_wide_cf is not None
                            and self._source_circuit is not None
                        ):
                            self._wait_before_browser_retry(attempt)
                else:
                    self._source_succeeded(items[0].url)

            if rechunk_after_cooldown:
                # Re-enter the outer chunker only after shared admission.  The
                # half-open permit then forces exactly one XHR, never the old
                # multi-item batch that observed the block.
                gated[offset:offset] = rechunk_after_cooldown
                continue
            if not pending:
                continue
            non_cf_errors: list[WhoScoredTransportError] = []
            for _, item, _ in pending:
                item_error = non_cf_evidence.get(item.cache_key)
                if item_error is not None and not isinstance(
                    item_error, CloudflareChallenge
                ):
                    non_cf_errors.append(item_error)
                    continue
                if cf_failure_counts[item.cache_key] != self.direct_browser_attempts:
                    incomplete_error = last_item_errors.get(item.cache_key)
                    if incomplete_error is not None:
                        non_cf_errors.append(incomplete_error)
            if non_cf_errors:
                self._direct_gate_circuits.discard(gate_key)
                raise non_cf_errors[0]
            if self._paid_gateway is None:
                assert last_cf is not None
                raise last_cf

            # Batch browser CF evidence is never authority to spend. Recheck
            # each URL through direct HTTP immediately before its bounded paid
            # lease, exactly like the serial state machine.
            self._direct_gate_circuits.discard(gate_key)
            paid_pending: list[tuple[int, FetchRequest]] = []
            for index, item, _ in pending:
                self._activate_request(
                    cache_key=item.cache_key,
                    scope=item.scope,
                    entity=item.entity,
                    request_id=request_ids[item.cache_key],
                )
                self._admit_structured_source(item.url)
                if item.before_network is not None:
                    item.before_network()
                fresh_cloudflare = False
                for attempt in range(self.direct_http_attempts):
                    fresh_direct: Optional[TransportResponse] = None
                    try:
                        fresh_direct = self._http_fetch(
                            item.url,
                            session=self._direct_http,
                            route=TransportRoute.DIRECT_HTTP,
                            referer=bootstrap_url,
                        )
                        self._validate(fresh_direct, item.validator)
                    except CloudflareChallenge:
                        self.stats.failures[FailureKind.CLOUDFLARE.value] += 1
                        fresh_cloudflare = True
                        break
                    except WhoScoredTransportError as exc:
                        retryable_direct = exc.retryable and exc.kind in {
                            FailureKind.HTTP_STATUS,
                            FailureKind.TIMEOUT,
                        }
                        if retryable_direct and attempt + 1 < self.direct_http_attempts:
                            self._wait_before_direct_http_retry(attempt)
                            if item.before_network is not None:
                                item.before_network()
                            continue
                        if exc.kind is FailureKind.CONTENT:
                            assert fresh_direct is not None
                            self._store_response(item.cache_key, fresh_direct)
                        raise
                    else:
                        assert fresh_direct is not None
                        self._store_response(item.cache_key, fresh_direct)
                        results[index] = fresh_direct
                        self._source_succeeded(item.url)
                        break
                if not fresh_cloudflare:
                    continue
                paid_pending.append((index, item))

            if paid_pending and self._paid_batch_enabled:
                paid_batch = self._paid_fetch_many(
                    [item for _, item in paid_pending],
                    browser_bootstrap_url=bootstrap_url,
                    request_ids=request_ids,
                )
                for (index, item), paid in zip(paid_pending, paid_batch):
                    self._store_response(item.cache_key, paid)
                    results[index] = paid
            else:
                for index, item in paid_pending:
                    self._activate_request(
                        cache_key=item.cache_key,
                        scope=item.scope,
                        entity=item.entity,
                        request_id=request_ids[item.cache_key],
                    )
                    paid = self._paid_fetch(
                        item.url,
                        item.validator,
                        cache_key=item.cache_key,
                        browser_bootstrap_url=bootstrap_url,
                    )
                    self._store_response(item.cache_key, paid)
                    results[index] = paid

        if any(response is None for response in results):
            raise WhoScoredTransportError(
                "structured batch finished without every response",
                kind=FailureKind.BROWSER,
                url=items[0].url,
                route=TransportRoute.DIRECT_FLARESOLVERR,
            )
        self._source_succeeded(items[0].url)
        return [response for response in results if response is not None]

    def _load_cached(
        self, key: str, url: str, validator: Optional[Validator]
    ) -> Optional[TransportResponse]:
        if self.raw_cache is None:
            return None
        try:
            payload = self.raw_cache.load(key)
        except Exception as exc:
            self._record_ledger(
                url=url,
                route=TransportRoute.RAW_CACHE,
                status="error",
                failure_kind=FailureKind.CACHE,
                error=exc,
            )
            raise WhoScoredTransportError(
                f"raw cache load failed: {exc}",
                kind=FailureKind.CACHE,
                url=url,
                route=TransportRoute.RAW_CACHE,
            ) from exc
        if payload is None:
            self._record_ledger(
                url=url,
                route=TransportRoute.RAW_CACHE,
                status="miss",
                cache_hit=False,
            )
            return None
        response = self._response(
            url=url,
            content=payload.content,
            status_code=payload.status_code,
            headers=payload.headers,
            route=TransportRoute.RAW_CACHE,
            wire_bytes=0,
        )
        try:
            self._validate(response, validator)
        except WhoScoredTransportError as exc:
            self.stats.cache_invalid += 1
            if exc.kind is FailureKind.CONTENT:
                # The cached object was still parsed first, but a transient
                # HTTP 200 body (partial edge response, maintenance shell)
                # must not poison an immutable target forever. Retry origin
                # direct exactly once. A second validator rejection is stored
                # for diagnosis and raised immediately; it never enables a
                # browser or paid-proxy route.
                logger.warning(
                    "Parser-invalid raw cache entry for %s; retrying direct origin",
                    key,
                )
                return None
            logger.warning("Ignoring transport-invalid raw cache entry for %s", key)
            return None
        self.stats.cache_hits += 1
        self.stats.route_requests[TransportRoute.RAW_CACHE.value] += 1
        return response

    def _store_and_return(
        self, key: str, response: TransportResponse
    ) -> TransportResponse:
        self._store_response(key, response)
        return response

    def _store_response(self, key: str, response: TransportResponse) -> None:
        """Persist one transport-safe response, including parser drift payloads."""
        if self.raw_cache is not None:
            payload = CachedPayload(
                content=response.content,
                status_code=response.status_code,
                headers=response.headers,
                observed_at=response.observed_at,
            )
            try:
                self.raw_cache.store(key, payload, response.sha256)
            except Exception as exc:
                raise WhoScoredTransportError(
                    "raw cache store failed: "
                    f"{_safe_route_exception_text(exc, route=response.route)}",
                    kind=FailureKind.CACHE,
                    url=response.url,
                    route=response.route,
                ) from exc

    def _http_fetch(
        self,
        url: str,
        *,
        session: Any,
        route: TransportRoute,
        referer: Optional[str] = None,
    ) -> TransportResponse:
        request_kwargs: dict[str, Any] = {"timeout": self.request_timeout}
        if _is_whoscored_structured_feed_url(url):
            request_headers = {
                "Accept": "application/json, text/javascript, */*; q=0.01",
                "X-Requested-With": "XMLHttpRequest",
                "Sec-Fetch-Dest": "empty",
                "Sec-Fetch-Mode": "cors",
                "Sec-Fetch-Site": "same-origin",
            }
            if referer:
                request_headers["Referer"] = referer
            request_kwargs["headers"] = request_headers
            # curl/requests otherwise follow redirects before validation,
            # which would defeat the browser endpoint's no-follow policy.
            request_kwargs["allow_redirects"] = False
        try:
            raw = session.get(url, **request_kwargs)
        except Exception as exc:
            self.stats.failures[FailureKind.TIMEOUT.value] += 1
            self._record_ledger(
                url=url,
                route=route,
                status="error",
                failure_kind=FailureKind.TIMEOUT,
                error=exc,
            )
            detail = _safe_route_exception_text(exc, route=route)
            if route not in {
                TransportRoute.PAID_HTTP,
                TransportRoute.PAID_FLARESOLVERR,
                TransportRoute.PAID_LEASE,
            }:
                detail = f"{type(exc).__name__}: {detail}"
            raise WhoScoredTransportError(
                f"HTTP request failed: {detail}",
                kind=FailureKind.TIMEOUT,
                url=url,
                route=route,
                retryable=True,
            ) from exc
        content = bytes(raw.content or b"")
        response = self._response(
            url=url,
            content=content,
            status_code=int(raw.status_code),
            headers=dict(raw.headers or {}),
            route=route,
            wire_bytes=_response_wire_bytes(raw, content),
            request_bytes=_request_wire_bytes(raw),
        )
        self._record_response(response)
        return response

    def _browser_fetch(
        self,
        url: str,
        *,
        client: FlareSolverrClient,
        route: TransportRoute,
        proxy_url: Optional[str],
        bootstrap_url: Optional[str] = None,
    ) -> TransportResponse:
        existing = self._browser_sessions.get(route)
        needs_bootstrap = bool(
            bootstrap_url
            and (existing is None or existing.bootstrap_url != bootstrap_url)
        )
        try:
            session = self._browser_session(
                client,
                route=route,
                proxy_url=proxy_url,
                required_requests=2 if needs_bootstrap else 1,
            )
        except FlareSolverrRuntimeIdentityError as exc:
            self._record_ledger(
                url=url,
                route=route,
                status="error",
                failure_kind=FailureKind.CONFIG,
                error=exc,
            )
            raise WhoScoredTransportError(
                _safe_route_exception_text(exc, route=route),
                kind=FailureKind.CONFIG,
                url=url,
                route=route,
                retryable=False,
            ) from exc
        except FlareSolverrCFChallengeFailed as exc:
            self._record_ledger(
                url=url,
                route=route,
                status="error",
                failure_kind=FailureKind.CLOUDFLARE,
                error=exc,
            )
            raise CloudflareChallenge(
                _safe_route_exception_text(exc, route=route),
                url=url,
                route=route,
                source_wide=route is TransportRoute.DIRECT_FLARESOLVERR,
            ) from exc
        except FlareSolverrResponseTooLarge as exc:
            self._record_ledger(
                url=url,
                route=route,
                status="error",
                failure_kind=FailureKind.BUDGET,
                error=exc,
            )
            raise TransportBudgetExceeded(
                _safe_route_exception_text(exc, route=route), url=url, route=route
            ) from exc
        except FlareSolverrTimeout as exc:
            self._record_ledger(
                url=url,
                route=route,
                status="error",
                failure_kind=FailureKind.TIMEOUT,
                error=exc,
            )
            raise WhoScoredTransportError(
                _safe_route_exception_text(exc, route=route),
                kind=FailureKind.TIMEOUT,
                url=url,
                route=route,
                retryable=True,
            ) from exc
        except FlareSolverrError as exc:
            self._record_ledger(
                url=url,
                route=route,
                status="error",
                failure_kind=FailureKind.BROWSER,
                error=exc,
            )
            raise WhoScoredTransportError(
                _safe_route_exception_text(exc, route=route),
                kind=FailureKind.BROWSER,
                url=url,
                route=route,
                retryable=True,
            ) from exc
        session_id = session.session_id
        try:
            if bootstrap_url and session.bootstrap_url != bootstrap_url:
                bootstrap_solution = client.get(
                    bootstrap_url,
                    session_id,
                    max_timeout_ms=self.browser_timeout_ms,
                    disable_media=True,
                )
                session.requests += 1
                bootstrap_content = str(bootstrap_solution.get("html") or "").encode(
                    "utf-8"
                )
                bootstrap_response = self._response(
                    url=bootstrap_url,
                    content=bootstrap_content,
                    status_code=int(bootstrap_solution.get("status") or 0),
                    headers={},
                    route=route,
                    wire_bytes=len(bootstrap_content),
                    response_bytes=len(bootstrap_content),
                )
                self._record_response(bootstrap_response)
                if is_chromium_error_page(bootstrap_response.text):
                    raise FlareSolverrErrorPage(
                        "FlareSolverr returned a Chromium network-error page"
                    )
                try:
                    self._validate(bootstrap_response, validator=None)
                except WhoScoredTransportError as exc:
                    # A browser session can occasionally receive WhoScored's
                    # own 302 verification shell while the next clean session
                    # reaches the exact same fixed TeamStatistics page. This
                    # is retryable direct-browser evidence, not Cloudflare
                    # proof and never authority for paid traffic.
                    if (
                        exc.kind is FailureKind.HTTP_STATUS
                        and bootstrap_response.status_code in {301, 302, 303, 307, 308}
                    ):
                        raise WhoScoredTransportError(
                            "WhoScored browser bootstrap returned a redirect",
                            kind=FailureKind.BROWSER,
                            url=bootstrap_url,
                            route=route,
                            status_code=bootstrap_response.status_code,
                            retryable=True,
                        ) from exc
                    raise
                if route is TransportRoute.DIRECT_FLARESOLVERR:
                    self._replay_browser_identity(bootstrap_solution)
                session.bootstrap_url = bootstrap_url

            if bootstrap_url:
                solution = client.xhr_get(
                    url,
                    session_id,
                    max_timeout_ms=self.browser_timeout_ms,
                )
                content = bytes(solution.get("content") or b"")
                headers = dict(solution.get("headers") or {})
                status = int(solution.get("status") or 0)
                response_bytes = int(solution.get("responseBytes") or len(content))
            else:
                solution = client.get(
                    url,
                    session_id,
                    max_timeout_ms=self.browser_timeout_ms,
                    disable_media=True,
                )
                content = str(solution.get("html") or "").encode("utf-8")
                headers = {}
                status = int(solution.get("status") or 0)
                response_bytes = len(content)
            session.requests += 1
        except FlareSolverrRuntimeIdentityError as exc:
            self._drop_browser_session(client, route)
            self._record_ledger(
                url=url,
                route=route,
                status="error",
                failure_kind=FailureKind.CONFIG,
                error=exc,
            )
            raise WhoScoredTransportError(
                _safe_route_exception_text(exc, route=route),
                kind=FailureKind.CONFIG,
                url=url,
                route=route,
                retryable=False,
            ) from exc
        except FlareSolverrCFChallengeFailed as exc:
            self._drop_browser_session(client, route)
            self._record_ledger(
                url=url,
                route=route,
                status="error",
                failure_kind=FailureKind.CLOUDFLARE,
                error=exc,
            )
            raise CloudflareChallenge(
                _safe_route_exception_text(exc, route=route),
                url=url,
                route=route,
                source_wide=route is TransportRoute.DIRECT_FLARESOLVERR,
            ) from exc
        except FlareSolverrResponseTooLarge as exc:
            self._drop_browser_session(client, route)
            self._record_ledger(
                url=url,
                route=route,
                status="error",
                failure_kind=FailureKind.BUDGET,
                error=exc,
            )
            raise TransportBudgetExceeded(
                _safe_route_exception_text(exc, route=route), url=url, route=route
            ) from exc
        except FlareSolverrTimeout as exc:
            self._drop_browser_session(client, route)
            self._record_ledger(
                url=url,
                route=route,
                status="error",
                failure_kind=FailureKind.TIMEOUT,
                error=exc,
            )
            raise WhoScoredTransportError(
                _safe_route_exception_text(exc, route=route),
                kind=FailureKind.TIMEOUT,
                url=url,
                route=route,
                retryable=True,
            ) from exc
        except FlareSolverrTabCrashed as exc:
            self._drop_browser_session(client, route)
            self._record_ledger(
                url=url,
                route=route,
                status="error",
                failure_kind=FailureKind.BROWSER,
                error=exc,
            )
            raise WhoScoredTransportError(
                _safe_route_exception_text(exc, route=route),
                kind=FailureKind.BROWSER,
                url=url,
                route=route,
                retryable=True,
            ) from exc
        except (FlareSolverrErrorPage, FlareSolverrError) as exc:
            self._drop_browser_session(client, route)
            self._record_ledger(
                url=url,
                route=route,
                status="error",
                failure_kind=FailureKind.BROWSER,
                error=exc,
            )
            raise WhoScoredTransportError(
                _safe_route_exception_text(exc, route=route),
                kind=FailureKind.BROWSER,
                url=url,
                route=route,
                retryable=True,
            ) from exc
        response = self._response(
            url=url,
            content=content,
            status_code=status,
            headers=headers,
            route=route,
            wire_bytes=response_bytes,
            response_bytes=response_bytes,
        )
        self._record_response(response)
        if is_chromium_error_page(response.text):
            self._drop_browser_session(client, route)
            error = WhoScoredTransportError(
                "FlareSolverr returned a Chromium network-error page",
                kind=FailureKind.BROWSER,
                url=url,
                route=route,
                retryable=True,
            )
            self._record_response_ledger(response, status="error", error=error)
            raise error
        if not bootstrap_url and route is TransportRoute.DIRECT_FLARESOLVERR:
            self._replay_browser_identity(solution)
        return response

    def _browser_fetch_many(
        self,
        requests: Sequence[FetchRequest],
        *,
        client: FlareSolverrClient,
        route: TransportRoute,
        proxy_url: Optional[str],
        bootstrap_url: str,
        request_ids: Mapping[str, str],
    ) -> list[_BrowserBatchOutcome]:
        """Execute a restricted XHR batch with explicit per-item outcomes."""

        items = tuple(requests)
        if not items:
            return []
        existing = self._browser_sessions.get(route)
        needs_bootstrap = existing is None or existing.bootstrap_url != bootstrap_url

        def record_error(kind: FailureKind, error: BaseException) -> None:
            for item in items:
                self._activate_request(
                    cache_key=item.cache_key,
                    scope=item.scope,
                    entity=item.entity,
                    request_id=request_ids[item.cache_key],
                )
                self._record_ledger(
                    url=item.url,
                    route=route,
                    status="error",
                    failure_kind=kind,
                    error=error,
                )

        try:
            session = self._browser_session(
                client,
                route=route,
                proxy_url=proxy_url,
                required_requests=len(items) + (1 if needs_bootstrap else 0),
            )
        except FlareSolverrRuntimeIdentityError as exc:
            record_error(FailureKind.CONFIG, exc)
            raise WhoScoredTransportError(
                _safe_route_exception_text(exc, route=route),
                kind=FailureKind.CONFIG,
                url=items[0].url,
                route=route,
                retryable=False,
            ) from exc
        except FlareSolverrCFChallengeFailed as exc:
            record_error(FailureKind.CLOUDFLARE, exc)
            raise CloudflareChallenge(
                _safe_route_exception_text(exc, route=route),
                url=items[0].url,
                route=route,
                source_wide=route is TransportRoute.DIRECT_FLARESOLVERR,
            ) from exc
        except FlareSolverrResponseTooLarge as exc:
            record_error(FailureKind.BUDGET, exc)
            raise TransportBudgetExceeded(
                _safe_route_exception_text(exc, route=route),
                url=items[0].url,
                route=route,
            ) from exc
        except FlareSolverrTimeout as exc:
            record_error(FailureKind.TIMEOUT, exc)
            raise WhoScoredTransportError(
                _safe_route_exception_text(exc, route=route),
                kind=FailureKind.TIMEOUT,
                url=items[0].url,
                route=route,
                retryable=True,
            ) from exc
        except FlareSolverrError as exc:
            record_error(FailureKind.BROWSER, exc)
            raise WhoScoredTransportError(
                _safe_route_exception_text(exc, route=route),
                kind=FailureKind.BROWSER,
                url=items[0].url,
                route=route,
                retryable=True,
            ) from exc

        try:
            if session.bootstrap_url != bootstrap_url:
                first = items[0]
                self._activate_request(
                    cache_key=first.cache_key,
                    scope=first.scope,
                    entity=first.entity,
                    request_id=request_ids[first.cache_key],
                )
                bootstrap_solution = client.get(
                    bootstrap_url,
                    session.session_id,
                    max_timeout_ms=self.browser_timeout_ms,
                    disable_media=True,
                )
                session.requests += 1
                bootstrap_content = str(bootstrap_solution.get("html") or "").encode(
                    "utf-8"
                )
                bootstrap_response = self._response(
                    url=bootstrap_url,
                    content=bootstrap_content,
                    status_code=int(bootstrap_solution.get("status") or 0),
                    headers={},
                    route=route,
                    wire_bytes=len(bootstrap_content),
                    response_bytes=len(bootstrap_content),
                )
                self._record_response(bootstrap_response)
                if is_chromium_error_page(bootstrap_response.text):
                    raise FlareSolverrErrorPage(
                        "FlareSolverr returned a Chromium network-error page"
                    )
                try:
                    self._validate(bootstrap_response, validator=None)
                except WhoScoredTransportError as exc:
                    if (
                        exc.kind is FailureKind.HTTP_STATUS
                        and bootstrap_response.status_code in {301, 302, 303, 307, 308}
                    ):
                        raise WhoScoredTransportError(
                            "WhoScored browser bootstrap returned a redirect",
                            kind=FailureKind.BROWSER,
                            url=bootstrap_url,
                            route=route,
                            status_code=bootstrap_response.status_code,
                            retryable=True,
                        ) from exc
                    raise
                if route is TransportRoute.DIRECT_FLARESOLVERR:
                    self._replay_browser_identity(bootstrap_solution)
                session.bootstrap_url = bootstrap_url

            self.stats.browser_batches += 1
            self.stats.browser_batch_items += len(items)
            solutions = client.xhr_get_many(
                [item.url for item in items],
                session.session_id,
                max_timeout_ms=self.browser_timeout_ms,
            )
            if len(solutions) != len(items):
                raise FlareSolverrError("FlareSolverr XHR batch is incomplete")
            session.requests += len(items)
        except FlareSolverrRuntimeIdentityError as exc:
            self._drop_browser_session(client, route)
            record_error(FailureKind.CONFIG, exc)
            raise WhoScoredTransportError(
                _safe_route_exception_text(exc, route=route),
                kind=FailureKind.CONFIG,
                url=items[0].url,
                route=route,
                retryable=False,
            ) from exc
        except FlareSolverrCFChallengeFailed as exc:
            self._drop_browser_session(client, route)
            record_error(FailureKind.CLOUDFLARE, exc)
            raise CloudflareChallenge(
                _safe_route_exception_text(exc, route=route),
                url=items[0].url,
                route=route,
                source_wide=route is TransportRoute.DIRECT_FLARESOLVERR,
            ) from exc
        except FlareSolverrResponseTooLarge as exc:
            self._drop_browser_session(client, route)
            record_error(FailureKind.BUDGET, exc)
            raise TransportBudgetExceeded(
                _safe_route_exception_text(exc, route=route),
                url=items[0].url,
                route=route,
            ) from exc
        except FlareSolverrTimeout as exc:
            self._drop_browser_session(client, route)
            record_error(FailureKind.TIMEOUT, exc)
            raise WhoScoredTransportError(
                _safe_route_exception_text(exc, route=route),
                kind=FailureKind.TIMEOUT,
                url=items[0].url,
                route=route,
                retryable=True,
            ) from exc
        except FlareSolverrTabCrashed as exc:
            self._drop_browser_session(client, route)
            record_error(FailureKind.BROWSER, exc)
            raise WhoScoredTransportError(
                _safe_route_exception_text(exc, route=route),
                kind=FailureKind.BROWSER,
                url=items[0].url,
                route=route,
                retryable=True,
            ) from exc
        except (FlareSolverrErrorPage, FlareSolverrError) as exc:
            self._drop_browser_session(client, route)
            record_error(FailureKind.BROWSER, exc)
            raise WhoScoredTransportError(
                _safe_route_exception_text(exc, route=route),
                kind=FailureKind.BROWSER,
                url=items[0].url,
                route=route,
                retryable=True,
            ) from exc

        outcomes: list[_BrowserBatchOutcome] = []
        drop_session = False
        for item, solution in zip(items, solutions):
            if solution.get("ok") is False:
                remote_kind = str(solution.get("kind") or "")
                failure_kind = (
                    FailureKind.TIMEOUT
                    if remote_kind == "timeout"
                    else (
                        FailureKind.BUDGET
                        if remote_kind == "response_too_large"
                        else FailureKind.BROWSER
                    )
                )
                error = WhoScoredTransportError(
                    f"restricted browser XHR item failed: {remote_kind}",
                    kind=failure_kind,
                    url=item.url,
                    route=route,
                    retryable=remote_kind in {"timeout", "fetch_failed"},
                )
                self._activate_request(
                    cache_key=item.cache_key,
                    scope=item.scope,
                    entity=item.entity,
                    request_id=request_ids[item.cache_key],
                )
                self._record_ledger(
                    url=item.url,
                    route=route,
                    status="error",
                    failure_kind=failure_kind,
                    error=error,
                )
                drop_session = drop_session or remote_kind in {
                    "timeout",
                    "fetch_failed",
                }
                outcomes.append(_BrowserBatchOutcome(error=error))
                continue
            if solution.get("ok") is not True:
                error = WhoScoredTransportError(
                    "restricted browser XHR item has invalid status",
                    kind=FailureKind.BROWSER,
                    url=item.url,
                    route=route,
                    retryable=False,
                )
                self._activate_request(
                    cache_key=item.cache_key,
                    scope=item.scope,
                    entity=item.entity,
                    request_id=request_ids[item.cache_key],
                )
                self._record_ledger(
                    url=item.url,
                    route=route,
                    status="error",
                    failure_kind=FailureKind.BROWSER,
                    error=error,
                )
                outcomes.append(_BrowserBatchOutcome(error=error))
                drop_session = True
                continue
            content = bytes(solution.get("content") or b"")
            response_bytes = int(solution.get("responseBytes") or len(content))
            response = self._response(
                url=item.url,
                content=content,
                status_code=int(solution.get("status") or 0),
                headers=dict(solution.get("headers") or {}),
                route=route,
                wire_bytes=response_bytes,
                response_bytes=response_bytes,
            )
            self._record_response(response)
            if is_chromium_error_page(response.text):
                error = WhoScoredTransportError(
                    "FlareSolverr returned a Chromium network-error page",
                    kind=FailureKind.BROWSER,
                    url=item.url,
                    route=route,
                    retryable=True,
                )
                self._activate_request(
                    cache_key=item.cache_key,
                    scope=item.scope,
                    entity=item.entity,
                    request_id=request_ids[item.cache_key],
                )
                self._record_response_ledger(response, status="error", error=error)
                outcomes.append(_BrowserBatchOutcome(error=error))
                drop_session = True
            else:
                outcomes.append(_BrowserBatchOutcome(response=response))
        if drop_session:
            self._drop_browser_session(client, route)
        return outcomes

    def _paid_fetch_many(
        self,
        items: Sequence[FetchRequest],
        *,
        browser_bootstrap_url: str,
        request_ids: Mapping[str, str],
    ) -> list[TransportResponse]:
        """Use the opt-in atomic gateway batch after all direct prechecks."""

        batch = tuple(items)
        if not 1 <= len(batch) <= MAX_PAID_GATEWAY_BATCH_URLS:
            raise WhoScoredTransportError(
                "paid gateway batch size is invalid",
                kind=FailureKind.CONFIG,
                url=batch[0].url if batch else "",
                route=TransportRoute.PAID_LEASE,
            )
        budgets = self.budgets
        logical_urls = [_logical_url_key(item.url) for item in batch]
        prospective = self.stats.paid_urls | set(logical_urls)
        if len(prospective) > budgets.max_paid_urls:
            raise TransportBudgetExceeded(
                f"paid URL limit reached ({budgets.max_paid_urls})",
                url=batch[0].url,
                route=TransportRoute.PAID_FLARESOLVERR,
            )
        remaining = budgets.max_paid_bytes_per_task - self.stats.paid_proxy_bytes
        per_url_remaining = sum(
            max(
                0,
                budgets.max_paid_bytes_per_url
                - self.stats.paid_proxy_bytes_by_url.get(logical_url, 0),
            )
            for logical_url in logical_urls
        )
        lease_budget = min(
            remaining,
            budgets.max_paid_bytes_per_lease,
            per_url_remaining,
        )
        if lease_budget <= 0:
            raise TransportBudgetExceeded(
                "paid task byte budget exhausted",
                url=batch[0].url,
                route=TransportRoute.PAID_FLARESOLVERR,
            )
        gateway = self._paid_gateway
        if gateway is None:
            raise WhoScoredTransportError(
                "paid application gateway is unavailable",
                kind=FailureKind.CONFIG,
                url=batch[0].url,
                route=TransportRoute.PAID_LEASE,
            )
        first = batch[0]
        self._activate_request(
            cache_key=first.cache_key,
            scope=first.scope,
            entity=first.entity,
            request_id=request_ids[first.cache_key],
        )
        try:
            paid = gateway.fetch_batch(
                [item.url for item in batch],
                context=self._active_context,
                max_response_bytes=budgets.max_response_bytes,
                max_provider_bytes=lease_budget,
                timeout_ms=min(60_000, max(1, int(self.browser_timeout_ms))),
                browser_bootstrap_url=browser_bootstrap_url,
            )
        except PaidGatewayRejected as exc:
            if isinstance(exc.receipt, PaidGatewayBatchReceipt):
                self._record_paid_gateway_batch_receipt(
                    batch, exc.receipt, request_ids=request_ids
                )
            raise WhoScoredTransportError(
                f"paid application gateway rejected batch: {exc.code}",
                kind=(
                    FailureKind.BUDGET
                    if exc.code == "budget_rejected"
                    else FailureKind.PROXY
                ),
                url=batch[0].url,
                route=TransportRoute.PAID_FLARESOLVERR,
                retryable=exc.code
                not in {"authority_rejected", "target_not_allowed"},
            ) from None
        except (PaidGatewayError, PaidGatewayProtocolError) as exc:
            raise WhoScoredTransportError(
                "paid application gateway batch failed: "
                f"{type(exc).__name__}",
                kind=FailureKind.PROXY,
                url=batch[0].url,
                route=TransportRoute.PAID_LEASE,
                retryable=True,
            ) from None
        self._record_paid_gateway_batch_receipt(
            batch, paid.receipt, request_ids=request_ids
        )
        responses: list[TransportResponse] = []
        for item, gateway_item in zip(batch, paid.results):
            endpoint_digest = hashlib.sha256(
                _canonical_url_key(item.url).encode("utf-8")
            ).hexdigest()
            attributed = paid.receipt.endpoint_provider_billed_bytes[endpoint_digest]
            if item is first:
                attributed += paid.receipt.bootstrap_provider_billed_bytes
            response = self._response(
                url=item.url,
                content=gateway_item.content,
                status_code=gateway_item.status_code,
                headers=gateway_item.headers,
                route=paid.route,
                wire_bytes=len(gateway_item.content),
                response_bytes=len(gateway_item.content),
                resource_bytes=attributed,
            )
            self._record_response(response)
            self._activate_request(
                cache_key=item.cache_key,
                scope=item.scope,
                entity=item.entity,
                request_id=request_ids[item.cache_key],
            )
            self._validate(response, item.validator)
            responses.append(response)
        if self.stats.paid_proxy_bytes > budgets.max_paid_bytes_per_task or any(
            self.stats.paid_proxy_bytes_by_url.get(logical_url, 0)
            > budgets.max_paid_bytes_per_url
            for logical_url in logical_urls
        ):
            raise TransportBudgetExceeded(
                "paid batch receipt exceeded a local byte ceiling",
                url=batch[0].url,
                route=TransportRoute.PAID_FLARESOLVERR,
            )
        return responses

    def _record_paid_gateway_batch_receipt(
        self,
        items: Sequence[FetchRequest],
        receipt: PaidGatewayBatchReceipt,
        *,
        request_ids: Mapping[str, str],
    ) -> None:
        if not receipt.close_complete or not receipt.cleanup_complete:
            raise PaidGatewayProtocolError(
                "paid gateway batch cleanup receipt is incomplete"
            )
        self.stats.paid_proxy_up_bytes += receipt.up_bytes
        self.stats.paid_proxy_down_bytes += receipt.down_bytes
        self._paid_browser_bootstraps += 1
        for index, item in enumerate(items):
            logical_url = _logical_url_key(item.url)
            digest = hashlib.sha256(
                _canonical_url_key(item.url).encode("utf-8")
            ).hexdigest()
            endpoint_bytes = receipt.endpoint_provider_billed_bytes[digest]
            attributed = endpoint_bytes + (
                receipt.bootstrap_provider_billed_bytes if index == 0 else 0
            )
            self.stats.paid_urls.add(logical_url)
            self.stats.paid_proxy_bytes_by_url[logical_url] = (
                self.stats.paid_proxy_bytes_by_url.get(logical_url, 0) + attributed
            )
        first = items[0]
        self._activate_request(
            cache_key=first.cache_key,
            scope=first.scope,
            entity=first.entity,
            request_id=request_ids[first.cache_key],
        )
        self._record_ledger(
            url=first.url,
            route=TransportRoute.PAID_LEASE,
            status="accounted",
            request_bytes=receipt.up_bytes,
            response_bytes=receipt.down_bytes,
            resource_bytes=receipt.total_bytes,
            paid_proxy_bytes=receipt.provider_billed_bytes,
            extra={
                "lease_id_hash": receipt.lease_id_hash,
                "paid_routes_attempted": [receipt.route.value],
                "final_paid_route": receipt.route.value,
                "gateway_cleanup_complete": True,
                "gateway_target_manifest_sha256": receipt.target_manifest_sha256,
                "gateway_endpoint_provider_bytes": dict(
                    receipt.endpoint_provider_billed_bytes
                ),
                "gateway_bootstrap_provider_bytes": (
                    receipt.bootstrap_provider_billed_bytes
                ),
            },
        )

    def _paid_fetch(
        self,
        url: str,
        validator: Optional[Validator],
        *,
        cache_key: str,
        browser_bootstrap_url: Optional[str] = None,
    ) -> TransportResponse:
        budgets = self.budgets
        logical_url = _logical_url_key(url)
        if (
            logical_url not in self.stats.paid_urls
            and len(self.stats.paid_urls) >= budgets.max_paid_urls
        ):
            raise TransportBudgetExceeded(
                f"paid URL limit reached ({budgets.max_paid_urls})",
                url=url,
                route=TransportRoute.PAID_HTTP,
            )
        remaining = budgets.max_paid_bytes_per_task - self.stats.paid_proxy_bytes
        url_remaining = (
            budgets.max_paid_bytes_per_url
            - self.stats.paid_proxy_bytes_by_url.get(logical_url, 0)
        )
        lease_budget = min(
            url_remaining,
            budgets.max_paid_bytes_per_lease,
            remaining,
        )
        if lease_budget <= 0:
            raise TransportBudgetExceeded(
                "paid task byte budget exhausted",
                url=url,
                route=TransportRoute.PAID_HTTP,
            )
        gateway = self._paid_gateway
        if gateway is None:
            raise WhoScoredTransportError(
                "paid application gateway is unavailable",
                kind=FailureKind.CONFIG,
                url=url,
                route=TransportRoute.PAID_LEASE,
                retryable=False,
            )
        try:
            paid = gateway.fetch(
                url,
                context=self._active_context,
                max_response_bytes=budgets.max_response_bytes,
                max_provider_bytes=lease_budget,
                timeout_ms=min(60_000, max(1, int(self.browser_timeout_ms))),
                browser_bootstrap_url=browser_bootstrap_url,
            )
        except PaidGatewayRejected as exc:
            if exc.receipt is not None:
                self.stats.paid_urls.add(logical_url)
                self._record_paid_gateway_receipt(logical_url, exc.receipt)
                if (
                    self.stats.paid_proxy_bytes > budgets.max_paid_bytes_per_task
                    or self.stats.paid_proxy_bytes_by_url.get(logical_url, 0)
                    > budgets.max_paid_bytes_per_url
                ):
                    raise TransportBudgetExceeded(
                        "paid gateway receipt exceeded a local byte ceiling",
                        url=url,
                        route=exc.receipt.route,
                    ) from None
            if exc.code == "budget_rejected":
                raise TransportBudgetExceeded(
                    str(exc),
                    url=url,
                    route=(
                        exc.receipt.route
                        if exc.receipt is not None
                        else TransportRoute.PAID_HTTP
                    ),
                ) from None
            raise WhoScoredTransportError(
                f"paid application gateway rejected fetch: {exc.code}",
                kind=FailureKind.PROXY,
                url=url,
                route=(
                    exc.receipt.route
                    if exc.receipt is not None
                    else TransportRoute.PAID_LEASE
                ),
                retryable=exc.code not in {"authority_rejected", "target_not_allowed"},
            ) from None
        except (PaidGatewayError, PaidGatewayProtocolError) as exc:
            raise WhoScoredTransportError(
                "paid application gateway fetch failed: "
                f"{type(exc).__name__}",
                kind=FailureKind.PROXY,
                url=url,
                route=TransportRoute.PAID_LEASE,
                retryable=True,
            ) from None
        self.stats.paid_urls.add(logical_url)
        receipt = paid.receipt
        self._record_paid_gateway_receipt(logical_url, receipt)
        result = self._response(
            url=url,
            content=paid.content,
            status_code=paid.status_code,
            headers=paid.headers,
            route=paid.route,
            wire_bytes=len(paid.content),
            request_bytes=receipt.up_bytes,
            response_bytes=len(paid.content),
            resource_bytes=receipt.total_bytes,
        )
        self._record_response(result)
        self._validate(result, validator)
        if self.stats.paid_proxy_bytes > budgets.max_paid_bytes_per_task:
            raise TransportBudgetExceeded(
                "paid task byte budget exceeded",
                url=url,
                route=result.route,
            )
        if (
            self.stats.paid_proxy_bytes_by_url.get(logical_url, 0)
            > budgets.max_paid_bytes_per_url
        ):
            raise TransportBudgetExceeded(
                "paid per-URL byte budget exceeded",
                url=url,
                route=result.route,
            )
        return result

    def _record_paid_gateway_receipt(
        self,
        url: str,
        receipt: PaidGatewayReceipt,
    ) -> None:
        """Account only a credential-free receipt from a fully cleaned fetch."""

        if not receipt.close_complete or not receipt.cleanup_complete:
            raise PaidGatewayProtocolError(
                "paid gateway cleanup receipt is incomplete"
            )
        self.stats.paid_proxy_up_bytes += receipt.up_bytes
        self.stats.paid_proxy_down_bytes += receipt.down_bytes
        self.stats.paid_proxy_bytes_by_url[url] = (
            self.stats.paid_proxy_bytes_by_url.get(url, 0) + receipt.total_bytes
        )
        if receipt.route is TransportRoute.PAID_FLARESOLVERR:
            self._paid_browser_bootstraps += 1
        self._record_ledger(
            url=url,
            route=TransportRoute.PAID_LEASE,
            status="accounted",
            request_bytes=receipt.up_bytes,
            response_bytes=receipt.down_bytes,
            resource_bytes=receipt.total_bytes,
            paid_proxy_bytes=receipt.provider_billed_bytes,
            extra={
                "lease_id_hash": receipt.lease_id_hash,
                "paid_routes_attempted": [receipt.route.value],
                "final_paid_route": receipt.route.value,
                "gateway_cleanup_complete": True,
            },
        )

    def _record_paid_lease(
        self,
        url: str,
        stats: Mapping[str, Any],
        *,
        expected_lease_id: str,
        routes_attempted: Sequence[TransportRoute],
        final_route: Optional[TransportRoute],
    ) -> None:
        def exact_nonnegative_integer(field: str) -> int:
            value = stats.get(field)
            if isinstance(value, bool) or not isinstance(value, int) or value < 0:
                raise ValueError(
                    f"filtering proxy lease {field} must be a non-negative integer"
                )
            return value

        reported_lease_id = stats.get("id")
        if (
            not isinstance(reported_lease_id, str)
            or reported_lease_id != expected_lease_id
        ):
            raise ValueError("filtering proxy lease id does not match the active lease")
        reported_url = stats.get("canonical_url")
        if not isinstance(reported_url, str) or not reported_url.strip():
            raise ValueError("filtering proxy lease canonical_url is missing")
        if _canonical_url_key(reported_url) != url:
            raise ValueError(
                "filtering proxy lease canonical_url does not match the request"
            )
        if stats.get("close_complete") is not True:
            raise ValueError(
                "filtering proxy lease did not durably complete close accounting"
            )

        up = exact_nonnegative_integer("up_bytes")
        down = exact_nonnegative_integer("down_bytes")
        total = exact_nonnegative_integer("total_bytes")
        provider_billed = exact_nonnegative_integer("provider_billed_bytes")
        if total != up + down:
            raise ValueError(
                "filtering proxy lease total_bytes does not equal up_bytes + down_bytes"
            )
        if provider_billed != total:
            raise ValueError(
                "filtering proxy provider_billed_bytes does not equal total_bytes"
            )
        self.stats.paid_proxy_up_bytes += up
        self.stats.paid_proxy_down_bytes += down
        self.stats.paid_proxy_bytes_by_url[url] = (
            self.stats.paid_proxy_bytes_by_url.get(url, 0) + up + down
        )
        self._record_ledger(
            url=url,
            # One lease can include the HTTP challenge response and the final
            # browser request. Its byte counter cannot be split exactly by
            # source route, so never mislabel the aggregate as paid_http.
            route=TransportRoute.PAID_LEASE,
            status="accounted",
            request_bytes=up,
            response_bytes=down,
            resource_bytes=up + down,
            paid_proxy_bytes=up + down,
            extra={
                "lease_id_hash": hashlib.sha256(
                    expected_lease_id.encode("utf-8")
                ).hexdigest(),
                "paid_routes_attempted": [route.value for route in routes_attempted],
                "final_paid_route": final_route.value if final_route else "",
            },
        )

    def _response(
        self,
        *,
        url: str,
        content: bytes,
        status_code: int,
        headers: Mapping[str, str],
        route: TransportRoute,
        wire_bytes: int,
        request_bytes: int = 0,
        response_bytes: Optional[int] = None,
        resource_bytes: int = 0,
    ) -> TransportResponse:
        return TransportResponse(
            url=url,
            content=content,
            status_code=status_code,
            headers=headers,
            route=route,
            wire_bytes=wire_bytes,
            sha256=hashlib.sha256(content).hexdigest(),
            request_bytes=max(0, int(request_bytes)),
            response_bytes=(
                max(0, int(response_bytes))
                if response_bytes is not None
                else max(0, int(wire_bytes))
            ),
            resource_bytes=max(0, int(resource_bytes)),
            # Bind raw ordering before parser and S3 commit work.  Supported
            # production uses one LocalExecutor host and a per-target lock;
            # this timestamp also prevents a slow commit from being mistaken
            # for a newer source observation.
            observed_at=datetime.now(timezone.utc).isoformat(),
        )

    def _record_response(self, response: TransportResponse) -> None:
        self.stats.route_requests[response.route.value] += 1
        self.stats.route_wire_bytes[response.route.value] += response.wire_bytes

    def _validate(
        self,
        response: TransportResponse,
        validator: Optional[Validator],
        *,
        record: bool = True,
    ) -> None:
        try:
            self._validate_impl(response, validator)
        except WhoScoredTransportError as exc:
            if record:
                self._record_response_ledger(response, status="error", error=exc)
            raise
        if record:
            self._record_response_ledger(response, status="success")

    def _validate_impl(
        self, response: TransportResponse, validator: Optional[Validator]
    ) -> None:
        if len(response.content) > self.budgets.max_response_bytes:
            raise TransportBudgetExceeded(
                f"response exceeded {self.budgets.max_response_bytes} bytes",
                url=response.url,
                route=response.route,
            )
        soft_origin_status = (
            _cloudflare_origin_error_status(response.content)
            if response.route
            in (
                TransportRoute.DIRECT_FLARESOLVERR,
                TransportRoute.PAID_FLARESOLVERR,
            )
            and response.status_code == 200
            else None
        )
        if soft_origin_status is not None:
            raise WhoScoredTransportError(
                f"HTTP {soft_origin_status} rendered as HTTP 200",
                kind=FailureKind.HTTP_STATUS,
                url=response.url,
                route=response.route,
                status_code=soft_origin_status,
                retryable=True,
            )
        browser_challenge = (
            response.route
            in (
                TransportRoute.DIRECT_FLARESOLVERR,
                TransportRoute.PAID_FLARESOLVERR,
            )
            and response.status_code in (403, 429, 503)
            and _has_cloudflare_challenge_markup(response.content)
        )
        structured_access_gate = is_whoscored_structured_feed_access_gate(
            response.url,
            response.status_code,
            response.content,
            response.headers,
        )
        if structured_access_gate and not browser_challenge:
            if response.route in {
                TransportRoute.DIRECT_HTTP,
                TransportRoute.PAID_HTTP,
            }:
                # This stable origin mask is only route-selection evidence:
                # the same feed may succeed inside its official stage page.
                # On PAID_HTTP the lease was already authorised by independent
                # direct-browser CF evidence plus a fresh direct recheck; this
                # exception only advances that same lease to paid browser XHR.
                raise CloudflareChallenge(
                    "WhoScored structured-feed HTTP access gate",
                    url=response.url,
                    route=response.route,
                    status_code=response.status_code,
                    source_wide=False,
                )
            # Seeing the mask in a browser is not a Cloudflare challenge and
            # must never count toward paid-route authority or shared cooldown.
            raise WhoScoredTransportError(
                "WhoScored structured-feed access gate rendered in browser",
                kind=FailureKind.CONTENT,
                url=response.url,
                route=response.route,
                status_code=response.status_code,
            )
        if browser_challenge or is_cloudflare_response(
            response.status_code, response.headers, response.content
        ):
            raise CloudflareChallenge(
                "Cloudflare challenge response",
                url=response.url,
                route=response.route,
                status_code=response.status_code,
                source_wide=bool(
                    response.route is TransportRoute.DIRECT_FLARESOLVERR
                    and browser_challenge
                ),
            )
        if not 200 <= response.status_code < 300:
            raise WhoScoredTransportError(
                f"HTTP {response.status_code}",
                kind=FailureKind.HTTP_STATUS,
                url=response.url,
                route=response.route,
                status_code=response.status_code,
                retryable=response.status_code >= 500,
            )
        if not response.content:
            raise WhoScoredTransportError(
                "empty response",
                kind=FailureKind.CONTENT,
                url=response.url,
                route=response.route,
            )
        if validator is None:
            return
        try:
            accepted = validator(response)
        except WhoScoredTransportError:
            raise
        except Exception as exc:
            raise WhoScoredTransportError(
                "content validator failed: "
                f"{_safe_route_exception_text(exc, route=response.route)}",
                kind=FailureKind.CONTENT,
                url=response.url,
                route=response.route,
            ) from exc
        if accepted is False:
            raise WhoScoredTransportError(
                "content validator rejected response",
                kind=FailureKind.CONTENT,
                url=response.url,
                route=response.route,
            )

    def _record_response_ledger(
        self,
        response: TransportResponse,
        *,
        status: str,
        error: Optional[WhoScoredTransportError] = None,
    ) -> None:
        self._record_ledger(
            url=response.url,
            route=response.route,
            status=status,
            status_code=response.status_code,
            failure_kind=error.kind if error is not None else None,
            error=error,
            cache_hit=response.route is TransportRoute.RAW_CACHE,
            raw_sha256=response.sha256,
            request_bytes=response.request_bytes,
            response_bytes=response.response_bytes,
            resource_bytes=response.resource_bytes,
        )

    def _record_ledger(
        self,
        *,
        url: str,
        route: TransportRoute,
        status: str,
        status_code: Optional[int] = None,
        failure_kind: Optional[FailureKind] = None,
        error: Optional[BaseException] = None,
        cache_hit: bool = False,
        raw_sha256: str = "",
        request_bytes: int = 0,
        response_bytes: int = 0,
        resource_bytes: int = 0,
        paid_proxy_bytes: int = 0,
        extra: Optional[Mapping[str, Any]] = None,
    ) -> None:
        if self.request_ledger is None:
            return
        event: dict[str, Any] = {
            "event_version": "whoscored-request-v1",
            "event_id": uuid.uuid4().hex,
            "occurred_at": datetime.now(timezone.utc).isoformat(),
            "request_id": self._active_request_id,
            **self._active_context.as_dict(),
            "cache_key": self._active_cache_key,
            "url": _canonical_url_key(url),
            "route": route.value,
            "status": status,
            "status_code": status_code,
            "failure_kind": failure_kind.value if failure_kind else "",
            "error_type": type(error).__name__ if error is not None else "",
            "cache_hit": cache_hit,
            "raw_sha256": raw_sha256,
            "request_bytes": max(0, int(request_bytes)),
            "response_bytes": max(0, int(response_bytes)),
            "resource_bytes": max(0, int(resource_bytes)),
            "paid_proxy_bytes": max(0, int(paid_proxy_bytes)),
        }
        if extra:
            event.update(dict(extra))
        try:
            self.request_ledger.append(event)
        except Exception as exc:
            # Missing telemetry means paid limits cannot be audited. Fail the
            # request instead of silently serving unaccounted production data.
            raise WhoScoredTransportError(
                "request ledger append failed: "
                f"{_safe_route_exception_text(exc, route=route)}",
                kind=FailureKind.CONFIG,
                url=url,
                route=route,
                retryable=True,
            ) from exc

    def get_traffic_stats(self) -> dict[str, Any]:
        return self.stats.as_dict()

    def close(self) -> None:
        if self._source_circuit is not None and self._source_circuit_permit is not None:
            permit = self._source_circuit_permit
            self._source_circuit_permit = None
            try:
                self._source_circuit.abandon(permit)
            except SourceCircuitError:
                logger.error("WhoScored source circuit probe cleanup failed")
        self._drop_browser_session(self._direct_fs, TransportRoute.DIRECT_FLARESOLVERR)
        if self._paid_fs is not None:
            self._drop_browser_session(
                self._paid_fs, TransportRoute.PAID_FLARESOLVERR
            )
        self._direct_gate_circuits.clear()
        resources = [self._direct_http, self._direct_fs, self._paid_fs]
        seen: set[int] = set()
        for resource in resources:
            if id(resource) in seen:
                continue
            seen.add(id(resource))
            close = getattr(resource, "close", None)
            if callable(close):
                close()
        gateway_close = getattr(self._paid_gateway, "close", None)
        if callable(gateway_close):
            gateway_close()
        proxy_close = getattr(self._proxy_client, "close_session", None)
        if callable(proxy_close):
            proxy_close()

    def __enter__(self) -> "WhoScoredTransport":
        return self

    def __exit__(self, exc_type: Any, exc: Any, tb: Any) -> bool:
        self.close()
        return False


def _response_wire_bytes(response: Any, content: bytes) -> int:
    """Best available receive size; curl_cffi exposes ``response_size``."""

    for value in (
        # curl_cffi's response_size is download_size + HTTP header_size.
        getattr(response, "response_size", None),
        getattr(response, "download_size", None),
        getattr(response, "downloaded", None),
    ):
        try:
            if value is not None and int(value) >= 0:
                return int(value)
        except (TypeError, ValueError):
            pass
    return len(content)


def _request_wire_bytes(response: Any) -> int:
    """Best available request/upload transfer size exposed by curl clients."""
    for value in (
        # curl_cffi's request_size includes the request headers and body. A GET
        # still exposes upload_size=0, so checking upload_size first would hide
        # every non-empty HTTP request.
        getattr(response, "request_size", None),
        getattr(response, "upload_size", None),
        getattr(response, "uploaded", None),
    ):
        try:
            if value is not None and int(value) >= 0:
                return int(value)
        except (TypeError, ValueError):
            pass
    return 0


def _logical_url_key(url: str) -> str:
    return _canonical_url_key(url)


def _canonical_url_key(url: str) -> str:
    parts = urlsplit(url)
    if parts.scheme and parts.netloc:
        query = urlencode(sorted(parse_qsl(parts.query, keep_blank_values=True)))
        return urlunsplit(
            (
                parts.scheme.lower(),
                parts.netloc.lower(),
                parts.path or "/",
                query,
                "",
            )
        )
    return url.split("?", 1)[0].split("#", 1)[0]


def _entity_from_cache_key(cache_key: str) -> str:
    if cache_key.startswith("whoscored:"):
        tokens = cache_key.split(":", 2)
        if len(tokens) > 1:
            return tokens[1]
    return ""


__all__ = [
    "CachedPayload",
    "CloudflareChallenge",
    "FailureKind",
    "JsonlRequestLedger",
    "PaidCampaignContext",
    "PaidGatewayClient",
    "PaidGatewayError",
    "PaidGatewayProtocolError",
    "PaidGatewayRejected",
    "ProxyFilterClient",
    "ProxyBudgetRejected",
    "ProxyConcurrencyLimited",
    "ProxyCampaignControlRejected",
    "ProxyLease",
    "RawCacheHook",
    "RequestLedger",
    "TransportBudgetExceeded",
    "TransportBudgets",
    "TransportResponse",
    "TransportRoute",
    "TransportStats",
    "TransportContext",
    "WhoScoredTransport",
    "WhoScoredTransportError",
    "is_cloudflare_response",
]
