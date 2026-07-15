#!/usr/bin/env python3
"""Filtering passthrough proxy for FlareSolverr residential traffic (#652).

Chain: Chrome (FlareSolverr) -> THIS proxy -> residential upstream -> internet.

The only lever FlareSolverr (Chromium) leaves us to cut residential-proxy traffic is
domain-level blocking: we cannot MITM the TLS (that swaps Chrome's TLS fingerprint on
the proxy->Cloudflare leg and breaks the CF bypass), so this proxy does a pure TCP
tunnel for CONNECT and only inspects the CONNECT *host*. Hosts on the ad-tech blocklist
are refused (403, never dialed upstream) so their bytes are never billed; everything
else (the target site, Cloudflare challenge, fonts) tunnels through unchanged.

The observe run (no --blocklist) showed ~90% of WhoScored and ~60% of SoFIFA
residential bytes are third-party ad-tech — see docs/research/flaresolverr-proxy-traffic-audit.md.

Run as the dedicated compose service (reaches the residential pool and Airflow):
    python scripts/proxy_filter/filter_proxy.py \
        --listen 0.0.0.0:8899 --lease-listen 0.0.0.0:8900 \
        --blocklist configs/proxy_filter/blocklist.txt

``POST /v1/leases`` on port 8899 returns a short-lived token.  Paid proxy
traffic goes to port 8900 with Basic auth ``lease:<token>``; the service pins
one upstream for the lease and never returns provider credentials to callers.
Port 8899 retains the credential-less legacy proxy route during migration.
"""

import argparse
import asyncio
import base64
import binascii
import fcntl
import hashlib
import ipaddress
import json
import logging
import math
import os
import re
import secrets
import signal
import sys
import time
from collections import defaultdict
from dataclasses import dataclass, field
from datetime import datetime, timezone
from typing import Any, Mapping, Sequence
from urllib.parse import parse_qsl, urlencode, urlsplit, urlunsplit

_REPO_ROOT = os.path.dirname(
    os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
)
for _import_root in (_REPO_ROOT, "/opt/airflow"):
    if _import_root not in sys.path:
        sys.path.insert(0, _import_root)

from scripts.proxy_filter.budget import (  # noqa: E402 - standalone entry point
    ProductionBudgetUnavailable,
    SharedBudgetLedger,
    experimental_canary_policy_id,
    load_verified_policy,
)
from scrapers.sofascore.workload_plan import (  # noqa: E402 - standalone entry point
    AllocationAccountingError,
    AllocationBudgetExceeded,
    AllocationClaim,
    AllocationError,
    AllocationLedger,
    SignedDagRunPlan,
    WorkloadAllocation,
    WorkloadPolicyUnavailable,
    WorkloadPlanError,
    WORKLOAD_METER,
    load_verified_workload_policy,
)

logging.basicConfig(
    level=logging.INFO, format="%(asctime)s %(levelname)s filter_proxy: %(message)s"
)
log = logging.getLogger("filter_proxy")
PROVIDER_METER_ID = "proxy_filter_provider_path_v2"

# billable residential bytes, per target host (sum of both tunnel directions)
up_bytes: dict[str, int] = defaultdict(int)
down_bytes: dict[str, int] = defaultdict(int)
conn_count: dict[str, int] = defaultdict(int)
blocked_count: dict[str, int] = defaultdict(int)

BLOCKLIST: set[str] = set()
provider_budget_guard = None
provider_budget_endpoint: str | None = None

# The explicit lease path is shared by WhoScored, Transfermarkt and SofaScore.
# A lease pins exactly one pool entry, has a one-time bearer token, and is
# accounted independently. This replaces the unreliable ``_active == 0``
# proxy selection heuristic for all
# callers which authenticate as ``lease:<token>``.  Credential-less clients are
# still accepted through the legacy path so existing SoFIFA deployments are not
# broken during migration.
DEFAULT_LEASE_BYTES = 8 * 1024 * 1024
MAX_LEASE_BYTES = 24 * 1024 * 1024
DEFAULT_LEASE_TTL_SECONDS = 60
MAX_LEASE_TTL_SECONDS = 3600
DAILY_BUDGET_BYTES = 100 * 1024 * 1024
DAGRUN_BUDGET_BYTES = 8_000_000
TRANSFERMARKT_DAGRUN_BUDGET_BYTES = 15_728_640
TRANSFERMARKT_DAG_IDS = frozenset(
    {
        "dag_ingest_transfermarkt",
        "dag_discover_transfermarkt_registry",
    }
)
FBREF_DAG_IDS = frozenset({"dag_ingest_fbref", "dag_backfill_fbref"})
SOFASCORE_DAG_IDS = frozenset({"dag_ingest_sofascore"})
SOFASCORE_CANARY_DAG_IDS = frozenset({"dag_canary_sofascore_proxy"})
# Registry discovery is a non-signed, metered JSON scan of the public catalog.
# It carries no workload plan and no allocation: its only bound is this DagRun
# cap, which stays zero (fail-closed) until an operator authorizes a scan.
SOFASCORE_DISCOVERY_DAG_IDS = frozenset({"dag_discover_sofascore_registry"})
SOFASCORE_DISCOVERY_DAGRUN_BUDGET_BYTES = 0
# Zero is deliberately fail-closed.  ``main`` replaces it only after loading a
# verified SofaScore canary; there is no hand-written production allowance.
SOFASCORE_DAGRUN_BUDGET_BYTES = 0
SOFASCORE_BUDGET_ARTIFACT_ID = ""
SOFASCORE_CANARY_HARD_CAP_BYTES = 0
SOFASCORE_CANARY_POLICY_ID = ""
URL_BUDGET_BYTES = 2_000_000
MAX_ACTIVE_LEASES = 4
LEASE_PROXY_URL = "http://proxy_filter:8900"
LEDGER_PATH = "/opt/airflow/logs/proxy_filter/paid_requests.jsonl"
SOFASCORE_ALLOCATION_LEDGER_PATH = (
    "/opt/airflow/logs/proxy_filter/sofascore_allocations.json"
)
SOFASCORE_ALLOCATION_WAL_PATH = (
    "/opt/airflow/logs/proxy_filter/sofascore_allocation_claims.jsonl"
)
SOFASCORE_PARENT_ENVELOPE_PATH = (
    "/opt/airflow/logs/proxy_filter/sofascore_parent_envelopes.json"
)
MAX_LEDGER_EVENT_BYTES = 256 * 1024
MAX_ALLOCATION_WAL_EVENT_BYTES = 4 * 1024 * 1024
MAX_CONTROL_BODY_BYTES = 4 * 1024 * 1024
MAX_PROVIDER_RESPONSE_HEAD_BYTES = 64 * 1024
# A dead residential exit accepts the TCP CONNECT and then goes silent.  Without
# these bounds the lease's only tunnel hangs forever (byte-metered head read),
# never drains ``active_tunnels`` and latches the single serial SofaScore slot
# (#946).  The connect/head reads are bounded, and a re-pin to a fresh exit is
# allowed only before the first provider byte has been billed.
LEASE_UPSTREAM_CONNECT_TIMEOUT_SECONDS = 5.0
LEASE_PROVIDER_HEAD_TIMEOUT_SECONDS = 4.0
LEASE_UPSTREAM_FAILOVER_ATTEMPTS = 2
SOFASCORE_CANARY_EXIT_PROBE_HOST = "api.ipify.org"
CONTROL_TOKEN = ""
SOFASCORE_ALLOCATION_LEDGER: AllocationLedger | None = None
_SOFASCORE_ALLOCATION_LEDGER_KEY: tuple[str, str] | None = None
SOFASCORE_PARENT_ENVELOPE_LEDGER: "ParentRunEnvelopeLedger | None" = None
_SOFASCORE_PARENT_ENVELOPE_LEDGER_PATH = ""
SOFASCORE_CHALLENGE_HOSTS = frozenset(
    {"challenges.cloudflare.com", "turnstile.cloudflare.com"}
)
FBREF_ALLOWED_HOSTS = frozenset(
    {
        "api.ipify.org",
        "challenges.cloudflare.com",
        "fbref.com",
        "ipinfo.io",
        "turnstile.cloudflare.com",
    }
)
_SENSITIVE_QUERY_KEYS = frozenset(
    {
        "access_token",
        "api_key",
        "apikey",
        "auth",
        "authorization",
        "cookie",
        "key",
        "password",
        "secret",
        "session",
        "signature",
        "token",
    }
)
PROXY_POOL_ENV = "PROXY_POOL_JSON"
MAX_PROXY_POOL_JSON_BYTES = 1024 * 1024
MAX_PROXY_POOL_ENTRIES = 1000
_PROXY_POOL_FIELDS = frozenset({"host", "port", "username", "password"})
_DNS_LABEL = re.compile(r"^[A-Za-z0-9](?:[A-Za-z0-9-]{0,61}[A-Za-z0-9])?$")


class ProxyPoolConfigurationError(ValueError):
    """A redaction-safe proxy-pool configuration error."""


class _DuplicateJsonField(ValueError):
    """Internal marker for duplicate JSON object fields."""


class UpstreamHeadTimeout(RuntimeError):
    """The residential upstream accepted the CONNECT but never sent a head."""


class UpstreamHeadIncomplete(RuntimeError):
    """The residential upstream closed before a complete response head arrived."""


class _LeaseBudgetRefused(Exception):
    """Internal marker: a lease write was refused by its own byte budget.

    Distinct from a dead-exit failure — the upstream is fine, the lease is out
    of allowance — so it must surface as the existing 429, never a failover.
    """


# Test seam for the residential dial.  Every point that opens an upstream
# connection goes through this name so the connect can be bounded and, for a
# lease, retargeted to a fresh exit without touching the pool selection logic.
_open_upstream_connection = asyncio.open_connection


def _dagrun_budget_bytes(dag_id: str) -> int:
    """Return the source-specific hard cap without weakening WhoScored."""
    if dag_id in SOFASCORE_CANARY_DAG_IDS:
        return SOFASCORE_CANARY_HARD_CAP_BYTES
    if dag_id in SOFASCORE_DISCOVERY_DAG_IDS:
        return SOFASCORE_DISCOVERY_DAGRUN_BUDGET_BYTES
    if dag_id in SOFASCORE_DAG_IDS:
        return SOFASCORE_DAGRUN_BUDGET_BYTES
    if dag_id in TRANSFERMARKT_DAG_IDS:
        return TRANSFERMARKT_DAGRUN_BUDGET_BYTES
    return DAGRUN_BUDGET_BYTES


def _source_for_dag(dag_id: str) -> str:
    if dag_id in SOFASCORE_CANARY_DAG_IDS:
        return "sofascore_canary"
    if dag_id in SOFASCORE_DISCOVERY_DAG_IDS:
        return "sofascore_discovery"
    if dag_id in SOFASCORE_DAG_IDS:
        return "sofascore"
    if dag_id in TRANSFERMARKT_DAG_IDS:
        return "transfermarkt"
    if dag_id in FBREF_DAG_IDS:
        return "fbref"
    if dag_id == "dag_ingest_whoscored":
        return "whoscored"
    return ""


def _canary_policy_id(hard_cap_bytes: int) -> str:
    """Hash the explicit experimental policy without pretending it is verified."""
    return experimental_canary_policy_id(hard_cap_bytes)


def _upstream_fingerprint(upstream: tuple[str, int, str, str]) -> str:
    """Return a non-reversible pool-entry identifier, never credentials."""
    return hashlib.sha256(f"{upstream[0]}:{upstream[1]}".encode("utf-8")).hexdigest()[
        :16
    ]


def _lease_budget_policy_id(lease: "Lease") -> str:
    if lease.source == "sofascore":
        return SOFASCORE_BUDGET_ARTIFACT_ID
    if lease.source == "sofascore_canary":
        return SOFASCORE_CANARY_POLICY_ID
    return ""


def _wall_time() -> float:
    """Wall clock seam used by lease TTL checks and deterministic tests."""

    return time.time()


@dataclass
class Lease:
    lease_id: str
    token: str = field(repr=False)
    upstream: tuple[str, int, str, str] = field(repr=False)
    created_at: float
    expires_at: float
    max_bytes: int
    dag_id: str = ""
    run_id: str = ""
    task_id: str = ""
    map_index: int = -1
    try_number: int = 0
    scope: str = ""
    capture_scope: str = ""
    entity: str = ""
    canonical_url: str = ""
    source: str = ""
    workload_plan: SignedDagRunPlan | None = field(default=None, repr=False)
    allocation_claim: AllocationClaim | None = field(default=None, repr=False)
    allocation_id: str = ""
    workload_class: str = ""
    allocation_batch_index: int = -1
    allocation_units: tuple[str, ...] = ()
    allocation_budget_bytes: int = 0
    run_cap_bytes: int = 0
    base_run_id: str = ""
    workload_phase: str = ""
    parent_run_cap_bytes: int = 0
    parent_run_spent_provider_bytes: int = 0
    current_request_id: str = field(default="", repr=False)
    current_endpoint: str = ""
    current_request_start_bytes: int = 0
    endpoint_request_provider_bytes: dict[str, list[int]] = field(default_factory=dict)
    proxy_exit_hash: str | None = None
    allocation_finished: bool = False
    up_bytes: int = 0
    down_bytes: int = 0
    reserved_bytes: int = 0
    active_tunnels: int = 0
    upstream_repins: int = 0
    closed: bool = False
    close_recorded: bool = False
    budget_exceeded: bool = False
    hosts: dict[str, dict[str, int]] = field(default_factory=dict)
    tunnel_writers: set[Any] = field(default_factory=set, repr=False)

    @property
    def total_bytes(self) -> int:
        return self.up_bytes + self.down_bytes

    @property
    def expired(self) -> bool:
        return _wall_time() >= self.expires_at

    @property
    def usable(self) -> bool:
        return not self.closed and not self.expired and not self.budget_exceeded

    def report(self) -> dict[str, Any]:
        return {
            "meter": PROVIDER_METER_ID,
            "id": self.lease_id,
            "created_at": self.created_at,
            "expires_at": self.expires_at,
            "max_bytes": self.max_bytes,
            "up_bytes": self.up_bytes,
            "down_bytes": self.down_bytes,
            "total_bytes": self.total_bytes,
            "active_tunnels": self.active_tunnels,
            "reserved_bytes": self.reserved_bytes,
            "upstream_repins": self.upstream_repins,
            "closed": self.closed,
            "expired": self.expired,
            "budget_exceeded": self.budget_exceeded,
            "hosts": self.hosts,
            "dag_id": self.dag_id,
            "run_id": self.run_id,
            "task_id": self.task_id,
            "map_index": self.map_index,
            "try_number": self.try_number,
            "scope": self.scope,
            "capture_scope": self.capture_scope,
            "entity": self.entity,
            "canonical_url": self.canonical_url,
            "source": self.source,
            "upstream_fingerprint": _upstream_fingerprint(self.upstream),
            "dagrun_total_bytes": _run_total_bytes(self.dagrun_key),
            "dagrun_budget_bytes": _lease_dagrun_budget_bytes(self),
            "url_total_bytes": _url_total_bytes(self.dagrun_key, self.canonical_url),
            "url_budget_bytes": _lease_url_budget_bytes(self),
            "budget_artifact_id": _lease_budget_policy_id(self),
            "plan_digest": (
                self.workload_plan.plan_digest if self.workload_plan else ""
            ),
            "allocation_id": self.allocation_id,
            "allocation_task_id": self.task_id if self.allocation_id else "",
            "allocation_scope": self.scope if self.allocation_id else "",
            "allocation_class": self.workload_class,
            "allocation_batch_index": self.allocation_batch_index,
            "allocation_units": list(self.allocation_units),
            "allocation_budget_bytes": self.allocation_budget_bytes,
            "allocation_spent_provider_bytes": (
                int(self.allocation_claim.spent_provider_bytes) + self.total_bytes
                if self.allocation_claim
                else 0
            ),
            "allocation_remaining_provider_bytes": (
                max(
                    0,
                    int(self.allocation_claim.remaining_provider_bytes)
                    - self.total_bytes,
                )
                if self.allocation_claim
                else 0
            ),
            "base_run_id": self.base_run_id,
            "workload_phase": self.workload_phase,
            "phase_plan_digest": (
                self.workload_plan.plan_digest if self.workload_plan else ""
            ),
            "parent_run_cap_bytes": self.parent_run_cap_bytes,
            "parent_run_spent_provider_bytes": (self.parent_run_spent_provider_bytes),
            "endpoint_request_provider_bytes": {
                endpoint: list(observations)
                for endpoint, observations in sorted(
                    self.endpoint_request_provider_bytes.items()
                )
            },
        }

    @property
    def dagrun_key(self) -> str:
        if self.dag_id and self.run_id:
            return f"{self.dag_id}/{self.run_id}"
        # Non-Airflow compatibility callers still receive an isolated hard
        # budget rather than accidentally sharing an anonymous global bucket.
        return f"standalone/{self.lease_id}"


LEASES: dict[str, Lease] = {}
LEASE_TOKENS: dict[str, str] = {}
_daily_day = ""
_daily_up_bytes = 0
_daily_down_bytes = 0
_daily_reserved_bytes = 0
_run_up_bytes: dict[str, int] = defaultdict(int)
_run_down_bytes: dict[str, int] = defaultdict(int)
_run_reserved_bytes: dict[str, int] = defaultdict(int)
_url_up_bytes: dict[tuple[str, str], int] = defaultdict(int)
_url_down_bytes: dict[tuple[str, str], int] = defaultdict(int)
_url_reserved_bytes: dict[tuple[str, str], int] = defaultdict(int)

# Idle-refresh rotation state (#652). The residential exit is refreshed only when
# no tunnel is currently open (``_active == 0``), so one exit IP serves a whole
# FlareSolverr/CF session (the page and its Turnstile challenge share an IP =
# CF-safe) and each new session — which closes all tunnels first — draws a fresh
# exit. Picking per-CONNECT instead would split a page and its CF challenge across
# different IPs and re-trigger the challenge.
_current_up: tuple[str, int, str, str] | None = None
_active = 0


def _load_blocklist(path: str | None) -> set[str]:
    out: set[str] = set()
    if not path:
        return out
    with open(path) as fh:
        for line in fh:
            line = line.strip()
            if line and not line.startswith("#"):
                out.add(line.lower())
    return out


def _is_blocked(host: str) -> bool:
    h = host.lower()
    return any(h == b or h.endswith("." + b) for b in BLOCKLIST)


def _lease_host_allowed(lease: Lease | None, host: str) -> bool:
    """Enforce source host scope before any residential upstream is dialled."""
    normalized = host.lower().rstrip(".")
    if normalized == SOFASCORE_CANARY_EXIT_PROBE_HOST:
        return lease is not None and lease.source == "sofascore_canary"
    if lease is not None and lease.source in ("sofascore", "sofascore_canary"):
        return (
            normalized == "sofascore.com"
            or normalized.endswith(".sofascore.com")
            or normalized in SOFASCORE_CHALLENGE_HOSTS
        )
    if lease is not None and lease.source == "fbref":
        return (
            normalized in FBREF_ALLOWED_HOSTS
            or normalized.endswith(".fbref.com")
        )
    return True


def _control_token_valid(headers: dict[str, str]) -> bool:
    supplied = str(headers.get("x-proxy-control-token") or "")
    return bool(CONTROL_TOKEN) and secrets.compare_digest(supplied, CONTROL_TOKEN)


def _json_object_without_duplicate_fields(
    pairs: list[tuple[str, Any]],
) -> dict[str, Any]:
    result: dict[str, Any] = {}
    for key, value in pairs:
        if key in result:
            raise _DuplicateJsonField
        result[key] = value
    return result


def _pool_error(index: int | None, field: str, reason: str) -> ProxyPoolConfigurationError:
    location = "proxy pool" if index is None else f"proxy pool entry {index}"
    return ProxyPoolConfigurationError(f"{location}: invalid {field} ({reason})")


def _contains_control_character(value: str) -> bool:
    return any(ord(character) < 32 or ord(character) == 127 for character in value)


def _normalise_proxy_host(value: Any, *, index: int) -> str:
    if not isinstance(value, str) or not value or value != value.strip():
        raise _pool_error(index, "host", "must be a non-empty trimmed string")
    if _contains_control_character(value) or len(value) > 253:
        raise _pool_error(index, "host", "contains unsupported characters")
    try:
        return ipaddress.ip_address(value).compressed
    except ValueError:
        pass
    try:
        ascii_host = value.encode("idna").decode("ascii").lower()
    except UnicodeError as exc:
        raise _pool_error(index, "host", "is not a valid hostname") from exc
    labels = ascii_host.rstrip(".").split(".")
    if (
        ascii_host.endswith(".")
        or len(ascii_host) > 253
        or not labels
        or any(not _DNS_LABEL.fullmatch(label) for label in labels)
    ):
        raise _pool_error(index, "host", "is not a valid hostname")
    return ascii_host


def _normalise_proxy_credential(
    value: Any,
    *,
    index: int,
    field: str,
    max_length: int,
) -> str:
    if not isinstance(value, str) or not value:
        raise _pool_error(index, field, "must be a non-empty string")
    try:
        value.encode("utf-8")
    except UnicodeEncodeError as exc:
        raise _pool_error(index, field, "contains unsupported characters") from exc
    if len(value) > max_length or _contains_control_character(value):
        raise _pool_error(index, field, "contains unsupported characters")
    if field == "username" and ":" in value:
        raise _pool_error(index, field, "must not contain ':'")
    return value


def _parse_proxy_pool_json(raw: str) -> tuple[dict[str, Any], ...]:
    """Parse a bounded, strict pool while keeping credential values out of errors."""
    if not isinstance(raw, str) or not raw.strip():
        raise _pool_error(None, PROXY_POOL_ENV, "is required and must not be blank")
    try:
        encoded_size = len(raw.encode("utf-8"))
    except UnicodeEncodeError as exc:
        raise _pool_error(None, PROXY_POOL_ENV, "is not valid UTF-8") from exc
    if encoded_size > MAX_PROXY_POOL_JSON_BYTES:
        raise _pool_error(None, PROXY_POOL_ENV, "exceeds the size limit")
    try:
        payload = json.loads(raw, object_pairs_hook=_json_object_without_duplicate_fields)
    except _DuplicateJsonField as exc:
        raise _pool_error(None, PROXY_POOL_ENV, "contains a duplicate object field") from exc
    except (json.JSONDecodeError, UnicodeDecodeError) as exc:
        raise _pool_error(None, PROXY_POOL_ENV, "is not valid JSON") from exc
    if not isinstance(payload, list) or not payload:
        raise _pool_error(None, PROXY_POOL_ENV, "must be a non-empty JSON array")
    if len(payload) > MAX_PROXY_POOL_ENTRIES:
        raise _pool_error(None, PROXY_POOL_ENV, "contains too many entries")

    records: list[dict[str, Any]] = []
    identities: set[tuple[str, int, str]] = set()
    for index, item in enumerate(payload):
        if not isinstance(item, dict):
            raise _pool_error(index, "entry", "must be a JSON object")
        keys = frozenset(item)
        if keys != _PROXY_POOL_FIELDS:
            raise _pool_error(
                index,
                "fields",
                "must contain exactly host, port, username and password",
            )
        host = _normalise_proxy_host(item["host"], index=index)
        port = item["port"]
        if isinstance(port, bool) or not isinstance(port, int) or not 1 <= port <= 65535:
            raise _pool_error(index, "port", "must be an integer in 1..65535")
        username = _normalise_proxy_credential(
            item["username"], index=index, field="username", max_length=1024
        )
        password = _normalise_proxy_credential(
            item["password"], index=index, field="password", max_length=4096
        )
        identity = (host, port, username)
        if identity in identities:
            raise _pool_error(index, "entry", "duplicates an earlier endpoint identity")
        identities.add(identity)
        records.append(
            {
                "host": host,
                "port": port,
                "username": username,
                "password": password,
            }
        )
    return tuple(records)


def _residential_manager(
    *,
    proxy_pool_json: str | None,
    proxy_file: str,
    allow_file_fallback: bool,
):
    """Load the paid pool from the environment, or an explicitly enabled file.

    The upstream is refreshed per FlareSolverr session (idle-refresh, see
    ``_acquire_upstream``) so each session lands on a different residential exit —
    keeping IP diversity that a pin-one-proxy startup would have thrown away, while
    holding one exit per CF session so the page and its Turnstile challenge stay on
    a single IP (#652). Credential values are never included in errors or logs.
    """
    from scrapers.utils.proxy_manager import ProxyManager, ProxyType

    mgr = ProxyManager(rotation_strategy="random")
    if proxy_pool_json is not None and proxy_pool_json.strip():
        records = _parse_proxy_pool_json(proxy_pool_json)
        for record in records:
            mgr.add_proxy(
                host=record["host"],
                port=record["port"],
                proxy_type=ProxyType.HTTP,
                username=record["username"],
                password=record["password"],
            )
        n = len(records)
        source = PROXY_POOL_ENV
    elif allow_file_fallback:
        n = mgr.load_from_file_custom_format(proxy_file)
        source = "explicit file fallback"
    else:
        raise ProxyPoolConfigurationError(
            f"{PROXY_POOL_ENV} is required; file fallback is disabled"
        )
    if n <= 0:
        raise ProxyPoolConfigurationError("proxy pool contains no usable entries")
    return mgr, source


def _pick_upstream(mgr):
    """(host, port, user, pass) of one residential proxy from the pool."""
    selected = mgr.get_proxy()
    if all(
        hasattr(selected, field)
        for field in ("host", "port", "username", "password")
    ):
        return selected.host, selected.port, selected.username, selected.password
    u = urlsplit(selected.url)  # Compatibility with the minimal unit-test fake.
    return u.hostname, u.port, u.username, u.password


def _utc_day() -> str:
    return datetime.now(timezone.utc).date().isoformat()


def _refresh_daily_counter() -> None:
    global _daily_day, _daily_up_bytes, _daily_down_bytes, _daily_reserved_bytes
    day = _utc_day()
    if day != _daily_day:
        _daily_day = day
        _daily_up_bytes = 0
        _daily_down_bytes = 0
        _daily_reserved_bytes = 0


def _daily_total_bytes() -> int:
    _refresh_daily_counter()
    return _daily_up_bytes + _daily_down_bytes


def _run_total_bytes(run_key: str) -> int:
    return _run_up_bytes[run_key] + _run_down_bytes[run_key]


def _url_total_bytes(run_key: str, canonical_url: str) -> int:
    key = (run_key, canonical_url)
    return _url_up_bytes[key] + _url_down_bytes[key]


def _lease_dagrun_budget_bytes(lease: Lease) -> int:
    """Use the signed sum of allocations for production SofaScore runs."""

    if lease.source == "sofascore":
        if lease.workload_plan is None or lease.run_cap_bytes <= 0:
            return 0
        return lease.run_cap_bytes
    return _dagrun_budget_bytes(lease.dag_id)


def _lease_url_budget_bytes(lease: Lease) -> int:
    # A warmed SofaScore browser intentionally captures many API endpoints in
    # one lease.  The measured DagRun cap is its URL/session cap too, so the
    # legacy per-page WhoScored ceiling cannot truncate the warmed session.
    # Registry discovery has the same shape: thousands of JSON reads against
    # one canonical API origin, so the 2 MB per-URL ceiling would strangle it.
    if lease.source in (
        "fbref",
        "sofascore",
        "sofascore_canary",
        "sofascore_discovery",
    ):
        return _lease_dagrun_budget_bytes(lease)
    return URL_BUDGET_BYTES


def _canonical_url(value: Any) -> str:
    raw = str(value or "").strip()
    parts = urlsplit(raw)
    if parts.scheme and parts.netloc:
        query = urlencode(
            sorted(
                (
                    key,
                    "[REDACTED]" if key.lower() in _SENSITIVE_QUERY_KEYS else item,
                )
                for key, item in parse_qsl(parts.query, keep_blank_values=True)
            )
        )
        return urlunsplit(
            (
                parts.scheme.lower(),
                parts.netloc.lower(),
                parts.path or "/",
                query,
                "",
            )
        )
    return raw.split("#", 1)[0]


def _append_budget_event(event_type: str, lease: Lease, **values: Any) -> None:
    """Append and fsync exact paid accounting before another lease is served."""
    if not LEDGER_PATH:
        raise RuntimeError("paid request ledger path is not configured")
    event = {
        "event_version": "paid-proxy-v2",
        "event_id": uuid_hex(24),
        "occurred_at": datetime.now(timezone.utc).isoformat(),
        "event_type": event_type,
        "lease_id": lease.lease_id,
        "route": "paid_proxy",
        "dag_id": lease.dag_id,
        "run_id": lease.run_id,
        "task_id": lease.task_id,
        "map_index": lease.map_index,
        "try_number": lease.try_number,
        "scope": lease.scope,
        "capture_scope": lease.capture_scope,
        "entity": lease.entity,
        "canonical_url": lease.canonical_url,
        "source": lease.source,
        "budget_policy_id": _lease_budget_policy_id(lease),
        "dagrun_budget_bytes": _lease_dagrun_budget_bytes(lease),
        "plan_digest": (lease.workload_plan.plan_digest if lease.workload_plan else ""),
        "allocation_id": lease.allocation_id,
        "allocation_class": lease.workload_class,
        "allocation_batch_index": lease.allocation_batch_index,
        "allocation_budget_bytes": lease.allocation_budget_bytes,
        "base_run_id": lease.base_run_id,
        "workload_phase": lease.workload_phase,
        "parent_run_cap_bytes": lease.parent_run_cap_bytes,
        "parent_run_spent_provider_bytes": (lease.parent_run_spent_provider_bytes),
        **values,
    }
    os.makedirs(os.path.dirname(LEDGER_PATH) or ".", exist_ok=True)
    payload = (json.dumps(event, sort_keys=True, separators=(",", ":")) + "\n").encode(
        "utf-8"
    )
    if len(payload) > MAX_LEDGER_EVENT_BYTES:
        raise RuntimeError(f"paid byte event exceeds {MAX_LEDGER_EVENT_BYTES} bytes")
    descriptor = os.open(
        LEDGER_PATH,
        os.O_APPEND | os.O_CREAT | os.O_WRONLY,
        0o600,
    )
    try:
        os.fchmod(descriptor, 0o600)
        pending = memoryview(payload)
        while pending:
            written = os.write(descriptor, pending)
            if written <= 0:
                raise OSError("paid byte ledger write made no progress")
            pending = pending[written:]
        os.fsync(descriptor)
    finally:
        os.close(descriptor)


def _restore_budget_ledger(path: str, *, restore_daily: bool = True) -> int:
    """Restore run/URL/daily counters from durable byte-delta events."""
    global _daily_day, _daily_up_bytes, _daily_down_bytes
    restored = 0
    try:
        stream = open(path, "rb")
    except (FileNotFoundError, OSError):
        return 0
    today = _utc_day()
    with stream:
        line_number = 0
        while True:
            raw = stream.readline(MAX_LEDGER_EVENT_BYTES + 1)
            if not raw:
                break
            line_number += 1
            try:
                if len(raw) > MAX_LEDGER_EVENT_BYTES:
                    raise ValueError("paid byte event exceeds bounded line limit")
                event = json.loads(raw.decode("utf-8"))
                if not isinstance(event, dict):
                    raise ValueError("paid byte event must be a JSON object")
                if event.get("event_type") != "bytes":
                    continue
                count = max(0, int(event.get("bytes", 0)))
                direction = event.get("direction")
                if direction not in ("up", "down") or count <= 0:
                    raise ValueError("invalid paid byte event")
                dag_id = str(event.get("dag_id") or "")
                run_id = str(event.get("run_id") or "")
                lease_id = str(event.get("lease_id") or "")
                run_key = (
                    f"{dag_id}/{run_id}"
                    if dag_id and run_id
                    else f"standalone/{lease_id}"
                )
                canonical = _canonical_url(event.get("canonical_url"))
                if direction == "up":
                    _run_up_bytes[run_key] += count
                    _url_up_bytes[(run_key, canonical)] += count
                else:
                    _run_down_bytes[run_key] += count
                    _url_down_bytes[(run_key, canonical)] += count
                occurred = str(event.get("occurred_at") or "")
                if restore_daily and occurred[:10] == today:
                    _daily_day = today
                    if direction == "up":
                        _daily_up_bytes += count
                    else:
                        _daily_down_bytes += count
                restored += 1
            except (
                json.JSONDecodeError,
                UnicodeDecodeError,
                TypeError,
                ValueError,
            ) as exc:
                raise RuntimeError(
                    f"corrupt paid byte ledger at line {line_number}"
                ) from exc
    return restored


def _restore_daily_counter(out_path: str) -> None:
    """Restore today's hard budget after a proxy-filter restart.

    Active leases/tunnels cannot survive a process restart, but billed bytes
    already consumed today must.  The periodic report is atomically replaced,
    so accepting it as the checkpoint cannot read a half-written value.
    """
    global _daily_day, _daily_up_bytes, _daily_down_bytes, _daily_reserved_bytes
    try:
        with open(out_path, encoding="utf-8") as fh:
            daily = (json.load(fh) or {}).get("daily") or {}
    except (FileNotFoundError, OSError, json.JSONDecodeError, TypeError):
        return
    if daily.get("day") != _utc_day():
        return
    _daily_day = str(daily["day"])
    _daily_up_bytes = max(0, int(daily.get("up_bytes", 0)))
    _daily_down_bytes = max(0, int(daily.get("down_bytes", 0)))
    if not (_daily_up_bytes or _daily_down_bytes):
        # Compatibility with reports written before directional counters were
        # added. Treat all historical bytes as downstream; only the sum gates.
        _daily_down_bytes = max(0, int(daily.get("total_bytes", 0)))
    _daily_reserved_bytes = 0


class ParentEnvelopeError(AllocationError):
    """A signed phase conflicts with the immutable parent DagRun envelope."""


class ParentEnvelopeBudgetExceeded(AllocationBudgetExceeded):
    """The next provider byte would cross the parent DagRun envelope."""


@dataclass(frozen=True)
class ParentRunEnvelope:
    dag_id: str
    base_run_id: str
    phase: str
    phase_plan_digest: str
    phase_cap_bytes: int
    parent_cap_bytes: int
    parent_spent_provider_bytes: int


def _split_phase_run_id(run_id: str) -> tuple[str, str]:
    value = str(run_id or "").strip()
    if value.count("::") != 1:
        raise ParentEnvelopeError(
            "production SofaScore run_id must end in ::season, ::targets or ::players"
        )
    base_run_id, phase = value.rsplit("::", 1)
    if not base_run_id or phase not in {"season", "targets", "players"}:
        raise ParentEnvelopeError(
            "production SofaScore run_id must end in ::season, ::targets or ::players"
        )
    return base_run_id, phase


class ParentRunEnvelopeLedger:
    """Atomic parent cap shared by immutable season/targets/players plans."""

    SCHEMA_VERSION = 1
    PHASE_ORDER = {"season": 0, "targets": 1, "players": 2}

    def __init__(self, path: str) -> None:
        self.path = path
        self.lock_path = path + ".lock"

    def _locked(self):
        os.makedirs(os.path.dirname(self.lock_path) or ".", exist_ok=True)
        handle = open(self.lock_path, "a+")
        os.fchmod(handle.fileno(), 0o600)
        fcntl.flock(handle.fileno(), fcntl.LOCK_EX)
        return handle

    def _read(self) -> dict[str, Any]:
        try:
            with open(self.path, encoding="utf-8") as stream:
                payload = json.load(stream)
        except FileNotFoundError:
            return {"schema_version": self.SCHEMA_VERSION, "runs": {}}
        except (OSError, json.JSONDecodeError) as exc:
            raise ParentEnvelopeError("parent envelope ledger is corrupt") from exc
        if (
            not isinstance(payload, dict)
            or payload.get("schema_version") != self.SCHEMA_VERSION
            or not isinstance(payload.get("runs"), dict)
        ):
            raise ParentEnvelopeError("unsupported parent envelope ledger")
        return payload

    def _write(self, payload: Mapping[str, Any]) -> None:
        directory_name = os.path.dirname(self.path) or "."
        os.makedirs(directory_name, exist_ok=True)
        temporary = f"{self.path}.tmp-{os.getpid()}-{uuid_hex(16)}"
        descriptor = os.open(
            temporary,
            os.O_CREAT | os.O_EXCL | os.O_WRONLY,
            0o600,
        )
        try:
            with os.fdopen(descriptor, "w", encoding="utf-8") as stream:
                json.dump(payload, stream, indent=2, sort_keys=True)
                stream.write("\n")
                stream.flush()
                os.fsync(stream.fileno())
            os.replace(temporary, self.path)
            directory = os.open(directory_name, os.O_RDONLY | os.O_DIRECTORY)
            try:
                os.fsync(directory)
            finally:
                os.close(directory)
        finally:
            try:
                os.unlink(temporary)
            except FileNotFoundError:
                pass

    @staticmethod
    def _key(dag_id: str, base_run_id: str) -> str:
        return hashlib.sha256(f"{dag_id}\0{base_run_id}".encode("utf-8")).hexdigest()

    def _snapshot(
        self,
        run: Mapping[str, Any],
        *,
        phase: str,
    ) -> ParentRunEnvelope:
        phase_state = run["phases"][phase]
        return ParentRunEnvelope(
            dag_id=str(run["dag_id"]),
            base_run_id=str(run["base_run_id"]),
            phase=phase,
            phase_plan_digest=str(phase_state["plan_digest"]),
            phase_cap_bytes=int(phase_state["run_cap_bytes"]),
            parent_cap_bytes=sum(
                int(item["run_cap_bytes"]) for item in run["phases"].values()
            ),
            parent_spent_provider_bytes=int(run["spent_provider_bytes"]),
        )

    def register(self, plan: SignedDagRunPlan) -> ParentRunEnvelope:
        base_run_id, phase = _split_phase_run_id(plan.run_id)
        allocation_scopes = {allocation.scope for allocation in plan.allocations}
        if phase == "season" and allocation_scopes not in (set(), {"season"}):
            raise ParentEnvelopeError(
                "season phase plan may contain only season allocations"
            )
        if phase == "targets" and not allocation_scopes.issubset({"match"}):
            raise ParentEnvelopeError(
                "targets phase plan may contain only match allocations"
            )
        if phase == "players" and not allocation_scopes.issubset({"player"}):
            raise ParentEnvelopeError(
                "players phase plan may contain only player allocations"
            )
        key = self._key(plan.dag_id, base_run_id)
        handle = self._locked()
        try:
            payload = self._read()
            run = payload["runs"].get(key)
            if run is None:
                run = {
                    "dag_id": plan.dag_id,
                    "base_run_id": base_run_id,
                    "artifact_id": plan.artifact_id,
                    "phases": {},
                    "spent_provider_bytes": 0,
                    "targets_registered_first": phase == "targets",
                    "created_at": datetime.now(timezone.utc).isoformat(),
                }
                payload["runs"][key] = run
            if (
                run.get("dag_id") != plan.dag_id
                or run.get("base_run_id") != base_run_id
                or run.get("artifact_id") != plan.artifact_id
            ):
                raise ParentEnvelopeError(
                    "parent DagRun envelope provenance is immutable"
                )
            phases = run.get("phases")
            if not isinstance(phases, dict):
                raise ParentEnvelopeError("parent DagRun phases are corrupt")
            if (
                phase == "season"
                and run.get("targets_registered_first") is True
                and "season" not in phases
            ):
                raise ParentEnvelopeError(
                    "season phase cannot expand a target-first no-traffic envelope"
                )
            expected = {
                "plan_digest": plan.plan_digest,
                "run_cap_bytes": plan.run_cap_bytes,
            }
            existing = phases.get(phase)
            if existing is None:
                later_phases = sorted(
                    existing_phase
                    for existing_phase in phases
                    if self.PHASE_ORDER[existing_phase] > self.PHASE_ORDER[phase]
                )
                if later_phases:
                    raise ParentEnvelopeError(
                        f"{phase} phase cannot expand an envelope after "
                        f"{later_phases[-1]} was registered"
                    )
                phases[phase] = {**expected, "spent_provider_bytes": 0}
            elif not isinstance(existing, Mapping) or any(
                existing.get(field) != value for field, value in expected.items()
            ):
                raise ParentEnvelopeError(
                    f"parent DagRun already has another immutable {phase} plan"
                )
            parent_cap = sum(int(item["run_cap_bytes"]) for item in phases.values())
            if int(run.get("spent_provider_bytes", 0)) > parent_cap:
                raise ParentEnvelopeError("parent DagRun spend exceeds its signed cap")
            run["updated_at"] = datetime.now(timezone.utc).isoformat()
            self._write(payload)
            return self._snapshot(run, phase=phase)
        finally:
            fcntl.flock(handle.fileno(), fcntl.LOCK_UN)
            handle.close()

    def consume(
        self,
        *,
        dag_id: str,
        base_run_id: str,
        phase: str,
        phase_plan_digest: str,
        provider_bytes: int,
    ) -> ParentRunEnvelope:
        if isinstance(provider_bytes, bool) or provider_bytes < 0:
            raise ParentEnvelopeError("parent provider bytes must be non-negative")
        handle = self._locked()
        try:
            payload = self._read()
            run = payload["runs"].get(self._key(dag_id, base_run_id))
            if not isinstance(run, dict):
                raise ParentEnvelopeError("parent DagRun envelope is unknown")
            phases = run.get("phases")
            phase_state = phases.get(phase) if isinstance(phases, dict) else None
            if (
                not isinstance(phase_state, dict)
                or phase_state.get("plan_digest") != phase_plan_digest
            ):
                raise ParentEnvelopeError("phase plan is absent from parent envelope")
            parent_cap = sum(int(item["run_cap_bytes"]) for item in phases.values())
            spent = int(run.get("spent_provider_bytes", 0))
            if spent + provider_bytes > parent_cap:
                raise ParentEnvelopeBudgetExceeded(
                    "provider chunk would exceed parent DagRun cap"
                )
            run["spent_provider_bytes"] = spent + provider_bytes
            phase_state["spent_provider_bytes"] = (
                int(phase_state.get("spent_provider_bytes", 0)) + provider_bytes
            )
            run["updated_at"] = datetime.now(timezone.utc).isoformat()
            self._write(payload)
            return self._snapshot(run, phase=phase)
        finally:
            fcntl.flock(handle.fileno(), fcntl.LOCK_UN)
            handle.close()


def _parent_envelope_ledger() -> ParentRunEnvelopeLedger:
    global SOFASCORE_PARENT_ENVELOPE_LEDGER
    global _SOFASCORE_PARENT_ENVELOPE_LEDGER_PATH
    if (
        SOFASCORE_PARENT_ENVELOPE_LEDGER is None
        or _SOFASCORE_PARENT_ENVELOPE_LEDGER_PATH != SOFASCORE_PARENT_ENVELOPE_PATH
    ):
        SOFASCORE_PARENT_ENVELOPE_LEDGER = ParentRunEnvelopeLedger(
            SOFASCORE_PARENT_ENVELOPE_PATH
        )
        _SOFASCORE_PARENT_ENVELOPE_LEDGER_PATH = SOFASCORE_PARENT_ENVELOPE_PATH
    return SOFASCORE_PARENT_ENVELOPE_LEDGER


def _allocation_ledger() -> AllocationLedger:
    """Return the production allocation ledger bound to this control secret."""

    global SOFASCORE_ALLOCATION_LEDGER, _SOFASCORE_ALLOCATION_LEDGER_KEY
    if len(CONTROL_TOKEN) < 32:
        raise RuntimeError("SofaScore allocation ledger has no control token")
    key = (
        SOFASCORE_ALLOCATION_LEDGER_PATH,
        hashlib.sha256(CONTROL_TOKEN.encode("utf-8")).hexdigest(),
    )
    if SOFASCORE_ALLOCATION_LEDGER is None or key != _SOFASCORE_ALLOCATION_LEDGER_KEY:
        SOFASCORE_ALLOCATION_LEDGER = AllocationLedger(
            SOFASCORE_ALLOCATION_LEDGER_PATH,
            control_token=CONTROL_TOKEN,
        )
        _SOFASCORE_ALLOCATION_LEDGER_KEY = key
    return SOFASCORE_ALLOCATION_LEDGER


def _append_allocation_wal(
    event_type: str,
    lease_id: str,
    **values: Any,
) -> None:
    """Fsync the private recovery WAL before state can reach the provider.

    Unlike the operator-facing paid-byte log, this mode-0600 file may contain
    the allocation recovery token.  It is never rendered in reports or logs.
    """

    event = {
        "event_version": "sofascore-allocation-wal-v1",
        "event_id": uuid_hex(24),
        "occurred_at": datetime.now(timezone.utc).isoformat(),
        "event_type": event_type,
        "lease_id": lease_id,
        **values,
    }
    payload = (json.dumps(event, sort_keys=True, separators=(",", ":")) + "\n").encode(
        "utf-8"
    )
    if len(payload) > MAX_ALLOCATION_WAL_EVENT_BYTES:
        raise RuntimeError("SofaScore allocation WAL event is too large")
    os.makedirs(os.path.dirname(SOFASCORE_ALLOCATION_WAL_PATH) or ".", exist_ok=True)
    descriptor = os.open(
        SOFASCORE_ALLOCATION_WAL_PATH,
        os.O_APPEND | os.O_CREAT | os.O_WRONLY,
        0o600,
    )
    try:
        os.fchmod(descriptor, 0o600)
        pending = memoryview(payload)
        while pending:
            written = os.write(descriptor, pending)
            if written <= 0:
                raise OSError("allocation WAL write made no progress")
            pending = pending[written:]
        os.fsync(descriptor)
    finally:
        os.close(descriptor)


def _read_allocation_wal() -> dict[str, dict[str, Any]]:
    states: dict[str, dict[str, Any]] = {}
    try:
        stream = open(SOFASCORE_ALLOCATION_WAL_PATH, "rb")
    except FileNotFoundError:
        return states
    except OSError as exc:
        raise RuntimeError("cannot read SofaScore allocation WAL") from exc
    with stream:
        for line_number in range(1, 10_000_001):
            raw = stream.readline(MAX_ALLOCATION_WAL_EVENT_BYTES + 1)
            if not raw:
                break
            try:
                if len(raw) > MAX_ALLOCATION_WAL_EVENT_BYTES:
                    raise ValueError("allocation WAL line exceeds its limit")
                event = json.loads(raw.decode("utf-8"))
                if (
                    not isinstance(event, dict)
                    or event.get("event_version") != "sofascore-allocation-wal-v1"
                ):
                    raise ValueError("unsupported allocation WAL event")
                lease_id = str(event.get("lease_id") or "").strip()
                if not lease_id:
                    raise ValueError("allocation WAL event has no lease_id")
                state = states.setdefault(
                    lease_id,
                    {
                        "finished": False,
                        "observations": {},
                        "active_request_id": "",
                        "active_endpoint": "",
                    },
                )
                kind = event.get("event_type")
                if kind == "claim_intent":
                    if state.get("plan") is not None:
                        raise ValueError("duplicate claim intent")
                    state["plan"] = event.get("workload_plan")
                    state["allocation_id"] = event.get("allocation_id")
                    state["claim_token"] = event.get("claim_token")
                elif kind == "endpoint_started":
                    if state.get("active_request_id"):
                        raise ValueError("overlapping endpoint requests")
                    state["active_request_id"] = str(event.get("request_id") or "")
                    state["active_endpoint"] = str(event.get("endpoint") or "")
                    if not state["active_request_id"] or not state["active_endpoint"]:
                        raise ValueError("invalid endpoint start")
                elif kind == "endpoint_finished":
                    if str(event.get("request_id") or "") != state.get(
                        "active_request_id"
                    ):
                        raise ValueError("endpoint finish does not match start")
                    endpoint = str(event.get("endpoint") or "")
                    if endpoint != state.get("active_endpoint"):
                        raise ValueError("endpoint finish changed endpoint")
                    amount = event.get("provider_bytes")
                    if (
                        isinstance(amount, bool)
                        or not isinstance(amount, int)
                        or amount < 0
                    ):
                        raise ValueError("invalid endpoint provider bytes")
                    state["observations"].setdefault(endpoint, []).append(amount)
                    state["active_request_id"] = ""
                    state["active_endpoint"] = ""
                elif kind == "allocation_finished":
                    state["finished"] = True
                    state["active_request_id"] = ""
                    state["active_endpoint"] = ""
                else:
                    raise ValueError("unknown allocation WAL event")
            except (
                json.JSONDecodeError,
                UnicodeDecodeError,
                TypeError,
                ValueError,
            ) as exc:
                raise RuntimeError(
                    f"corrupt SofaScore allocation WAL at line {line_number}"
                ) from exc
    return states


def _recover_allocation_wal() -> int:
    """Release crash-orphaned attempts without returning their spent bytes."""

    recovered = 0
    ledger = _allocation_ledger()
    for lease_id, state in _read_allocation_wal().items():
        if state.get("finished") or state.get("plan") is None:
            continue
        try:
            plan = SignedDagRunPlan.from_dict(
                state["plan"], control_token=CONTROL_TOKEN
            )
            allocation_id = str(state.get("allocation_id") or "")
            claim_token = str(state.get("claim_token") or "")
            claim = ledger.resume_claim(
                plan,
                allocation_id,
                claim_token=claim_token,
            )
        except AllocationAccountingError:
            # A crash before ``claim`` leaves an intent but no active owner; a
            # crash after ``finish`` leaves the allocation already safe.  Only
            # an actually active, different token is a corruption condition.
            snapshot = ledger.snapshot(plan)
            allocation = snapshot["allocations"][allocation_id]
            active = allocation.get("active_claim")
            if active is not None:
                raise RuntimeError(
                    "SofaScore allocation WAL cannot recover an active claim"
                ) from None
            _append_allocation_wal(
                "allocation_finished", lease_id, recovered_without_active_claim=True
            )
            recovered += 1
            continue
        observations = {
            str(endpoint): [int(value) for value in values]
            for endpoint, values in state.get("observations", {}).items()
        }
        snapshot = ledger.snapshot(plan)
        persisted = snapshot["allocations"][allocation_id]
        active = persisted.get("active_claim") or {}
        attempt_spent = int(persisted["spent_provider_bytes"]) - int(
            active.get("start_spent_provider_bytes", 0)
        )
        reported = sum(sum(values) for values in observations.values())
        remainder = attempt_spent - reported
        if remainder < 0:
            raise RuntimeError("allocation WAL reports more bytes than its ledger")
        active_endpoint = str(state.get("active_endpoint") or "")
        if remainder or active_endpoint:
            if not active_endpoint:
                raise RuntimeError(
                    "allocation WAL lost endpoint provenance for provider bytes"
                )
            observations.setdefault(active_endpoint, []).append(remainder)
        ledger.finish(
            plan,
            claim,
            lease_id=lease_id,
            endpoint_request_provider_bytes=observations,
            completed=False,
        )
        _append_allocation_wal(
            "allocation_finished", lease_id, recovered_after_restart=True
        )
        recovered += 1
    return recovered


def _signed_allocation_from_request(
    metadata: Mapping[str, Any],
    *,
    max_bytes: int,
) -> tuple[SignedDagRunPlan, WorkloadAllocation]:
    """HMAC-validate and exactly bind every mirrored allocation field."""

    raw_plan = metadata.get("workload_plan")
    plan = SignedDagRunPlan.from_dict(raw_plan, control_token=CONTROL_TOKEN)
    if plan.artifact_id != SOFASCORE_BUDGET_ARTIFACT_ID:
        raise WorkloadPlanError(
            "signed workload plan does not use the verified budget artifact"
        )
    if (
        plan.dag_id != str(metadata.get("dag_id") or "").strip()
        or plan.run_id != str(metadata.get("run_id") or "").strip()
    ):
        raise WorkloadPlanError("signed workload plan DAG/run provenance mismatch")
    allocation_id = str(metadata.get("allocation_id") or "").strip()
    try:
        allocation = next(
            item for item in plan.allocations if item.allocation_id == allocation_id
        )
    except StopIteration as exc:
        raise WorkloadPlanError("allocation is absent from the signed plan") from exc
    if metadata.get("allocation") != allocation.to_dict():
        raise WorkloadPlanError("lease allocation fields differ from the signed plan")
    if (
        str(metadata.get("task_id") or "").strip() != allocation.task_id
        or str(metadata.get("scope") or "").strip() != allocation.scope
        or max_bytes > allocation.budget_bytes
    ):
        raise WorkloadPlanError("lease task/scope/budget differ from signed allocation")
    attempt_id = str(metadata.get("attempt_id") or "").strip()
    if not attempt_id:
        raise WorkloadPlanError("production allocation requires attempt_id")
    return plan, allocation


def _create_lease(
    mgr,
    *,
    max_bytes: int,
    ttl_seconds: int,
    metadata: dict[str, Any] | None = None,
    require_context: bool = False,
) -> Lease:
    """Create one explicit, byte-bounded sticky residential lease."""
    _reap_expired_leases()
    _refresh_daily_counter()
    if max_bytes <= 0 or max_bytes > MAX_LEASE_BYTES:
        raise ValueError(f"max_bytes must be in 1..{MAX_LEASE_BYTES}")
    if ttl_seconds <= 0 or ttl_seconds > MAX_LEASE_TTL_SECONDS:
        raise ValueError(f"ttl_seconds must be in 1..{MAX_LEASE_TTL_SECONDS}")
    if _daily_total_bytes() >= DAILY_BUDGET_BYTES:
        raise RuntimeError("daily paid-proxy budget exhausted")
    metadata = metadata or {}
    if require_context and not all(
        str(metadata.get(field) or "").strip()
        for field in ("dag_id", "run_id", "task_id", "canonical_url")
    ):
        raise ValueError(
            "dag_id, run_id, task_id and canonical_url are required for paid leases"
        )
    dag_id = str(metadata.get("dag_id") or "").strip()
    requested_source = str(metadata.get("source") or "").strip().lower()
    inferred_source = _source_for_dag(dag_id)
    if requested_source and requested_source != inferred_source:
        raise ValueError("paid lease source does not match dag_id")
    source = inferred_source or requested_source
    if source == "sofascore" and SOFASCORE_DAGRUN_BUDGET_BYTES <= 0:
        raise RuntimeError(
            "SofaScore paid-proxy budget unavailable: verified canary required"
        )
    if source == "sofascore_canary" and SOFASCORE_CANARY_HARD_CAP_BYTES <= 0:
        raise RuntimeError(
            "SofaScore canary lease unavailable: explicit experimental cap required"
        )
    if source == "sofascore_discovery" and SOFASCORE_DISCOVERY_DAGRUN_BUDGET_BYTES <= 0:
        raise RuntimeError(
            "SofaScore discovery lease unavailable: explicit DagRun cap required"
        )
    active_leases = [
        item
        for item in LEASES.values()
        if (
            (not item.closed and not item.expired)
            or item.active_tunnels > 0
            or item.reserved_bytes > 0
        )
    ]
    if len(active_leases) >= MAX_ACTIVE_LEASES:
        raise RuntimeError("paid-proxy concurrency limit reached")
    if source == "sofascore" and any(
        item.source == "sofascore" for item in active_leases
    ):
        raise RuntimeError("SofaScore paid-proxy concurrency limit reached")
    # The catalog scan is a single serial walker: it rotates exhausted leases
    # one after another.  Two concurrent discovery leases would mean a second
    # unaccounted walker, so the source is serial exactly like production.
    if source == "sofascore_discovery" and any(
        item.source == "sofascore_discovery" for item in active_leases
    ):
        raise RuntimeError("SofaScore discovery paid-proxy concurrency limit reached")
    if source == "fbref" and any(item.source == "fbref" for item in active_leases):
        raise RuntimeError("FBref paid-proxy concurrency limit reached")
    # Canary deltas must not overlap any other paid traffic on this provider
    # process.  Conversely, no normal lease starts while a canary is active.
    if (source == "sofascore_canary" and active_leases) or any(
        item.source == "sofascore_canary" for item in active_leases
    ):
        raise RuntimeError("SofaScore canary requires an isolated serial lease")
    now = _wall_time()
    lease_id = uuid_hex(12)
    run_id = str(metadata.get("run_id") or "").strip()
    run_key = f"{dag_id}/{run_id}" if dag_id and run_id else f"standalone/{lease_id}"
    canonical_url = _canonical_url(metadata.get("canonical_url"))
    workload_plan: SignedDagRunPlan | None = None
    allocation: WorkloadAllocation | None = None
    allocation_claim: AllocationClaim | None = None
    parent_envelope: ParentRunEnvelope | None = None
    if source == "sofascore":
        workload_plan, allocation = _signed_allocation_from_request(
            metadata,
            max_bytes=max_bytes,
        )
        parent_envelope = _parent_envelope_ledger().register(workload_plan)
        dagrun_budget = workload_plan.run_cap_bytes
    else:
        dagrun_budget = _dagrun_budget_bytes(dag_id)
    url_budget = (
        dagrun_budget
        if source
        in ("fbref", "sofascore", "sofascore_canary", "sofascore_discovery")
        else URL_BUDGET_BYTES
    )
    available = min(
        DAILY_BUDGET_BYTES - _daily_total_bytes(),
        dagrun_budget - _run_total_bytes(run_key),
        url_budget - _url_total_bytes(run_key, canonical_url),
        (
            parent_envelope.parent_cap_bytes
            - parent_envelope.parent_spent_provider_bytes
            if parent_envelope is not None
            else DAILY_BUDGET_BYTES
        ),
    )
    if available <= 0:
        raise RuntimeError("paid-proxy DagRun or URL budget exhausted")
    if workload_plan is not None and allocation is not None:
        claim_token = secrets.token_urlsafe(32)
        _append_allocation_wal(
            "claim_intent",
            lease_id,
            workload_plan=workload_plan.to_dict(),
            allocation_id=allocation.allocation_id,
            claim_token=claim_token,
        )
        try:
            allocation_claim = _allocation_ledger().claim(
                workload_plan,
                allocation.allocation_id,
                attempt_id=str(metadata["attempt_id"]),
                claim_token=claim_token,
            )
        except BaseException:
            _append_allocation_wal("allocation_finished", lease_id, claim_rejected=True)
            raise
        available = min(available, allocation_claim.remaining_provider_bytes)
        if available <= 0:
            raise AllocationBudgetExceeded(
                "signed allocation has no remaining provider bytes"
            )
    try:
        lease = Lease(
            lease_id=lease_id,
            token=secrets.token_urlsafe(24),
            upstream=_pick_upstream(mgr),
            created_at=now,
            expires_at=now + ttl_seconds,
            max_bytes=min(max_bytes, available),
            dag_id=dag_id,
            run_id=run_id,
            task_id=str(metadata.get("task_id") or ""),
            map_index=int(metadata.get("map_index", -1)),
            try_number=int(metadata.get("try_number", 0)),
            scope=str(metadata.get("scope") or ""),
            capture_scope=str(metadata.get("capture_scope") or ""),
            entity=str(metadata.get("entity") or ""),
            canonical_url=canonical_url,
            source=source,
            workload_plan=workload_plan,
            allocation_claim=allocation_claim,
            allocation_id=allocation.allocation_id if allocation else "",
            workload_class=allocation.workload_class if allocation else "",
            allocation_batch_index=allocation.batch_index if allocation else -1,
            allocation_units=allocation.units if allocation else (),
            allocation_budget_bytes=allocation.budget_bytes if allocation else 0,
            run_cap_bytes=workload_plan.run_cap_bytes if workload_plan else 0,
            base_run_id=(parent_envelope.base_run_id if parent_envelope else ""),
            workload_phase=(parent_envelope.phase if parent_envelope else ""),
            parent_run_cap_bytes=(
                parent_envelope.parent_cap_bytes if parent_envelope else 0
            ),
            parent_run_spent_provider_bytes=(
                parent_envelope.parent_spent_provider_bytes if parent_envelope else 0
            ),
        )
    except BaseException:
        if workload_plan is not None and allocation_claim is not None:
            _allocation_ledger().finish(
                workload_plan,
                allocation_claim,
                lease_id=lease_id,
                endpoint_request_provider_bytes={},
                completed=False,
            )
            _append_allocation_wal(
                "allocation_finished", lease_id, creation_failed=True
            )
        raise
    LEASES[lease.lease_id] = lease
    LEASE_TOKENS[lease.token] = lease.lease_id
    try:
        _append_budget_event("lease_created", lease, max_bytes=lease.max_bytes)
    except Exception:
        LEASES.pop(lease.lease_id, None)
        LEASE_TOKENS.pop(lease.token, None)
        if workload_plan is not None and allocation_claim is not None:
            _allocation_ledger().finish(
                workload_plan,
                allocation_claim,
                lease_id=lease_id,
                endpoint_request_provider_bytes={},
                completed=False,
            )
            _append_allocation_wal(
                "allocation_finished", lease_id, creation_failed=True
            )
        raise
    log.info(
        "lease %s created: source=%s upstream=%s max_bytes=%d ttl=%ds",
        lease.lease_id,
        lease.source or "legacy",
        _upstream_fingerprint(lease.upstream),
        lease.max_bytes,
        ttl_seconds,
    )
    return lease


def uuid_hex(length: int) -> str:
    """Random identifier helper kept separate for deterministic unit tests."""
    return secrets.token_hex((length + 1) // 2)[:length]


def _lease_from_proxy_authorization(value: str | None) -> Lease | None:
    if not value or not value.lower().startswith("basic "):
        return None
    try:
        decoded = base64.b64decode(
            value.split(None, 1)[1],
            validate=True,
        ).decode("utf-8")
        username, token = decoded.split(":", 1)
    except (binascii.Error, ValueError, UnicodeDecodeError):
        return None
    if username != "lease":
        return None
    lease_id = LEASE_TOKENS.get(token)
    return LEASES.get(lease_id or "")


def _authorized_control_lease(lease_id: str, authorization: str | None) -> Lease | None:
    lease = LEASES.get(lease_id)
    if (
        lease is None
        or not authorization
        or not authorization.lower().startswith("bearer ")
    ):
        return None
    token = authorization.split(None, 1)[1]
    return lease if secrets.compare_digest(token, lease.token) else None


def _lease_remaining(lease: Lease) -> int:
    if not lease.usable:
        return 0
    daily_remaining = max(
        0, DAILY_BUDGET_BYTES - _daily_total_bytes() - _daily_reserved_bytes
    )
    run_key = lease.dagrun_key
    url_key = (run_key, lease.canonical_url)
    run_remaining = max(
        0,
        _lease_dagrun_budget_bytes(lease)
        - _run_total_bytes(run_key)
        - _run_reserved_bytes[run_key],
    )
    url_remaining = max(
        0,
        _lease_url_budget_bytes(lease)
        - _url_total_bytes(run_key, lease.canonical_url)
        - _url_reserved_bytes[url_key],
    )
    parent_remaining = (
        max(
            0,
            lease.parent_run_cap_bytes
            - lease.parent_run_spent_provider_bytes
            - lease.reserved_bytes,
        )
        if lease.source == "sofascore"
        else daily_remaining
    )
    return min(
        max(0, lease.max_bytes - lease.total_bytes - lease.reserved_bytes),
        daily_remaining,
        run_remaining,
        url_remaining,
        parent_remaining,
    )


def _reserve_lease_bytes(lease: Lease, wanted: int) -> int:
    """Atomically reserve allowance before an async read/write yields control."""
    global _daily_reserved_bytes
    count = min(max(0, wanted), _lease_remaining(lease))
    lease.reserved_bytes += count
    _daily_reserved_bytes += count
    _run_reserved_bytes[lease.dagrun_key] += count
    _url_reserved_bytes[(lease.dagrun_key, lease.canonical_url)] += count
    return count


def _release_lease_reservation(lease: Lease, count: int) -> None:
    global _daily_reserved_bytes
    lease.reserved_bytes = max(0, lease.reserved_bytes - count)
    _daily_reserved_bytes = max(0, _daily_reserved_bytes - count)
    run_key = lease.dagrun_key
    url_key = (run_key, lease.canonical_url)
    _run_reserved_bytes[run_key] = max(0, _run_reserved_bytes[run_key] - count)
    _url_reserved_bytes[url_key] = max(0, _url_reserved_bytes[url_key] - count)


def _account_lease_bytes(lease: Lease, host: str, direction: str, count: int) -> None:
    """Account bytes that were actually written across the paid upstream leg."""
    global _daily_up_bytes, _daily_down_bytes
    if count <= 0:
        return
    if lease.source == "sofascore":
        if (
            lease.workload_plan is None
            or lease.allocation_claim is None
            or not lease.current_endpoint
        ):
            raise RuntimeError(
                "production SofaScore bytes have no signed endpoint allocation"
            )
        envelope = _parent_envelope_ledger().consume(
            dag_id=lease.dag_id,
            base_run_id=lease.base_run_id,
            phase=lease.workload_phase,
            phase_plan_digest=lease.workload_plan.plan_digest,
            provider_bytes=count,
        )
        lease.parent_run_cap_bytes = envelope.parent_cap_bytes
        lease.parent_run_spent_provider_bytes = envelope.parent_spent_provider_bytes
        # Persist the immutable allocation charge before any later lease can
        # reuse this remaining allowance.  The pre-read reservation already
        # guarantees that this chunk cannot cross the cap.
        _allocation_ledger().consume(
            lease.workload_plan,
            lease.allocation_claim,
            count,
        )
    _refresh_daily_counter()
    host_stats = lease.hosts.setdefault(host, {"up_bytes": 0, "down_bytes": 0})
    if direction == "up":
        lease.up_bytes += count
        _daily_up_bytes += count
        _run_up_bytes[lease.dagrun_key] += count
        _url_up_bytes[(lease.dagrun_key, lease.canonical_url)] += count
        host_stats["up_bytes"] += count
        up_bytes[host] += count
    else:
        lease.down_bytes += count
        _daily_down_bytes += count
        _run_down_bytes[lease.dagrun_key] += count
        _url_down_bytes[(lease.dagrun_key, lease.canonical_url)] += count
        host_stats["down_bytes"] += count
        down_bytes[host] += count
    try:
        _append_budget_event(
            "bytes",
            lease,
            host=host,
            direction=direction,
            bytes=count,
            lease_total_bytes=lease.total_bytes,
            dagrun_total_bytes=_run_total_bytes(lease.dagrun_key),
            url_total_bytes=_url_total_bytes(lease.dagrun_key, lease.canonical_url),
        )
    except Exception:
        log.exception(
            "paid byte ledger append failed; closing lease %s", lease.lease_id
        )
        lease.budget_exceeded = True
        raise RuntimeError("durable paid byte accounting failed")
    if (
        lease.total_bytes >= lease.max_bytes
        or _daily_total_bytes() >= DAILY_BUDGET_BYTES
        or _run_total_bytes(lease.dagrun_key) >= _lease_dagrun_budget_bytes(lease)
        or _url_total_bytes(lease.dagrun_key, lease.canonical_url)
        >= _lease_url_budget_bytes(lease)
        or (
            lease.source == "sofascore"
            and lease.parent_run_spent_provider_bytes >= lease.parent_run_cap_bytes
        )
    ):
        lease.budget_exceeded = True


def _acquire_upstream(mgr):
    """Return the residential upstream for a new tunnel, refreshing it only when idle.

    A fresh random exit is drawn only when no tunnel is currently open
    (``_active == 0``): so all tunnels within one FlareSolverr session reuse the
    same exit IP (the page + its Turnstile challenge — CF-safe), and the next
    session (which destroys the tab, closing every tunnel through us) draws a new
    one. Callers MUST ``_active += 1`` right after and ``-= 1`` when the tunnel
    closes. Safe without a lock: asyncio is cooperative and this runs with no
    ``await`` between the check and the caller's increment."""
    global _current_up
    if _active == 0 or _current_up is None:
        _current_up = _pick_upstream(mgr)
        log.info(
            "residential upstream selected: %s",
            _upstream_fingerprint(_current_up),
        )
    return _current_up


async def _pump(
    reader: asyncio.StreamReader,
    writer: asyncio.StreamWriter,
    host: str,
    counter: dict[str, int],
    budget_guard=None,
    *,
    lease: Lease | None = None,
    direction: str | None = None,
) -> None:
    if lease is not None and budget_guard is not None:
        raise ValueError("lease and legacy budget guard are mutually exclusive")
    try:
        while True:
            read_size = 65536
            reservation = 0
            precharged = False
            if lease is not None:
                reservation = _reserve_lease_bytes(lease, read_size)
                if reservation <= 0:
                    lease.budget_exceeded = True
                    break
                read_size = reservation
            try:
                metered_read = getattr(budget_guard, "read_metered", None)
                precharged = callable(metered_read)
                if precharged:
                    chunk = await metered_read(reader, read_size)
                else:
                    chunk = await reader.read(read_size)
            except Exception:
                if lease is not None:
                    _release_lease_reservation(lease, reservation)
                raise
            if lease is not None:
                # No await between release and accounting: asyncio cannot let
                # a sibling tunnel steal this allowance in the middle.
                _release_lease_reservation(lease, reservation)
                if chunk:
                    assert direction in ("up", "down")
                    _account_lease_bytes(lease, host, direction, len(chunk))
            elif budget_guard is not None and chunk and not precharged:
                budget_guard.consume(len(chunk))
            if not chunk:
                break
            writer.write(chunk)
            await writer.drain()
            if lease is None:
                counter[host] += len(chunk)
    except Exception:  # noqa: BLE001 — proxy must never crash a flow
        pass
    finally:
        try:
            writer.close()
        except Exception:  # noqa: BLE001
            pass


async def _read_headers(reader: asyncio.StreamReader) -> list[bytes]:
    lines: list[bytes] = []
    while True:
        h = await reader.readline()
        if h in (b"\r\n", b"\n", b""):
            return lines
        lines.append(h)


async def _read_metered_provider_head(
    reader: asyncio.StreamReader,
    lease: Lease,
    host: str,
    timeout_seconds: float | None = None,
) -> tuple[bytes, list[bytes]]:
    """Read an upstream HTTP response head without crossing a paid budget.

    ``StreamReader.readline`` has no per-call byte limit.  Reading the provider
    response one byte at a time under one pre-reserved window is deliberate:
    CONNECT heads are small, and this guarantees that even a maliciously large
    header cannot be read (and billed) past the daily/DagRun/lease boundary.

    ``timeout_seconds`` bounds a silent exit that accepts the CONNECT but never
    replies (#946).  The deadline is enforced *per read* from inside this
    function — never as an external ``wait_for`` around the whole call — so a
    cancellation cannot abandon the partial-byte accounting below.  With
    ``timeout_seconds=None`` the behaviour is byte-for-byte unchanged.
    """
    reservation = _reserve_lease_bytes(
        lease,
        min(MAX_PROVIDER_RESPONSE_HEAD_BYTES, _lease_remaining(lease)),
    )
    if reservation <= 0:
        lease.budget_exceeded = True
        raise RuntimeError("provider budget exhausted before response head")
    payload = bytearray()
    complete = False
    timed_out = False
    deadline = (
        None if timeout_seconds is None else time.monotonic() + timeout_seconds
    )
    try:
        while len(payload) < reservation:
            if deadline is None:
                item = await reader.read(1)
            else:
                remaining = deadline - time.monotonic()
                if remaining <= 0:
                    timed_out = True
                    break
                try:
                    item = await asyncio.wait_for(reader.read(1), remaining)
                except (asyncio.TimeoutError, TimeoutError):
                    timed_out = True
                    break
            if not item:
                break
            payload.extend(item)
            if payload.endswith(b"\r\n\r\n") or payload.endswith(b"\n\n"):
                complete = True
                break
    finally:
        _release_lease_reservation(lease, reservation)
    if payload:
        _account_lease_bytes(lease, host, "down", len(payload))
    if timed_out:
        raise UpstreamHeadTimeout("provider response head timed out")
    if not complete:
        if len(payload) >= reservation:
            lease.budget_exceeded = True
            raise RuntimeError("incomplete or over-budget provider response head")
        raise UpstreamHeadIncomplete(
            "provider closed before a complete response head"
        )
    lines = bytes(payload).splitlines(keepends=True)
    if not lines:
        raise RuntimeError("empty provider response head")
    return lines[0], lines[1:-1]


def _header_map(lines: list[bytes]) -> dict[str, str]:
    headers: dict[str, str] = {}
    for line in lines:
        try:
            name, value = line.decode("latin1").split(":", 1)
        except ValueError:
            continue
        headers[name.strip().lower()] = value.strip()
    return headers


async def _send_json(
    writer: asyncio.StreamWriter, status: int, payload: dict[str, Any]
) -> None:
    reason = {
        200: "OK",
        201: "Created",
        400: "Bad Request",
        401: "Unauthorized",
        404: "Not Found",
        409: "Conflict",
        413: "Content Too Large",
        429: "Too Many Requests",
        500: "Internal Server Error",
        503: "Service Unavailable",
    }.get(status, "Error")
    body = json.dumps(payload, separators=(",", ":")).encode("utf-8")
    writer.write(
        f"HTTP/1.1 {status} {reason}\r\nContent-Type: application/json\r\n"
        f"Content-Length: {len(body)}\r\nConnection: close\r\n\r\n".encode()
        + body
    )
    await writer.drain()
    writer.close()


def _begin_endpoint_request(lease: Lease, endpoint: str) -> str:
    endpoint = str(endpoint or "").strip()
    if not endpoint or len(endpoint) > 200:
        raise ValueError("endpoint must be a non-empty bounded name")
    if lease.closed or lease.expired or lease.current_request_id:
        raise RuntimeError("lease cannot start a concurrent endpoint request")
    request_id = uuid_hex(24)
    if lease.source == "sofascore":
        _append_allocation_wal(
            "endpoint_started",
            lease.lease_id,
            request_id=request_id,
            endpoint=endpoint,
        )
    lease.current_request_id = request_id
    lease.current_endpoint = endpoint
    lease.current_request_start_bytes = lease.total_bytes
    return request_id


def _finish_endpoint_request(lease: Lease, request_id: str) -> int:
    if not lease.current_request_id or not secrets.compare_digest(
        lease.current_request_id, str(request_id or "")
    ):
        raise ValueError("endpoint request id is stale or invalid")
    endpoint = lease.current_endpoint
    amount = lease.total_bytes - lease.current_request_start_bytes
    if amount < 0:
        raise RuntimeError("lease provider counter moved backwards")
    if lease.source == "sofascore":
        _append_allocation_wal(
            "endpoint_finished",
            lease.lease_id,
            request_id=lease.current_request_id,
            endpoint=endpoint,
            provider_bytes=amount,
        )
    lease.endpoint_request_provider_bytes.setdefault(endpoint, []).append(amount)
    lease.current_request_id = ""
    lease.current_endpoint = ""
    lease.current_request_start_bytes = lease.total_bytes
    return amount


def _reap_expired_leases() -> int:
    """Finalize drained TTL-expired leases without minting retry allowance.

    A worker can disappear after acquiring a production allocation and never
    call the control-plane close endpoint.  TTL already revokes its data plane,
    but the durable allocation claim must also be released once every tunnel
    and byte reservation has drained.  Provider bytes were charged eagerly by
    ``_account_lease_bytes``; finishing the claim records their endpoint map and
    lets the next attempt use only the original allocation's remainder.

    This function is synchronous on purpose.  The control server runs it on its
    event-loop thread before creating another lease, so claim finalization and
    the subsequent retry claim cannot interleave.
    """

    reaped = 0
    for lease in tuple(LEASES.values()):
        if not lease.expired or lease.active_tunnels or lease.reserved_bytes:
            continue
        if lease.source == "sofascore" and not lease.allocation_finished:
            if lease.workload_plan is None or lease.allocation_claim is None:
                raise RuntimeError(
                    "expired SofaScore lease has no signed allocation claim"
                )
            lease.closed = True
            for tunnel_writer in tuple(lease.tunnel_writers):
                try:
                    tunnel_writer.close()
                except Exception:  # noqa: BLE001 - lease is already revoked
                    pass
            if lease.current_request_id:
                _finish_endpoint_request(lease, lease.current_request_id)
            _allocation_ledger().finish(
                lease.workload_plan,
                lease.allocation_claim,
                lease_id=lease.lease_id,
                endpoint_request_provider_bytes=(
                    lease.endpoint_request_provider_bytes
                ),
                completed=False,
                meter=WORKLOAD_METER,
                proxy_exit_hash=lease.proxy_exit_hash,
            )
            _append_allocation_wal(
                "allocation_finished",
                lease.lease_id,
                completed=False,
                expired=True,
            )
            lease.allocation_finished = True
            reaped += 1
        elif not lease.closed:
            # Canary/legacy leases have no allocation claim, but their expired
            # data plane should still be represented as closed in reports.
            lease.closed = True
            reaped += 1
        if lease.closed and not lease.close_recorded:
            try:
                _append_budget_event(
                    "lease_closed",
                    lease,
                    total_bytes=lease.total_bytes,
                    expired=True,
                )
                lease.close_recorded = True
            except Exception:  # noqa: BLE001 - byte deltas are already durable
                log.exception("could not persist expiry for lease %s", lease.lease_id)
    return reaped


def _normalize_endpoint_map(value: object) -> dict[str, list[int]]:
    if not isinstance(value, Mapping):
        raise ValueError("endpoint_request_provider_bytes must be an object")
    normalized: dict[str, list[int]] = {}
    for endpoint, raw_values in sorted(value.items()):
        name = str(endpoint or "").strip()
        if (
            not name
            or not isinstance(raw_values, Sequence)
            or isinstance(raw_values, (str, bytes, bytearray))
        ):
            raise ValueError("endpoint provider observations are invalid")
        observations: list[int] = []
        for raw in raw_values:
            if isinstance(raw, bool) or not isinstance(raw, int) or raw < 0:
                raise ValueError(
                    "endpoint provider bytes must be non-negative integers"
                )
            observations.append(raw)
        if not observations:
            raise ValueError("endpoint provider observations must not be empty")
        normalized[name] = observations
    return normalized


async def _close_lease(
    lease: Lease,
    *,
    completed: bool = False,
    endpoint_request_provider_bytes: object = None,
    proxy_exit_hash: object = None,
) -> dict[str, Any]:
    """Stop all tunnels and report success only after counters are final."""
    if not isinstance(completed, bool):
        raise ValueError("completed must be boolean")
    lease.closed = True
    for tunnel_writer in tuple(lease.tunnel_writers):
        try:
            tunnel_writer.close()
        except Exception:  # noqa: BLE001
            pass
    deadline = time.monotonic() + 2.0
    while lease.active_tunnels and time.monotonic() < deadline:
        await asyncio.sleep(0.01)
    drained = lease.active_tunnels == 0 and lease.reserved_bytes == 0
    if drained and lease.current_request_id:
        _finish_endpoint_request(lease, lease.current_request_id)
    client_map_matches = True
    if lease.source == "sofascore":
        try:
            reported_map = _normalize_endpoint_map(
                endpoint_request_provider_bytes
                if endpoint_request_provider_bytes is not None
                else {}
            )
        except ValueError:
            reported_map = {}
            client_map_matches = False
        if reported_map != lease.endpoint_request_provider_bytes:
            client_map_matches = False
        if proxy_exit_hash is not None:
            candidate = str(proxy_exit_hash)
            if len(candidate) < 12 or len(candidate) > 128:
                client_map_matches = False
            else:
                lease.proxy_exit_hash = candidate
        if (
            drained
            and not lease.allocation_finished
            and lease.workload_plan is not None
            and lease.allocation_claim is not None
        ):
            _allocation_ledger().finish(
                lease.workload_plan,
                lease.allocation_claim,
                lease_id=lease.lease_id,
                endpoint_request_provider_bytes=(lease.endpoint_request_provider_bytes),
                completed=bool(
                    completed and client_map_matches and not lease.budget_exceeded
                ),
                meter=WORKLOAD_METER,
                proxy_exit_hash=lease.proxy_exit_hash,
            )
            _append_allocation_wal(
                "allocation_finished",
                lease.lease_id,
                completed=bool(
                    completed and client_map_matches and not lease.budget_exceeded
                ),
            )
            lease.allocation_finished = True
    if drained and not lease.close_recorded:
        try:
            _append_budget_event("lease_closed", lease, total_bytes=lease.total_bytes)
            lease.close_recorded = True
        except Exception:
            log.exception("could not persist close for lease %s", lease.lease_id)
    report = _control_report(lease)
    # ``closed`` stops new traffic immediately. ``close_complete`` is the
    # stronger control-plane acknowledgement: every provider tunnel and byte
    # reservation is drained and the final counter was durably journalled.
    report["close_complete"] = bool(
        drained
        and lease.close_recorded
        and (lease.source != "sofascore" or lease.allocation_finished)
        and client_map_matches
    )
    if not client_map_matches:
        report["close_error"] = "endpoint provider map mismatch"
    return report


def _control_report(lease: Lease) -> dict[str, Any]:
    report = lease.report()
    report["daily_total_bytes"] = _daily_total_bytes()
    report["daily_budget_bytes"] = DAILY_BUDGET_BYTES
    return report


def _service_health_report(mgr) -> dict[str, Any]:
    """Credential-free configuration and counters, never pool identities."""

    remaining = max(0, DAILY_BUDGET_BYTES - _daily_total_bytes())
    return {
        "status": "ok",
        "meter": PROVIDER_METER_ID,
        "daily_total_bytes": _daily_total_bytes(),
        "daily_budget_bytes": DAILY_BUDGET_BYTES,
        "daily_remaining_bytes": remaining,
        "max_lease_bytes": MAX_LEASE_BYTES,
        "max_lease_ttl_seconds": MAX_LEASE_TTL_SECONDS,
        "max_active_leases": MAX_ACTIVE_LEASES,
        "dagrun_budget_bytes": DAGRUN_BUDGET_BYTES,
        "url_budget_bytes": URL_BUDGET_BYTES,
        "lease_proxy_url": LEASE_PROXY_URL,
        "configured_pool_count": int(mgr.total_count),
        "fbref_source_ready": bool(
            FBREF_DAG_IDS
            and DAGRUN_BUDGET_BYTES > 0
            and URL_BUDGET_BYTES > 0
            and MAX_LEASE_BYTES > 0
            and int(mgr.total_count) > 0
        ),
        "fbref_dag_ids": sorted(FBREF_DAG_IDS),
        "sofascore_paid_enabled": SOFASCORE_DAGRUN_BUDGET_BYTES > 0,
        "sofascore_dagrun_budget_bytes": SOFASCORE_DAGRUN_BUDGET_BYTES,
        "sofascore_budget_artifact_id": SOFASCORE_BUDGET_ARTIFACT_ID,
        "sofascore_canary_enabled": SOFASCORE_CANARY_HARD_CAP_BYTES > 0,
        "sofascore_canary_hard_cap_bytes": SOFASCORE_CANARY_HARD_CAP_BYTES,
        "sofascore_canary_policy_id": SOFASCORE_CANARY_POLICY_ID,
        "sofascore_discovery_enabled": (
            SOFASCORE_DISCOVERY_DAGRUN_BUDGET_BYTES > 0
        ),
        "sofascore_discovery_dagrun_budget_bytes": (
            SOFASCORE_DISCOVERY_DAGRUN_BUDGET_BYTES
        ),
    }


async def _handle_control(
    method: str,
    target: str,
    headers: dict[str, str],
    reader: asyncio.StreamReader,
    writer: asyncio.StreamWriter,
    mgr,
) -> bool:
    """Serve the lease API on the proxy listener; return False for proxy traffic."""
    path = urlsplit(target).path
    if method == "GET" and path == "/health":
        await _send_json(writer, 200, _service_health_report(mgr))
        return True

    if method == "GET" and path == "/v1/auth-check":
        if not _control_token_valid(headers):
            await _send_json(writer, 401, {"error": "invalid control token"})
            return True
        await _send_json(writer, 200, _service_health_report(mgr))
        return True
    if path.startswith("/v1/leases") and not _control_token_valid(headers):
        await _send_json(writer, 401, {"error": "invalid control token"})
        return True
    if not path.startswith("/v1/leases"):
        return False
    if method == "POST" and path == "/v1/leases":
        try:
            length = int(headers.get("content-length", "0"))
            if length < 0 or length > MAX_CONTROL_BODY_BYTES:
                raise ValueError(
                    f"lease request body must be in 0..{MAX_CONTROL_BODY_BYTES} bytes"
                )
            body = await reader.readexactly(length) if length else b"{}"
            request = json.loads(body)
            if not isinstance(request, dict):
                raise ValueError("lease request body must be a JSON object")
            lease = _create_lease(
                mgr,
                max_bytes=int(request.get("max_bytes", DEFAULT_LEASE_BYTES)),
                ttl_seconds=int(request.get("ttl_seconds", DEFAULT_LEASE_TTL_SECONDS)),
                metadata=request,
                require_context=True,
            )
        except (ValueError, TypeError, json.JSONDecodeError) as exc:
            await _send_json(writer, 400, {"error": str(exc)})
            return True
        except AllocationError as exc:
            name = exc.__class__.__name__
            code = re.sub(r"(?<!^)(?=[A-Z])", "_", name).lower()
            status = 429 if isinstance(exc, AllocationBudgetExceeded) else 409
            await _send_json(writer, status, {"code": code, "error": str(exc)})
            return True
        except RuntimeError as exc:
            message = str(exc)
            code = (
                "concurrency_limited" if "concurrency" in message else "budget_exceeded"
            )
            await _send_json(writer, 429, {"code": code, "error": message})
            return True
        await _send_json(
            writer,
            201,
            {
                "id": lease.lease_id,
                "token": lease.token,
                "max_bytes": lease.max_bytes,
                "expires_at": lease.expires_at,
                "proxy_url": LEASE_PROXY_URL,
                "plan_digest": (
                    lease.workload_plan.plan_digest if lease.workload_plan else ""
                ),
                "allocation_id": lease.allocation_id,
                "allocation_budget_bytes": lease.allocation_budget_bytes,
            },
        )
        return True

    parts = path.strip("/").split("/")
    if len(parts) == 4 and parts[:2] == ["v1", "leases"] and parts[3] == "endpoints":
        lease = _authorized_control_lease(parts[2], headers.get("authorization"))
        if lease is None:
            await _send_json(writer, 401, {"error": "invalid lease token"})
            return True
        if method != "POST":
            await _send_json(writer, 404, {"error": "unknown lease endpoint"})
            return True
        try:
            length = int(headers.get("content-length", "0"))
            if length <= 0 or length > 4096:
                raise ValueError("endpoint request body must be in 1..4096 bytes")
            body = json.loads((await reader.readexactly(length)).decode("utf-8"))
            if not isinstance(body, dict):
                raise ValueError("endpoint request body must be an object")
            request_id = _begin_endpoint_request(lease, body.get("endpoint"))
        except (ValueError, TypeError, json.JSONDecodeError, UnicodeDecodeError) as exc:
            await _send_json(writer, 400, {"error": str(exc)})
            return True
        except RuntimeError as exc:
            await _send_json(
                writer, 409, {"code": "endpoint_concurrent", "error": str(exc)}
            )
            return True
        await _send_json(writer, 201, {"request_id": request_id})
        return True
    if len(parts) == 5 and parts[:2] == ["v1", "leases"] and parts[3] == "endpoints":
        lease = _authorized_control_lease(parts[2], headers.get("authorization"))
        if lease is None:
            await _send_json(writer, 401, {"error": "invalid lease token"})
            return True
        if method != "DELETE":
            await _send_json(writer, 404, {"error": "unknown lease endpoint"})
            return True
        try:
            _finish_endpoint_request(lease, parts[4])
        except ValueError as exc:
            await _send_json(writer, 409, {"error": str(exc)})
            return True
        except RuntimeError as exc:
            await _send_json(
                writer,
                503,
                {"code": "endpoint_accounting_unavailable", "error": str(exc)},
            )
            return True
        await _send_json(writer, 200, _control_report(lease))
        return True
    if len(parts) != 4 or parts[:2] != ["v1", "leases"]:
        await _send_json(writer, 404, {"error": "unknown lease endpoint"})
        return True
    lease_id, action = parts[2], parts[3]
    lease = _authorized_control_lease(lease_id, headers.get("authorization"))
    if lease is None:
        await _send_json(writer, 401, {"error": "invalid lease token"})
        return True
    if method == "GET" and action == "stats":
        await _send_json(writer, 200, _control_report(lease))
        return True
    if method == "DELETE" and action == "close":
        try:
            length = int(headers.get("content-length", "0"))
            if length < 0 or length > MAX_CONTROL_BODY_BYTES:
                raise ValueError("lease close body is too large")
            close_request = (
                json.loads((await reader.readexactly(length)).decode("utf-8"))
                if length
                else {}
            )
            if not isinstance(close_request, dict):
                raise ValueError("lease close body must be an object")
            report = await _close_lease(
                lease,
                completed=close_request.get("completed", False),
                endpoint_request_provider_bytes=close_request.get(
                    "endpoint_request_provider_bytes"
                ),
                proxy_exit_hash=close_request.get("proxy_exit_hash"),
            )
        except (ValueError, TypeError, json.JSONDecodeError, UnicodeDecodeError) as exc:
            await _send_json(writer, 400, {"error": str(exc)})
            return True
        except AllocationError as exc:
            await _send_json(
                writer,
                409,
                {"code": "allocation_close_rejected", "error": str(exc)},
            )
            return True
        if report["close_complete"]:
            await _send_json(writer, 200, report)
        else:
            await _send_json(
                writer,
                409,
                {
                    "code": "lease_close_pending",
                    "error": "lease provider counters are not final",
                    **report,
                },
            )
        return True
    # Convenience: DELETE /v1/leases/{id} is represented by three path parts,
    # handled below before declaring the route unknown.
    await _send_json(writer, 404, {"error": "unknown lease endpoint"})
    return True


async def _handle_control_delete_short(
    method: str,
    target: str,
    headers: dict[str, str],
    writer: asyncio.StreamWriter,
) -> bool:
    path = urlsplit(target).path
    parts = path.strip("/").split("/")
    if method != "DELETE" or len(parts) != 3 or parts[:2] != ["v1", "leases"]:
        return False
    if not _control_token_valid(headers):
        await _send_json(writer, 401, {"error": "invalid control token"})
        return True
    lease = _authorized_control_lease(parts[2], headers.get("authorization"))
    if lease is None:
        await _send_json(writer, 401, {"error": "invalid lease token"})
        return True
    report = await _close_lease(lease)
    if report["close_complete"]:
        await _send_json(writer, 200, report)
    else:
        await _send_json(
            writer,
            409,
            {
                "code": "lease_close_pending",
                "error": "lease provider counters are not final",
                **report,
            },
        )
    return True


async def _write_upstream(
    writer: asyncio.StreamWriter,
    payload: bytes,
    *,
    lease: Lease | None,
    host: str,
    direction: str,
) -> bool:
    reservation = 0
    if lease is not None:
        reservation = _reserve_lease_bytes(lease, len(payload))
        if reservation != len(payload):
            _release_lease_reservation(lease, reservation)
            lease.budget_exceeded = True
            return False
    writer.write(payload)
    if lease is not None:
        _release_lease_reservation(lease, reservation)
        _account_lease_bytes(lease, host, direction, len(payload))
    await writer.drain()
    return True


async def _open_lease_upstream_tunnel(
    lease: Lease,
    mgr,
    *,
    target: str,
    host: str,
) -> tuple[asyncio.StreamReader, asyncio.StreamWriter, bytes, list[bytes]]:
    """Open the lease's CONNECT tunnel, failing over a silent residential exit.

    A dead exit accepts the TCP connection and then never answers the CONNECT.
    Each attempt dials the lease's currently-pinned exit, sends the CONNECT head
    (its up-bytes are metered even against a dead exit) and reads the metered
    response head under a deadline.  On a connect/write/head failure the exit is
    re-pinned to a fresh pool entry — but only before the first *down* byte is
    billed (``down_bytes == 0``): the up-bytes already charged to the dead exit
    make the running total unusable as the failover gate.  Every failed attempt
    closes its socket and unregisters it from ``tunnel_writers``.  Credentials
    and ``host:port`` are never logged — only non-reversible fingerprint hashes.
    """
    last_error: BaseException | None = None
    for _attempt in range(1 + LEASE_UPSTREAM_FAILOVER_ATTEMPTS):
        up_host, up_port, up_user, up_pass = lease.upstream
        srv_w = None
        try:
            srv_r, srv_w = await asyncio.wait_for(
                _open_upstream_connection(up_host, up_port),
                LEASE_UPSTREAM_CONNECT_TIMEOUT_SECONDS,
            )
            lease.tunnel_writers.add(srv_w)
            auth = base64.b64encode(f"{up_user}:{up_pass}".encode()).decode()
            connect_request = (
                f"CONNECT {target} HTTP/1.1\r\nHost: {target}\r\n"
                f"Proxy-Authorization: Basic {auth}\r\n\r\n".encode()
            )
            # An OSError here (exit accepts, then RSTs the CONNECT write) is
            # failover-eligible like a silent head; a budget refusal is not.
            if not await _write_upstream(
                srv_w, connect_request, lease=lease, host=host, direction="up"
            ):
                raise _LeaseBudgetRefused()
            status, response_headers = await _read_metered_provider_head(
                srv_r,
                lease,
                host,
                timeout_seconds=LEASE_PROVIDER_HEAD_TIMEOUT_SECONDS,
            )
            return srv_r, srv_w, status, response_headers
        except BaseException as exc:
            # No failed attempt may leak its socket or its tunnel_writers entry.
            if srv_w is not None:
                try:
                    srv_w.close()
                except Exception:  # noqa: BLE001 - exit is already dead
                    pass
                lease.tunnel_writers.discard(srv_w)
            if not isinstance(
                exc,
                (
                    asyncio.TimeoutError,
                    TimeoutError,
                    OSError,
                    UpstreamHeadTimeout,
                    UpstreamHeadIncomplete,
                ),
            ):
                # Budget refusals (429), over-budget heads and cancellation are
                # not dead-exit signals: never failover, surface them as before.
                raise
            last_error = exc
        # Reached only on a connect/write/head failure: re-pin a fresh exit
        # while no provider byte has yet been received on this lease, and retry.
        if (
            lease.down_bytes == 0
            and lease.usable
            and _attempt < LEASE_UPSTREAM_FAILOVER_ATTEMPTS
        ):
            previous = lease.upstream
            # The pool draw is random: re-draw (bounded) so the replacement is
            # not the exit that just failed, unless the pool has nothing else.
            candidate = _pick_upstream(mgr)
            for _redraw in range(5):
                if candidate != previous:
                    break
                candidate = _pick_upstream(mgr)
            lease.upstream = candidate
            lease.upstream_repins += 1
            log.warning(
                "lease %s residential exit unreachable; failing over %s -> %s",
                lease.lease_id,
                _upstream_fingerprint(previous),
                _upstream_fingerprint(lease.upstream),
            )
            continue
        raise last_error


async def handle(
    client_r: asyncio.StreamReader,
    client_w: asyncio.StreamWriter,
    mgr,
    *,
    require_lease: bool = False,
) -> None:
    global _active
    try:
        first = await client_r.readline()
        if not first:
            client_w.close()
            return
        parts = first.decode("latin1").split()
        if len(parts) < 2:
            client_w.close()
            return
        method, target = parts[0].upper(), parts[1]
        raw_headers = await _read_headers(client_r)
        headers = _header_map(raw_headers)
        if await _handle_control_delete_short(method, target, headers, client_w):
            return
        if await _handle_control(method, target, headers, client_r, client_w, mgr):
            return
        host = (
            target.rsplit(":", 1)[0]
            if method == "CONNECT"
            else (urlsplit(target).hostname or target)
        )

        if _is_blocked(host):
            blocked_count[host] += 1
            # refuse without dialing the residential upstream → bytes never billed
            client_w.write(b"HTTP/1.1 403 Forbidden\r\n\r\n")
            await client_w.drain()
            client_w.close()
            return

        proxy_authorization = headers.get("proxy-authorization")
        lease = _lease_from_proxy_authorization(proxy_authorization)
        if (require_lease and lease is None) or (proxy_authorization and lease is None):
            client_w.write(
                b"HTTP/1.1 407 Proxy Authentication Required\r\n"
                b"Proxy-Authenticate: Basic realm=lease\r\n\r\n"
            )
            await client_w.drain()
            client_w.close()
            return
        if lease is not None and not lease.usable:
            client_w.write(b"HTTP/1.1 429 Too Many Requests\r\n\r\n")
            await client_w.drain()
            client_w.close()
            return
        if (
            lease is not None
            and lease.source == "sofascore"
            and not lease.current_endpoint
        ):
            # A signed allocation is necessary but not sufficient: production
            # traffic also needs an active endpoint boundary so every billed
            # byte has exact table provenance after a crash.
            client_w.write(b"HTTP/1.1 409 Conflict\r\n\r\n")
            await client_w.drain()
            client_w.close()
            return
        if not _lease_host_allowed(lease, host):
            blocked_count[host] += 1
            client_w.write(b"HTTP/1.1 403 Forbidden\r\n\r\n")
            await client_w.drain()
            client_w.close()
            return

        # Explicit callers use the immutable lease upstream.  Credential-less
        # callers retain the old idle-refresh path until their transports are
        # migrated, which preserves compatibility without weakening lease
        # isolation.
        if lease is not None:
            up_host, up_port, up_user, up_pass = lease.upstream
            lease.active_tunnels += 1
            lease.tunnel_writers.add(client_w)
        else:
            up_host, up_port, up_user, up_pass = _acquire_upstream(mgr)
            _active += 1
        auth = base64.b64encode(f"{up_user}:{up_pass}".encode()).decode()
        srv_w = None
        try:
            if method == "CONNECT":
                conn_count[host] += 1
                if lease is not None:
                    # Bounded dial + metered head, failing a silent exit over to
                    # a fresh pool entry so one dead exit cannot latch the slot.
                    try:
                        (
                            srv_r,
                            srv_w,
                            status,
                            response_headers,
                        ) = await _open_lease_upstream_tunnel(
                            lease, mgr, target=target, host=host
                        )
                    except _LeaseBudgetRefused:
                        client_w.write(b"HTTP/1.1 429 Too Many Requests\r\n\r\n")
                        await client_w.drain()
                        return
                    except (
                        asyncio.TimeoutError,
                        TimeoutError,
                        OSError,
                        UpstreamHeadTimeout,
                        UpstreamHeadIncomplete,
                    ):
                        # Do not close the lease: the finally below drains this
                        # slot and the TTL reaper finalizes it as usual.
                        client_w.write(b"HTTP/1.1 502 Bad Gateway\r\n\r\n")
                        await client_w.drain()
                        client_w.close()
                        return
                else:
                    srv_r, srv_w = await asyncio.open_connection(up_host, up_port)
                    connect_request = (
                        f"CONNECT {target} HTTP/1.1\r\nHost: {target}\r\n"
                        f"Proxy-Authorization: Basic {auth}\r\n\r\n".encode()
                    )
                    if not await _write_upstream(
                        srv_w, connect_request, lease=lease, host=host, direction="up"
                    ):
                        client_w.write(b"HTTP/1.1 429 Too Many Requests\r\n\r\n")
                        await client_w.drain()
                        return
                    status = await srv_r.readline()
                    await _read_headers(srv_r)
                if b"200" not in status:
                    client_w.write(b"HTTP/1.1 502 Bad Gateway\r\n\r\n")
                    await client_w.drain()
                    client_w.close()
                    srv_w.close()
                    return
                client_w.write(b"HTTP/1.1 200 Connection established\r\n\r\n")
                await client_w.drain()
                await asyncio.gather(
                    _pump(
                        client_r,
                        srv_w,
                        host,
                        up_bytes,
                        provider_budget_guard if lease is None else None,
                        lease=lease,
                        direction="up",
                    ),
                    _pump(
                        srv_r,
                        client_w,
                        host,
                        down_bytes,
                        provider_budget_guard if lease is None else None,
                        lease=lease,
                        direction="down",
                    ),
                )
            else:
                conn_count[host] += 1
                try:
                    srv_r, srv_w = await asyncio.wait_for(
                        _open_upstream_connection(up_host, up_port),
                        LEASE_UPSTREAM_CONNECT_TIMEOUT_SECONDS,
                    )
                except (asyncio.TimeoutError, TimeoutError, OSError):
                    client_w.write(b"HTTP/1.1 502 Bad Gateway\r\n\r\n")
                    await client_w.drain()
                    client_w.close()
                    return
                if lease is not None:
                    lease.tunnel_writers.add(srv_w)
                forwarded_headers = b"".join(
                    line
                    for line in raw_headers
                    if not line.lower().startswith(b"proxy-authorization:")
                )
                request_head = (
                    first
                    + forwarded_headers
                    + f"Proxy-Authorization: Basic {auth}\r\n\r\n".encode()
                )
                if not await _write_upstream(
                    srv_w, request_head, lease=lease, host=host, direction="up"
                ):
                    client_w.write(b"HTTP/1.1 429 Too Many Requests\r\n\r\n")
                    await client_w.drain()
                    return
                await asyncio.gather(
                    _pump(
                        client_r,
                        srv_w,
                        host,
                        up_bytes,
                        provider_budget_guard if lease is None else None,
                        lease=lease,
                        direction="up",
                    ),
                    _pump(
                        srv_r,
                        client_w,
                        host,
                        down_bytes,
                        provider_budget_guard if lease is None else None,
                        lease=lease,
                        direction="down",
                    ),
                )
        finally:
            if lease is not None:
                lease.tunnel_writers.discard(client_w)
                if srv_w is not None:
                    lease.tunnel_writers.discard(srv_w)
                lease.active_tunnels = max(0, lease.active_tunnels - 1)
            else:
                _active -= 1
    except Exception:  # noqa: BLE001
        try:
            client_w.close()
        except Exception:  # noqa: BLE001
            pass


def _dump(out_path: str, quiet: bool = False) -> None:
    os.makedirs(os.path.dirname(out_path) or ".", exist_ok=True)
    hosts = sorted(
        set(up_bytes) | set(down_bytes), key=lambda h: -(up_bytes[h] + down_bytes[h])
    )
    rows = [
        {
            "host": h,
            "conns": conn_count.get(h, 0),
            "down_mb": round(down_bytes[h] / 1048576, 3),
            "up_mb": round(up_bytes[h] / 1048576, 3),
            "total_mb": round((up_bytes[h] + down_bytes[h]) / 1048576, 3),
        }
        for h in hosts
    ]
    total = sum(up_bytes.values()) + sum(down_bytes.values())
    report = {
        "total_mb": round(total / 1048576, 3),
        "daily": {
            "day": _daily_day or _utc_day(),
            "up_bytes": _daily_up_bytes,
            "down_bytes": _daily_down_bytes,
            "total_bytes": _daily_total_bytes(),
            "budget_bytes": DAILY_BUDGET_BYTES,
        },
        "leases": [lease.report() for lease in LEASES.values()],
        "dagruns": [
            {
                "key": key,
                "up_bytes": _run_up_bytes[key],
                "down_bytes": _run_down_bytes[key],
                "total_bytes": _run_total_bytes(key),
                "budget_bytes": _dagrun_budget_bytes(key.split("/", 1)[0]),
            }
            for key in sorted(set(_run_up_bytes) | set(_run_down_bytes))
        ],
        "allowed_hosts": rows,
        "blocked_hosts": sorted(
            ({"host": h, "attempts": c} for h, c in blocked_count.items()),
            key=lambda r: -r["attempts"],
        ),
    }
    if provider_budget_guard is not None and provider_budget_endpoint:
        # Backwards-compatible standalone canary report.  Production leases
        # expose exact per-session counters through ``/v1/leases/{id}/stats``.
        report["total_provider_bytes"] = total
        report["endpoint_provider_bytes"] = {provider_budget_endpoint: total}
        report["endpoint_request_provider_bytes"] = {provider_budget_endpoint: [total]}
    tmp = out_path + ".tmp"
    with open(tmp, "w") as fh:
        json.dump(report, fh, indent=2)
    os.replace(tmp, out_path)
    if not quiet:
        log.info("=== allowed (total %.2f MB) ===", total / 1048576)
        for r in rows[:30]:
            log.info("  %8.3f MB  (%dx)  %s", r["total_mb"], r["conns"], r["host"])
        log.info("=== blocked attempts: %d hosts ===", len(blocked_count))
        log.info("wrote %s", out_path)


async def _periodic_dump(out_path: str, interval: float = 2.0) -> None:
    while True:
        await asyncio.sleep(interval)
        try:
            _reap_expired_leases()
            _dump(out_path, quiet=True)
        except Exception:  # noqa: BLE001
            log.exception("periodic proxy lease cleanup/report failed")


class _SharedBudgetGuard:
    """Compatibility adapter for the existing standalone canary ledger."""

    def __init__(
        self,
        ledger: SharedBudgetLedger,
        run_id: str,
        reservation_token: str,
    ) -> None:
        self.ledger = ledger
        self.run_id = run_id
        self.reservation_token = reservation_token

    async def read_metered(
        self,
        reader: asyncio.StreamReader,
        max_bytes: int,
    ) -> bytes:
        claimed = self.ledger.claim(
            self.run_id,
            self.reservation_token,
            max_bytes,
        )
        try:
            chunk = await reader.read(claimed)
        except BaseException:
            self.ledger.refund(
                self.run_id,
                self.reservation_token,
                claimed,
            )
            raise
        unused = claimed - len(chunk)
        if unused:
            self.ledger.refund(
                self.run_id,
                self.reservation_token,
                unused,
            )
        return chunk

    def consume(self, amount: int) -> None:
        self.ledger.consume(self.run_id, self.reservation_token, amount)


async def main() -> None:
    global BLOCKLIST, DAILY_BUDGET_BYTES, MAX_LEASE_BYTES, LEASE_PROXY_URL
    global MAX_LEASE_TTL_SECONDS, DAGRUN_BUDGET_BYTES
    global TRANSFERMARKT_DAGRUN_BUDGET_BYTES
    global SOFASCORE_DAGRUN_BUDGET_BYTES, SOFASCORE_BUDGET_ARTIFACT_ID
    global SOFASCORE_CANARY_HARD_CAP_BYTES, SOFASCORE_CANARY_POLICY_ID
    global SOFASCORE_DISCOVERY_DAGRUN_BUDGET_BYTES
    global URL_BUDGET_BYTES, MAX_ACTIVE_LEASES, LEDGER_PATH, CONTROL_TOKEN
    global LEASE_UPSTREAM_CONNECT_TIMEOUT_SECONDS, LEASE_PROVIDER_HEAD_TIMEOUT_SECONDS
    global LEASE_UPSTREAM_FAILOVER_ATTEMPTS
    global SOFASCORE_ALLOCATION_LEDGER_PATH, SOFASCORE_ALLOCATION_WAL_PATH
    global SOFASCORE_ALLOCATION_LEDGER, _SOFASCORE_ALLOCATION_LEDGER_KEY
    global SOFASCORE_PARENT_ENVELOPE_PATH, SOFASCORE_PARENT_ENVELOPE_LEDGER
    global _SOFASCORE_PARENT_ENVELOPE_LEDGER_PATH
    global _daily_day, _daily_up_bytes, _daily_down_bytes
    global provider_budget_guard, provider_budget_endpoint
    ap = argparse.ArgumentParser()
    ap.add_argument("--listen", default="0.0.0.0:8899")
    ap.add_argument(
        "--lease-listen",
        default="0.0.0.0:8900",
        help="authenticated proxy listener used by explicit leases",
    )
    ap.add_argument(
        "--lease-proxy-url",
        default=os.environ.get("PROXY_FILTER_LEASE_URL", "http://proxy_filter:8900"),
        help="lease proxy URL returned by POST /v1/leases",
    )
    ap.add_argument("--proxy-file", default="/opt/airflow/proxys.txt")
    ap.add_argument(
        "--allow-proxy-file-fallback",
        action="store_true",
        help=(
            "explicitly allow --proxy-file only when PROXY_POOL_JSON is blank; "
            "disabled by default"
        ),
    )
    ap.add_argument(
        "--blocklist", default=None, help="domain blocklist file (omit = observe only)"
    )
    ap.add_argument("--out", default="/tmp/filter_bytes.json")
    ap.add_argument("--pidfile", default="/tmp/filter_proxy.pid")
    ap.add_argument("--daily-budget-mb", type=float, default=100.0)
    ap.add_argument("--max-lease-mb", type=float, default=24.0)
    ap.add_argument(
        "--max-lease-ttl-seconds",
        type=int,
        default=MAX_LEASE_TTL_SECONDS,
    )
    ap.add_argument("--dagrun-budget-bytes", type=int, default=8_000_000)
    ap.add_argument(
        "--transfermarkt-dagrun-budget-bytes",
        type=int,
        default=TRANSFERMARKT_DAGRUN_BUDGET_BYTES,
    )
    ap.add_argument("--url-budget-bytes", type=int, default=2_000_000)
    ap.add_argument("--max-active-leases", type=int, default=MAX_ACTIVE_LEASES)
    ap.add_argument(
        "--lease-upstream-connect-timeout-seconds",
        type=float,
        default=float(
            os.environ.get(
                "PROXY_FILTER_LEASE_CONNECT_TIMEOUT_SECONDS",
                LEASE_UPSTREAM_CONNECT_TIMEOUT_SECONDS,
            )
        ),
        help="bound the residential CONNECT dial for a lease tunnel",
    )
    ap.add_argument(
        "--lease-provider-head-timeout-seconds",
        type=float,
        default=float(
            os.environ.get(
                "PROXY_FILTER_LEASE_HEAD_TIMEOUT_SECONDS",
                LEASE_PROVIDER_HEAD_TIMEOUT_SECONDS,
            )
        ),
        help="bound the metered upstream response-head read for a lease tunnel",
    )
    ap.add_argument(
        "--lease-upstream-failover-attempts",
        type=int,
        default=int(
            os.environ.get(
                "PROXY_FILTER_LEASE_FAILOVER_ATTEMPTS",
                LEASE_UPSTREAM_FAILOVER_ATTEMPTS,
            )
        ),
        help="silent-exit re-pin attempts before failing a lease CONNECT closed",
    )
    ap.add_argument(
        "--ledger",
        default=os.environ.get(
            "PROXY_FILTER_LEDGER_PATH",
            "/opt/airflow/logs/proxy_filter/paid_requests.jsonl",
        ),
        help="durable append-only paid byte ledger",
    )
    ap.add_argument(
        "--sofascore-budget-artifact",
        default=os.environ.get("SOFASCORE_PROXY_BUDGET_ARTIFACT"),
        help=(
            "verified SofaScore canary; its measured hard_run_bytes becomes "
            "the SofaScore DagRun lease cap"
        ),
    )
    ap.add_argument(
        "--sofascore-allocation-ledger",
        default=os.environ.get(
            "SOFASCORE_PROXY_ALLOCATION_LEDGER_PATH",
            SOFASCORE_ALLOCATION_LEDGER_PATH,
        ),
        help="atomic signed SofaScore allocation state",
    )
    ap.add_argument(
        "--sofascore-allocation-wal",
        default=os.environ.get(
            "SOFASCORE_PROXY_ALLOCATION_WAL_PATH",
            SOFASCORE_ALLOCATION_WAL_PATH,
        ),
        help="mode-0600 recovery WAL for active SofaScore claims",
    )
    ap.add_argument(
        "--sofascore-parent-envelope",
        default=os.environ.get(
            "SOFASCORE_PROXY_PARENT_ENVELOPE_PATH",
            SOFASCORE_PARENT_ENVELOPE_PATH,
        ),
        help=("atomic cap shared by SofaScore season/targets/players phase plans"),
    )
    ap.add_argument(
        "--sofascore-canary-hard-cap-bytes",
        type=int,
        default=int(
            os.environ.get("PROXY_FILTER_SOFASCORE_CANARY_HARD_CAP_BYTES", "0")
        ),
        help=(
            "explicit experimental cap for dag_canary_sofascore_proxy; "
            "zero disables bootstrap canaries and never authorizes production"
        ),
    )
    ap.add_argument(
        "--sofascore-discovery-dagrun-budget-bytes",
        type=int,
        default=int(
            os.environ.get("PROXY_FILTER_SOFASCORE_DISCOVERY_DAGRUN_BUDGET_BYTES", "0")
        ),
        help=(
            "explicit hard cap for one dag_discover_sofascore_registry scan; "
            "zero (default) refuses every metered discovery lease"
        ),
    )
    # Kept for the existing offline canary process and SharedBudgetLedger API.
    # Production warmed sessions use leases instead.
    ap.add_argument("--budget-artifact")
    ap.add_argument("--budget-ledger")
    ap.add_argument("--budget-run-id")
    ap.add_argument("--budget-reservation-token")
    ap.add_argument("--budget-endpoint")
    ap.add_argument("--budget-workload-class")
    args = ap.parse_args()

    CONTROL_TOKEN = os.environ.get("PROXY_FILTER_CONTROL_TOKEN", "")
    if len(CONTROL_TOKEN) < 32:
        raise SystemExit(
            "PROXY_FILTER_CONTROL_TOKEN must contain at least 32 characters"
        )

    daily_budget_mb = float(getattr(args, "daily_budget_mb", 100.0))
    max_lease_mb = float(getattr(args, "max_lease_mb", 24.0))
    max_lease_ttl_seconds = int(
        getattr(args, "max_lease_ttl_seconds", MAX_LEASE_TTL_SECONDS)
    )
    dagrun_budget_bytes = int(getattr(args, "dagrun_budget_bytes", DAGRUN_BUDGET_BYTES))
    transfermarkt_budget_bytes = int(
        getattr(
            args,
            "transfermarkt_dagrun_budget_bytes",
            TRANSFERMARKT_DAGRUN_BUDGET_BYTES,
        )
    )
    url_budget_bytes = int(getattr(args, "url_budget_bytes", URL_BUDGET_BYTES))
    max_active_leases = int(getattr(args, "max_active_leases", MAX_ACTIVE_LEASES))
    lease_connect_timeout_seconds = float(
        getattr(
            args,
            "lease_upstream_connect_timeout_seconds",
            LEASE_UPSTREAM_CONNECT_TIMEOUT_SECONDS,
        )
    )
    lease_head_timeout_seconds = float(
        getattr(
            args,
            "lease_provider_head_timeout_seconds",
            LEASE_PROVIDER_HEAD_TIMEOUT_SECONDS,
        )
    )
    lease_failover_attempts = int(
        getattr(
            args,
            "lease_upstream_failover_attempts",
            LEASE_UPSTREAM_FAILOVER_ATTEMPTS,
        )
    )
    sofascore_canary_hard_cap_bytes = int(
        getattr(args, "sofascore_canary_hard_cap_bytes", 0)
    )
    sofascore_discovery_budget_bytes = int(
        getattr(args, "sofascore_discovery_dagrun_budget_bytes", 0)
    )
    if (
        daily_budget_mb <= 0
        or max_lease_mb <= 0
        or max_lease_ttl_seconds <= 0
        or dagrun_budget_bytes <= 0
        or transfermarkt_budget_bytes <= 0
        or url_budget_bytes <= 0
        or max_active_leases <= 0
        or sofascore_canary_hard_cap_bytes < 0
        or sofascore_discovery_budget_bytes < 0
    ):
        raise SystemExit("proxy byte budgets must be positive")
    # ``inf``/``nan`` pass a bare ``<= 0`` check but would disable the dead-exit
    # bound entirely, so the timeouts require strictly finite positive values.
    if (
        not (
            math.isfinite(lease_connect_timeout_seconds)
            and lease_connect_timeout_seconds > 0
        )
        or not (
            math.isfinite(lease_head_timeout_seconds)
            and lease_head_timeout_seconds > 0
        )
        or lease_failover_attempts < 0
    ):
        raise SystemExit(
            "lease upstream timeouts must be finite positive seconds and "
            "failover attempts must be >= 0"
        )
    DAILY_BUDGET_BYTES = int(daily_budget_mb * 1024 * 1024)
    MAX_LEASE_BYTES = int(max_lease_mb * 1024 * 1024)
    MAX_LEASE_TTL_SECONDS = max_lease_ttl_seconds
    DAGRUN_BUDGET_BYTES = dagrun_budget_bytes
    TRANSFERMARKT_DAGRUN_BUDGET_BYTES = transfermarkt_budget_bytes
    URL_BUDGET_BYTES = url_budget_bytes
    MAX_ACTIVE_LEASES = max_active_leases
    LEASE_UPSTREAM_CONNECT_TIMEOUT_SECONDS = lease_connect_timeout_seconds
    LEASE_PROVIDER_HEAD_TIMEOUT_SECONDS = lease_head_timeout_seconds
    LEASE_UPSTREAM_FAILOVER_ATTEMPTS = lease_failover_attempts
    LEDGER_PATH = str(getattr(args, "ledger", LEDGER_PATH))
    SOFASCORE_ALLOCATION_LEDGER_PATH = str(
        getattr(
            args,
            "sofascore_allocation_ledger",
            SOFASCORE_ALLOCATION_LEDGER_PATH,
        )
    )
    SOFASCORE_ALLOCATION_WAL_PATH = str(
        getattr(args, "sofascore_allocation_wal", SOFASCORE_ALLOCATION_WAL_PATH)
    )
    SOFASCORE_ALLOCATION_LEDGER = None
    _SOFASCORE_ALLOCATION_LEDGER_KEY = None
    SOFASCORE_PARENT_ENVELOPE_PATH = str(
        getattr(
            args,
            "sofascore_parent_envelope",
            SOFASCORE_PARENT_ENVELOPE_PATH,
        )
    )
    SOFASCORE_PARENT_ENVELOPE_LEDGER = None
    _SOFASCORE_PARENT_ENVELOPE_LEDGER_PATH = ""
    LEASE_PROXY_URL = str(getattr(args, "lease_proxy_url", LEASE_PROXY_URL)).rstrip("/")

    SOFASCORE_DAGRUN_BUDGET_BYTES = 0
    SOFASCORE_BUDGET_ARTIFACT_ID = ""
    SOFASCORE_CANARY_HARD_CAP_BYTES = sofascore_canary_hard_cap_bytes
    SOFASCORE_CANARY_POLICY_ID = (
        _canary_policy_id(SOFASCORE_CANARY_HARD_CAP_BYTES)
        if SOFASCORE_CANARY_HARD_CAP_BYTES > 0
        else ""
    )
    if SOFASCORE_CANARY_HARD_CAP_BYTES > 0:
        MAX_LEASE_BYTES = max(MAX_LEASE_BYTES, SOFASCORE_CANARY_HARD_CAP_BYTES)
        log.warning(
            "experimental SofaScore canary enabled: cap=%d policy=%s production_authorized=false",
            SOFASCORE_CANARY_HARD_CAP_BYTES,
            SOFASCORE_CANARY_POLICY_ID,
        )
    # A discovery scan is deliberately served by consecutive leases bounded by
    # MAX_LEASE_BYTES; the DagRun cap is the whole-scan ceiling and never
    # raises the per-lease maximum.
    SOFASCORE_DISCOVERY_DAGRUN_BUDGET_BYTES = sofascore_discovery_budget_bytes
    if SOFASCORE_DISCOVERY_DAGRUN_BUDGET_BYTES > 0:
        log.warning(
            "metered SofaScore registry discovery enabled: dagrun_cap=%d",
            SOFASCORE_DISCOVERY_DAGRUN_BUDGET_BYTES,
        )
    sofascore_artifact = getattr(args, "sofascore_budget_artifact", None)
    if sofascore_artifact:
        try:
            sofascore_policy = load_verified_workload_policy(sofascore_artifact)
        except (ProductionBudgetUnavailable, WorkloadPolicyUnavailable) as exc:
            # Other live lease consumers remain available, but SofaScore stays
            # fail-closed until a reviewed canary is checked in.
            log.warning("SofaScore paid leases disabled: %s", exc)
        else:
            SOFASCORE_DAGRUN_BUDGET_BYTES = max(
                item.hard_task_bytes for item in sofascore_policy.classes.values()
            )
            SOFASCORE_BUDGET_ARTIFACT_ID = sofascore_policy.artifact_id
            MAX_LEASE_BYTES = max(
                MAX_LEASE_BYTES,
                SOFASCORE_DAGRUN_BUDGET_BYTES,
            )
            log.info(
                "SofaScore signed allocations enabled: max_allocation_bytes=%d artifact=%s",
                SOFASCORE_DAGRUN_BUDGET_BYTES,
                SOFASCORE_BUDGET_ARTIFACT_ID,
            )

    out_path = str(getattr(args, "out", "/tmp/filter_bytes.json"))
    _restore_daily_counter(out_path)
    report_daily = (_daily_up_bytes, _daily_down_bytes)
    _daily_day = ""
    _daily_up_bytes = _daily_down_bytes = 0
    restored_events = _restore_budget_ledger(LEDGER_PATH, restore_daily=True)
    if sum(report_daily) > _daily_total_bytes():
        # Conservative compatibility for bytes recorded before the WAL was
        # deployed. Never add report+WAL, which would double count.
        _daily_day = _utc_day()
        _daily_up_bytes, _daily_down_bytes = report_daily
    log.info("restored %d durable paid byte events", restored_events)
    recovered_allocations = _recover_allocation_wal()
    log.info(
        "recovered %d crash-orphaned SofaScore allocation attempts",
        recovered_allocations,
    )

    pidfile = str(getattr(args, "pidfile", "/tmp/filter_proxy.pid"))
    with open(pidfile, "w") as fh:
        fh.write(str(os.getpid()))

    blocklist = getattr(args, "blocklist", None)
    BLOCKLIST = _load_blocklist(blocklist)
    log.info(
        "blocklist: %d domains from %s",
        len(BLOCKLIST),
        blocklist or "(none — observe mode)",
    )

    proxy_file = str(getattr(args, "proxy_file", "/opt/airflow/proxys.txt"))
    env_file_fallback = os.environ.get("PROXY_FILTER_ALLOW_FILE_FALLBACK", "false")
    if env_file_fallback.lower() not in {"true", "false"}:
        raise SystemExit("PROXY_FILTER_ALLOW_FILE_FALLBACK must be true or false")
    allow_file_fallback = (
        bool(getattr(args, "allow_proxy_file_fallback", False))
        or env_file_fallback.lower() == "true"
    )
    try:
        mgr, pool_source = _residential_manager(
            proxy_pool_json=os.environ.get(PROXY_POOL_ENV),
            proxy_file=proxy_file,
            allow_file_fallback=allow_file_fallback,
        )
    except (OSError, ProxyPoolConfigurationError) as exc:
        raise SystemExit(f"proxy pool configuration error: {exc}") from None
    log.info(
        "residential pool = %d proxies from %s "
        "(explicit sticky leases; legacy idle-refresh enabled)",
        mgr.total_count,
        pool_source,
    )
    listen = str(getattr(args, "listen", "0.0.0.0:8899"))
    host, port = listen.rsplit(":", 1)

    budget_args = (
        getattr(args, "budget_artifact", None),
        getattr(args, "budget_ledger", None),
        getattr(args, "budget_run_id", None),
        getattr(args, "budget_reservation_token", None),
        getattr(args, "budget_endpoint", None),
    )
    if any(budget_args) and not all(budget_args):
        raise SystemExit("all --budget-* arguments are required together")
    provider_budget_guard = None
    provider_budget_endpoint = None
    if all(budget_args):
        policy = load_verified_policy(
            budget_args[0],
            workload_class=getattr(args, "budget_workload_class", None),
        )
        compatibility_ledger = SharedBudgetLedger(budget_args[1], policy)
        provider_budget_guard = _SharedBudgetGuard(
            compatibility_ledger,
            str(budget_args[2]),
            str(budget_args[3]),
        )
        provider_budget_endpoint = str(budget_args[4])
        log.info(
            "standalone canary budget active run=%s endpoint=%s artifact=%s",
            budget_args[2],
            provider_budget_endpoint,
            policy.artifact_id,
        )

    server = await asyncio.start_server(lambda r, w: handle(r, w, mgr), host, int(port))
    lease_server = None
    lease_listen = getattr(args, "lease_listen", None)
    if lease_listen:
        lease_host, lease_port = str(lease_listen).rsplit(":", 1)
        lease_server = await asyncio.start_server(
            lambda r, w: handle(r, w, mgr, require_lease=True),
            lease_host,
            int(lease_port),
        )
    log.info(
        "listening on %s (lease API + authenticated proxy; legacy no-auth enabled)",
        listen,
    )
    if lease_server is not None:
        log.info(
            "authenticated lease proxy listening on %s (advertised as %s)",
            lease_listen,
            LEASE_PROXY_URL,
        )

    asyncio.ensure_future(_periodic_dump(out_path))

    stop = asyncio.Event()
    loop = asyncio.get_running_loop()
    for sig in (signal.SIGTERM, signal.SIGINT):
        loop.add_signal_handler(sig, stop.set)

    if lease_server is None:
        async with server:
            await stop.wait()
    else:
        async with server, lease_server:
            await stop.wait()
    _dump(out_path)


if __name__ == "__main__":
    asyncio.run(main())
