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
Port 8899 requires lease authentication for proxy traffic by default.  The
credential-less route exists only behind the development-only
``--allow-legacy-noauth`` flag.
"""

# ruff: noqa: E402 -- the trust anchor must run before every non-built-in import

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
import asyncio
import base64
import binascii
import fcntl
import hashlib
import hmac
import ipaddress
import json
import logging
import math
import os
import re
import secrets
import signal
import ssl
import stat
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
for _import_root in (_REPO_ROOT,):
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
from scrapers.whoscored.proxy_campaign import (  # noqa: E402 - standalone entry point
    DEFAULT_WHOSCORED_PAID_CAP_BYTES,
    WHOSCORED_CANARY_DAG_ID,
    PROXY_CAMPAIGN_METER,
    PROXY_CAMPAIGN_CONTROL_ARGUMENT_FIELDS,
    PROXY_CAMPAIGN_CONTROL_RESULT_FIELDS,
    PROXY_CAMPAIGN_CONTROL_SCHEMA_VERSION,
    WHOSCORED_PAID_APPLICATION_GATEWAY_AVAILABLE,
    WHOSCORED_PROVIDER_INVOICE_HARD_CAP_AVAILABLE,
    WHOSCORED_FULL_PAID_CRAWL_AVAILABLE,
    WHOSCORED_PAID_DAG_IDS,
    WHOSCORED_PROXY_ALLOWED_HOSTS,
    SCHEDULED_PROXY_CAMPAIGN_SCHEMA_VERSION,
    ProxyCampaignApproval,
    ProxyCampaignBudgetExceeded,
    ProxyCampaignClaim,
    ProxyCampaignConcurrencyLimited,
    ProxyCampaignError,
    ProxyCampaignLedger,
    ProxyCampaignValidationError,
    ProxyWorkAllocation,
    approval_from_campaign_authority_context,
    approval_from_context,
    canonical_json_bytes,
    daily_ingest_paid_crawl_allowed,
    strict_json_loads,
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
# callers which authenticate as ``lease:<token>``.  Production also requires a
# lease on the control listener; credential-less proxying is development-only.
DEFAULT_LEASE_BYTES = 8 * 1024 * 1024
MAX_LEASE_BYTES = 24 * 1024 * 1024
DEFAULT_LEASE_TTL_SECONDS = 60
MAX_LEASE_TTL_SECONDS = 3600
DAILY_BUDGET_BYTES = 100 * 1024 * 1024
DAGRUN_BUDGET_BYTES = 8_000_000
# Transfermarkt has no signed workload plan.  Keep its paid path disabled until
# an operator supplies both a source-scoped token and an explicit deployment cap.
TRANSFERMARKT_DAGRUN_BUDGET_BYTES = 0
TRANSFERMARKT_DAG_IDS = frozenset(
    {
        "dag_ingest_transfermarkt",
        "dag_discover_transfermarkt_registry",
    }
)
TRANSFERMARKT_PROXY_ALLOWED_HOSTS = frozenset(
    {"www.transfermarkt.com", "www.transfermarkt.us"}
)
FBREF_DAG_IDS = frozenset(
    {
        "dag_ingest_fbref",
        "dag_bootstrap_fbref",
        "dag_backfill_fbref",
        "dag_accept_fbref_bronze",
    }
)
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
# No environment/CLI scalar can enable WhoScored.  A valid signed campaign is
# the only authority which replaces this zero at lease-creation time.
WHOSCORED_DAGRUN_BUDGET_BYTES = DEFAULT_WHOSCORED_PAID_CAP_BYTES
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
WHOSCORED_CAMPAIGN_LEDGER_PATH = (
    "/opt/airflow/logs/proxy_filter/whoscored_campaigns.json"
)
WHOSCORED_STATE_MARKER_PATH = (
    "/opt/airflow/state/whoscored-proxy-filter/.whoscored_state_initialized.json"
)
# Report/checkpoint payloads remain v1; the namespace marker itself is v2 and
# binds that whole protected state tree to one exact provider order/policy.
WHOSCORED_STATE_SCHEMA_VERSION = 1
WHOSCORED_STATE_MARKER_SCHEMA_VERSION = 2
WHOSCORED_PAID_LEDGER_CHAIN_SCHEMA_VERSION = 1
# The owner-approved provider policy caps this order at exact decimal bytes.
# Neither MiB conversion nor CLI/environment may enlarge these outer ceilings.
WHOSCORED_PROVIDER_ORDER_SAFETY_CAP_BYTES = 300_000_000
WHOSCORED_MAX_LEASE_SAFETY_CAP_BYTES = 2_000_000
MAX_LEDGER_EVENT_BYTES = 256 * 1024
MAX_ALLOCATION_WAL_EVENT_BYTES = 4 * 1024 * 1024
MAX_CONTROL_BODY_BYTES = 4 * 1024 * 1024
MAX_PROVIDER_RESPONSE_HEAD_BYTES = 64 * 1024
# WhoScored leases escrow their complete byte ceiling (and fsync that escrow)
# before the first provider dial.  Socket observations can therefore be
# journalled in bounded batches without moving the hard-cap boundary after
# I/O.  Four MiB keeps normal page-sized leases to one terminal flush while a
# large measurement lease still emits regular durable checkpoints.
WHOSCORED_METER_BATCH_BYTES = 4 * 1024 * 1024
# Completed lease objects are only a short idempotency/debug cache.  Exact
# forensic history lives in the authenticated campaign and paid-byte ledgers;
# keeping every token and nested host map in RAM would make the 2-second status
# dump grow without bound during a long canary.
MAX_FINALIZED_LEASES = 128
FINALIZED_LEASE_TTL_SECONDS = 15 * 60
# Client request heads arrive before either the control token or a paid-proxy
# lease can be authenticated.  Keep that unauthenticated surface small and
# time-bounded so another backend container cannot slowloris this 256 MiB
# service or force it to retain an unbounded number of header lines.
MAX_CLIENT_REQUEST_LINE_BYTES = 8 * 1024
MAX_CLIENT_HEADER_LINE_BYTES = 8 * 1024
MAX_CLIENT_HEADER_BYTES = 32 * 1024
MAX_CLIENT_HEADER_COUNT = 64
CLIENT_HEAD_TIMEOUT_SECONDS = 5.0
MAX_PREAUTH_CONNECTIONS = 32
_CLIENT_HEAD_SLOTS = asyncio.BoundedSemaphore(MAX_PREAUTH_CONNECTIONS)
# The WhoScored data plane accepts the local CONNECT before contacting the paid
# provider, then buffers only the first plaintext TLS ClientHello.  This makes
# the browser reveal its SNI while the provider dial count is still zero.
WHOSCORED_CLIENT_HELLO_TIMEOUT_SECONDS = 5.0
MAX_WHOSCORED_CLIENT_HELLO_BYTES = 64 * 1024
MAX_TLS_PLAINTEXT_RECORD_BYTES = 16 * 1024
MAX_PENDING_WHOSCORED_CLIENT_HELLOS = 4
MAX_PENDING_WHOSCORED_CLIENT_HELLOS_PER_LEASE = 1
_WHOSCORED_CLIENT_HELLO_SLOTS = asyncio.BoundedSemaphore(
    MAX_PENDING_WHOSCORED_CLIENT_HELLOS
)
_TLS_HANDSHAKE_CONTENT_TYPE = 22
_TLS_CLIENT_HELLO_HANDSHAKE_TYPE = 1
_TLS_SERVER_NAME_EXTENSION = 0
_TLS_ECH_EXTENSION_TYPES = frozenset({0xFE0D, 0xFFCE})
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
TRANSFERMARKT_CONTROL_TOKEN = ""
WHOSCORED_PROXY_APPROVAL_HMAC_SECRET = ""
WHOSCORED_PROXY_LEDGER_HMAC_SECRET = ""
SOFASCORE_ALLOCATION_LEDGER: AllocationLedger | None = None
_SOFASCORE_ALLOCATION_LEDGER_KEY: tuple[str, str] | None = None
SOFASCORE_PARENT_ENVELOPE_LEDGER: "ParentRunEnvelopeLedger | None" = None
_SOFASCORE_PARENT_ENVELOPE_LEDGER_PATH = ""
WHOSCORED_CAMPAIGN_LEDGER: ProxyCampaignLedger | None = None
_WHOSCORED_CAMPAIGN_LEDGER_KEY: tuple[str, str, str, bool] | None = None
WHOSCORED_PROXY_RUNTIME_SHA256 = ""
WHOSCORED_STATE_ID = ""
WHOSCORED_PROVIDER_ORDER_ID = ""
WHOSCORED_PROVIDER_POLICY_SHA256 = ""
WHOSCORED_LEGACY_STATE_MARKER_LOADED = False
_PAID_LEDGER_CHAIN_COUNT = 0
_PAID_LEDGER_CHAIN_OFFSET = 0
_PAID_LEDGER_CHAIN_TAIL = ""
SOURCE_MODE = "shared-no-whoscored"
SOFASCORE_CHALLENGE_HOSTS = frozenset(
    {"challenges.cloudflare.com", "turnstile.cloudflare.com"}
)
FBREF_ALLOWED_HOSTS = frozenset(
    {
        "api.ipify.org",
        "challenges.cloudflare.com",
        "fbref.com",
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
_PROVIDER_HTTP_STATUS_LINE = re.compile(
    rb"HTTP/1\.[01] ([0-9]{3})(?:[ \t][^\r\n]*)?\r?\n"
)
_CANONICAL_TOKEN_RE = re.compile(r"\A[A-Za-z0-9][A-Za-z0-9._:-]{0,127}\Z", re.ASCII)
_LOWER_SHA256_RE = re.compile(r"\A[0-9a-f]{64}\Z", re.ASCII)
# The dedicated WhoScored pool is reached through the PROXYS.IO dial name, but
# the provider presents an Infatica certificate.  Keep both names code-owned:
# using the dial name for ``server_hostname`` fails certificate verification,
# while a configurable SNI value would turn hostname verification into an
# operator-controlled downgrade.  This transport is enabled only for the
# isolated ``whoscored-only`` service; the shared legacy pool is unchanged.
WHOSCORED_UPSTREAM_DIAL_HOST = "pool.proxys.io"
WHOSCORED_UPSTREAM_TLS_SERVER_NAME = "pool.infatica.io"


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


class ClientHelloValidationError(ValueError):
    """A pre-provider ClientHello which cannot authorize a WhoScored dial."""


def _new_whoscored_upstream_tls_context() -> ssl.SSLContext:
    """Return a system-CA context which cannot silently disable verification."""

    context = ssl.create_default_context(ssl.Purpose.SERVER_AUTH)
    context.minimum_version = ssl.TLSVersion.TLSv1_2
    context.verify_mode = ssl.CERT_REQUIRED
    context.check_hostname = True
    return context


# Reuse one immutable-by-convention context so every paid connection gets the
# same CA policy and TLS session setup does not repeatedly reload the CA store.
_WHOSCORED_UPSTREAM_TLS_CONTEXT = _new_whoscored_upstream_tls_context()


async def _open_upstream_connection(
    host: str,
    port: int,
) -> tuple[asyncio.StreamReader, asyncio.StreamWriter]:
    """Dial an upstream, using verified TLS for the isolated WhoScored pool.

    ``server_hostname`` supplies both SNI and the hostname checked against the
    certificate.  There is deliberately no plaintext retry after a TLS error.
    """

    if SOURCE_MODE != "whoscored-only":
        return await asyncio.open_connection(host, port)
    if host != WHOSCORED_UPSTREAM_DIAL_HOST:
        raise ProxyPoolConfigurationError(
            "WhoScored upstream does not match the pinned provider dial host"
        )
    return await asyncio.open_connection(
        host,
        port,
        ssl=_WHOSCORED_UPSTREAM_TLS_CONTEXT,
        server_hostname=WHOSCORED_UPSTREAM_TLS_SERVER_NAME,
    )


def _dagrun_budget_bytes(dag_id: str) -> int:
    """Return the source-specific hard cap without weakening WhoScored."""
    if dag_id in SOFASCORE_CANARY_DAG_IDS:
        return SOFASCORE_CANARY_HARD_CAP_BYTES
    if dag_id in SOFASCORE_DISCOVERY_DAG_IDS:
        return SOFASCORE_DISCOVERY_DAGRUN_BUDGET_BYTES
    if dag_id in SOFASCORE_DAG_IDS:
        return SOFASCORE_DAGRUN_BUDGET_BYTES
    if dag_id in WHOSCORED_PAID_DAG_IDS:
        return WHOSCORED_DAGRUN_BUDGET_BYTES
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
    if dag_id in WHOSCORED_PAID_DAG_IDS:
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
    if lease.proxy_campaign_approval is not None:
        return lease.proxy_campaign_approval.approval_sha256
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
    proxy_campaign_approval: ProxyCampaignApproval | None = field(
        default=None, repr=False
    )
    proxy_campaign_claim: ProxyCampaignClaim | None = field(default=None, repr=False)
    proxy_work_allocation: ProxyWorkAllocation | None = field(default=None, repr=False)
    proxy_attempt_id: str = ""
    proxy_work_item_id: str = ""
    proxy_campaign_finished: bool = False
    provider_request_count: int = 0
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
    target_manifest_sha256: str = ""
    logical_target_units: int = 1
    expected_endpoint_labels: tuple[str, ...] = ()
    proxy_exit_hash: str | None = None
    allocation_finished: bool = False
    up_bytes: int = 0
    down_bytes: int = 0
    reserved_bytes: int = 0
    # Mirror of this lease's slice of the durable provider-order escrow.  It is
    # reduced only by observed provider bytes or a clean durable release.
    global_budget_escrow_bytes: int = 0
    active_tunnels: int = 0
    pending_client_hellos: int = field(default=0, repr=False)
    upstream_repins: int = 0
    closed: bool = False
    close_recorded: bool = False
    budget_exceeded: bool = False
    accounting_uncertain: bool = False
    # Provider bytes are observed immediately for the in-process hard cap, but
    # WhoScored persists them in bounded aggregates on top of the already
    # durable full-lease escrow.  A failed flush is never retried because the
    # campaign state may have committed before the proxy WAL append failed;
    # that ambiguity revokes the campaign and retains the remaining escrow.
    pending_whoscored_bytes: dict[tuple[str, str], int] = field(
        default_factory=dict, repr=False
    )
    settled_whoscored_bytes: int = 0
    metering_flush_failed: bool = False
    finalized_at: float = 0.0
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
        return (
            not self.closed
            and not self.expired
            and not self.budget_exceeded
            and not self.accounting_uncertain
        )

    def report(self) -> dict[str, Any]:
        pending_provider_bytes = sum(self.pending_whoscored_bytes.values())
        settled_provider_bytes = (
            self.settled_whoscored_bytes
            if self.source == "whoscored"
            else self.total_bytes
        )
        report = {
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
            "global_budget_escrow_bytes": self.global_budget_escrow_bytes,
            "upstream_repins": self.upstream_repins,
            "closed": self.closed,
            "expired": self.expired,
            "budget_exceeded": self.budget_exceeded,
            "accounting_uncertain": self.accounting_uncertain,
            "pending_provider_bytes": pending_provider_bytes,
            "durably_settled_provider_bytes": settled_provider_bytes,
            "metering_flush_failed": self.metering_flush_failed,
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
                else (
                    int(self.proxy_campaign_claim.allocation_spent_provider_bytes)
                    + self.total_bytes
                    if self.proxy_campaign_claim
                    else 0
                )
            ),
            "allocation_remaining_provider_bytes": (
                max(
                    0,
                    int(self.allocation_claim.remaining_provider_bytes)
                    - self.total_bytes,
                )
                if self.allocation_claim
                else (
                    max(
                        0,
                        int(self.proxy_campaign_claim.remaining_provider_bytes)
                        - self.total_bytes,
                    )
                    if self.proxy_campaign_claim
                    else 0
                )
            ),
            "proxy_campaign_id": (
                self.proxy_campaign_approval.campaign_id
                if self.proxy_campaign_approval
                else ""
            ),
            "proxy_approval_id": (
                self.proxy_campaign_approval.approval_id
                if self.proxy_campaign_approval
                else ""
            ),
            "proxy_approval_sha256": (
                self.proxy_campaign_approval.approval_sha256
                if self.proxy_campaign_approval
                else ""
            ),
            "provider_billed_bytes": self.total_bytes,
            "provider_request_count": self.provider_request_count,
            "provider_meter": (
                PROXY_CAMPAIGN_METER if self.proxy_campaign_approval else ""
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
        if self.target_manifest_sha256:
            report.update(
                target_manifest_sha256=self.target_manifest_sha256,
                logical_target_units=self.logical_target_units,
                expected_endpoint_labels=list(self.expected_endpoint_labels),
            )
        return report

    @property
    def dagrun_key(self) -> str:
        if self.proxy_campaign_approval is not None:
            # Continuations and retries intentionally share one in-memory key;
            # the durable campaign ledger supplies the restart boundary.
            return f"whoscored-campaign/{self.proxy_campaign_approval.campaign_id}"
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
_campaign_reserved_bytes: dict[str, int] = defaultdict(int)
_campaign_phase_reserved_bytes: dict[tuple[str, str], int] = defaultdict(int)
_campaign_allocation_reserved_bytes: dict[tuple[str, str], int] = defaultdict(int)


def _lease_has_durable_terminal(lease: Lease) -> bool:
    if (
        not lease.closed
        or not lease.close_recorded
        or lease.active_tunnels
        or lease.reserved_bytes
        or lease.global_budget_escrow_bytes
        or lease.accounting_uncertain
    ):
        return False
    if lease.source == "sofascore" and not lease.allocation_finished:
        return False
    if lease.source == "whoscored" and (
        not lease.proxy_campaign_finished
        or lease.pending_whoscored_bytes
        or lease.settled_whoscored_bytes != lease.total_bytes
        or lease.metering_flush_failed
    ):
        return False
    return True


def _prune_finalized_leases(*, now: float | None = None) -> int:
    """Bound the in-memory idempotency cache after durable terminal evidence."""

    current = _wall_time() if now is None else float(now)
    finalized = []
    for lease in LEASES.values():
        if not _lease_has_durable_terminal(lease):
            continue
        if lease.finalized_at <= 0:
            lease.finalized_at = current
        finalized.append(lease)
    finalized.sort(key=lambda item: (item.finalized_at, item.created_at, item.lease_id))
    overflow = max(0, len(finalized) - MAX_FINALIZED_LEASES)
    expired_ids = {
        lease.lease_id
        for lease in finalized
        if current - lease.finalized_at >= FINALIZED_LEASE_TTL_SECONDS
    }
    expired_ids.update(lease.lease_id for lease in finalized[:overflow])
    removed = 0
    for lease_id in sorted(expired_ids):
        lease = LEASES.get(lease_id)
        if lease is None or not _lease_has_durable_terminal(lease):
            continue
        LEASES.pop(lease_id, None)
        if LEASE_TOKENS.get(lease.token) == lease_id:
            LEASE_TOKENS.pop(lease.token, None)
        removed += 1
    return removed


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


def _proxy_target_host_port(method: str, target: str) -> tuple[str, int]:
    """Parse an HTTP proxy target without treating a malformed port as safe."""
    try:
        if method == "CONNECT":
            parsed = urlsplit(f"//{target}")
            return (parsed.hostname or "", parsed.port or 443)
        parsed = urlsplit(target)
        default_port = 443 if parsed.scheme.lower() == "https" else 80
        return (parsed.hostname or target, parsed.port or default_port)
    except ValueError:
        return "", 0


def _lease_host_allowed(lease: Lease | None, host: str, port: int = 443) -> bool:
    """Enforce exact source host/port scope before a residential dial."""
    normalized = host.lower().rstrip(".")
    if lease is not None and lease.source == "fbref":
        # FBref's browser bootstrap still sends absolute-form HTTP requests;
        # keep that route while rejecting every other port and host.
        return port in {80, 443} and (
            normalized in FBREF_ALLOWED_HOSTS or normalized.endswith(".fbref.com")
        )
    if port != 443:
        return False
    if normalized == SOFASCORE_CANARY_EXIT_PROBE_HOST:
        # Production SofaScore leases need the exit-probe host too. Camoufox's
        # geoip=True resolves the residential exit IP via api.ipify.org at browser
        # startup (camoufox/ip.py:public_ip, tried first and cached per proxy);
        # blocking it aborts the browser with InvalidProxy before any capture.
        # The canary lease already reaches it, so measured hard_task_bytes already
        # carry the probe cost — production matches the measured configuration.
        return lease is not None and lease.source in {
            "fbref",
            "sofascore",
            "sofascore_canary",
        }
    if lease is not None and lease.source in ("sofascore", "sofascore_canary"):
        return (
            normalized == "sofascore.com"
            or normalized.endswith(".sofascore.com")
            or normalized in SOFASCORE_CHALLENGE_HOSTS
        )
    if lease is not None and lease.source == "whoscored":
        # Exact names only: neither arbitrary subdomains nor the apex can be
        # CONNECTed through the paid provider.
        return normalized in WHOSCORED_PROXY_ALLOWED_HOSTS
    if lease is not None and lease.source == "transfermarkt":
        return normalized in TRANSFERMARKT_PROXY_ALLOWED_HOSTS
    # Unknown lease sources never receive an unrestricted paid exit.  A
    # credential-less development listener is handled separately by ``handle``.
    return lease is None


def _control_token_for_source(source: str) -> str:
    if source == "transfermarkt":
        return TRANSFERMARKT_CONTROL_TOKEN
    return CONTROL_TOKEN


def _control_token_valid(headers: dict[str, str], *, source: str = "") -> bool:
    supplied = str(headers.get("x-proxy-control-token") or "")
    expected = _control_token_for_source(source)
    return bool(expected) and secrets.compare_digest(supplied, expected)


def _any_control_token_valid(headers: dict[str, str]) -> bool:
    """Reject unauthenticated control bodies before reading or parsing them."""
    supplied = str(headers.get("x-proxy-control-token") or "")
    return any(
        expected and secrets.compare_digest(supplied, expected)
        for expected in (CONTROL_TOKEN, TRANSFERMARKT_CONTROL_TOKEN)
    )


def _json_object_without_duplicate_fields(
    pairs: list[tuple[str, Any]],
) -> dict[str, Any]:
    result: dict[str, Any] = {}
    for key, value in pairs:
        if key in result:
            raise _DuplicateJsonField
        result[key] = value
    return result


def _pool_error(
    index: int | None, field: str, reason: str
) -> ProxyPoolConfigurationError:
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
        payload = json.loads(
            raw, object_pairs_hook=_json_object_without_duplicate_fields
        )
    except _DuplicateJsonField as exc:
        raise _pool_error(
            None, PROXY_POOL_ENV, "contains a duplicate object field"
        ) from exc
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
        if (
            isinstance(port, bool)
            or not isinstance(port, int)
            or not 1 <= port <= 65535
        ):
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
        if SOURCE_MODE == "whoscored-only" and any(
            record["host"] != WHOSCORED_UPSTREAM_DIAL_HOST for record in records
        ):
            raise ProxyPoolConfigurationError(
                "WhoScored proxy pool must use the pinned provider dial host"
            )
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
        if SOURCE_MODE == "whoscored-only":
            raise ProxyPoolConfigurationError(
                "WhoScored-only mode requires the validated PROXY_POOL_JSON pool"
            )
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
        hasattr(selected, field) for field in ("host", "port", "username", "password")
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


def _whoscored_provider_order_cap_bytes() -> int:
    """Return the immutable order cap, further reduced by a lower daily cap."""

    return min(DAILY_BUDGET_BYTES, WHOSCORED_PROVIDER_ORDER_SAFETY_CAP_BYTES)


def _whoscored_budget_availability(
    ledger: ProxyCampaignLedger,
) -> tuple[int, Mapping[str, int]]:
    """Conjoin signed state, global daily spend and lifetime order exposure."""

    accounting = ledger.provider_order_accounting()
    order_remaining = max(
        0,
        _whoscored_provider_order_cap_bytes()
        - int(accounting["exposure_provider_bytes"]),
    )
    # The proxy WAL/report and campaign ledger are independent witnesses.  A
    # crash between their fsyncs may make either one lead, so the larger proven
    # spend is authoritative; durable whole-lease escrow covers every pending
    # byte which has not yet become campaign spend.
    proven_today = max(
        _daily_total_bytes(),
        int(accounting["current_day_spent_provider_bytes"]),
    )
    daily_remaining = max(
        0,
        DAILY_BUDGET_BYTES
        - proven_today
        - int(accounting["current_day_reserved_provider_bytes"]),
    )
    return min(order_remaining, daily_remaining), accounting


def _assert_whoscored_provider_order_bound(
    ledger: ProxyCampaignLedger,
) -> None:
    """Re-read the durable escrow after mutation and reject any cap drift."""

    _, accounting = _whoscored_budget_availability(ledger)
    if int(accounting["exposure_provider_bytes"]) > (
        _whoscored_provider_order_cap_bytes()
    ):
        raise RuntimeError("WhoScored provider-order lifetime cap was exceeded")
    proven_today = max(
        _daily_total_bytes(),
        int(accounting["current_day_spent_provider_bytes"]),
    )
    if proven_today + int(accounting["current_day_reserved_provider_bytes"]) > (
        DAILY_BUDGET_BYTES
    ):
        raise RuntimeError("WhoScored global daily budget was exceeded")


def _release_whoscored_global_escrow(lease: Lease, released: int) -> None:
    """Clear the local mirror only after the durable escrow was released."""

    if (
        lease.source != "whoscored"
        or isinstance(released, bool)
        or not isinstance(released, int)
        or released < 0
        or released != lease.global_budget_escrow_bytes
    ):
        raise RuntimeError("WhoScored global escrow release differs from ledger")
    lease.global_budget_escrow_bytes = 0


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
    if lease.source == "whoscored":
        if lease.proxy_campaign_approval is None:
            return DEFAULT_WHOSCORED_PAID_CAP_BYTES
        return lease.proxy_campaign_approval.caps.total_provider_bytes
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
        "whoscored",
    ):
        if lease.source == "whoscored" and lease.proxy_work_allocation is not None:
            return lease.proxy_work_allocation.budget_bytes
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


def _whoscored_state_secret() -> bytes:
    secret = WHOSCORED_PROXY_LEDGER_HMAC_SECRET.encode("utf-8")
    if len(secret) < 32:
        raise RuntimeError("WhoScored state has no ledger HMAC secret")
    return secret


def _state_path_sha256(path: str) -> str:
    return hashlib.sha256(os.path.abspath(path).encode("utf-8")).hexdigest()


def _paid_ledger_checkpoint_path() -> str:
    return LEDGER_PATH + ".checkpoint.json"


def _atomic_private_bytes(path: str, payload: bytes, *, replace: bool) -> None:
    directory_path = os.path.dirname(path) or "."
    os.makedirs(directory_path, exist_ok=True)
    if (
        os.path.islink(path)
        or (not replace and os.path.exists(path))
        or (replace and not os.path.isfile(path))
    ):
        raise RuntimeError("refusing to replace protected WhoScored state")
    temporary = os.path.join(
        directory_path,
        f".{os.path.basename(path)}.{os.getpid()}.{secrets.token_hex(8)}.tmp",
    )
    flags = os.O_CREAT | os.O_EXCL | os.O_WRONLY | getattr(os, "O_NOFOLLOW", 0)
    descriptor = os.open(temporary, flags, 0o600)
    try:
        os.fchmod(descriptor, 0o600)
        pending = memoryview(payload)
        while pending:
            written = os.write(descriptor, pending)
            if written <= 0:
                raise OSError("protected state write made no progress")
            pending = pending[written:]
        os.fsync(descriptor)
    finally:
        os.close(descriptor)
    try:
        if not replace and os.path.exists(path):
            raise RuntimeError("protected WhoScored state appeared concurrently")
        os.replace(temporary, path)
        os.chmod(path, 0o600)
        directory = os.open(directory_path, os.O_RDONLY | os.O_DIRECTORY)
        try:
            os.fsync(directory)
        finally:
            os.close(directory)
    finally:
        try:
            os.unlink(temporary)
        except FileNotFoundError:
            pass


def _require_private_regular_file(
    path: str,
    *,
    allow_empty: bool,
    max_bytes: int | None = MAX_CONTROL_BODY_BYTES * 4,
    read: bool = True,
) -> bytes:
    if os.path.islink(path):
        raise RuntimeError("protected WhoScored state cannot be a symlink")
    try:
        metadata = os.stat(path)
    except OSError as exc:
        raise RuntimeError("protected WhoScored state is missing") from exc
    if (
        not stat.S_ISREG(metadata.st_mode)
        or stat.S_IMODE(metadata.st_mode) != 0o600
        or (not allow_empty and metadata.st_size <= 0)
        or (max_bytes is not None and metadata.st_size > max_bytes)
    ):
        raise RuntimeError("protected WhoScored state file is invalid")
    if not read:
        return b""
    with open(path, "rb") as stream:
        return stream.read()


def _state_hmac(body: Mapping[str, object]) -> str:
    return hmac.new(
        _whoscored_state_secret(),
        canonical_json_bytes(dict(body)),
        hashlib.sha256,
    ).hexdigest()


def _paid_ledger_seed(state_id: str) -> str:
    return hmac.new(
        _whoscored_state_secret(),
        canonical_json_bytes(
            {
                "schema_version": WHOSCORED_PAID_LEDGER_CHAIN_SCHEMA_VERSION,
                "state_id": state_id,
                "kind": "whoscored_paid_ledger_seed",
            }
        ),
        hashlib.sha256,
    ).hexdigest()


def _checkpoint_body(
    *, state_id: str, event_count: int, byte_length: int, tail_hmac: str
) -> dict[str, object]:
    return {
        "schema_version": WHOSCORED_STATE_SCHEMA_VERSION,
        "state_id": state_id,
        "event_count": event_count,
        "byte_length": byte_length,
        "tail_hmac": tail_hmac,
    }


def _write_paid_ledger_checkpoint(
    *, event_count: int, byte_length: int, tail_hmac: str, replace: bool = True
) -> None:
    body = _checkpoint_body(
        state_id=WHOSCORED_STATE_ID,
        event_count=event_count,
        byte_length=byte_length,
        tail_hmac=tail_hmac,
    )
    document = {**body, "signature": _state_hmac(body)}
    _atomic_private_bytes(
        _paid_ledger_checkpoint_path(),
        canonical_json_bytes(document) + b"\n",
        replace=replace,
    )


def _read_paid_ledger_checkpoint() -> Mapping[str, object]:
    raw = _require_private_regular_file(
        _paid_ledger_checkpoint_path(), allow_empty=False
    )
    try:
        value = strict_json_loads(raw)
    except (json.JSONDecodeError, UnicodeDecodeError, ProxyCampaignError) as exc:
        raise RuntimeError("paid ledger checkpoint is corrupt") from exc
    fields = {
        "schema_version",
        "state_id",
        "event_count",
        "byte_length",
        "tail_hmac",
        "signature",
    }
    if not isinstance(value, Mapping) or frozenset(value) != fields:
        raise RuntimeError("paid ledger checkpoint fields are invalid")
    body = {name: value[name] for name in fields if name != "signature"}
    count = body.get("event_count")
    offset = body.get("byte_length")
    tail = body.get("tail_hmac")
    if (
        body.get("schema_version") != WHOSCORED_STATE_SCHEMA_VERSION
        or body.get("state_id") != WHOSCORED_STATE_ID
        or isinstance(count, bool)
        or not isinstance(count, int)
        or count < 0
        or isinstance(offset, bool)
        or not isinstance(offset, int)
        or offset < 0
        or not isinstance(tail, str)
        or re.fullmatch(r"[0-9a-f]{64}", tail) is None
        or not hmac.compare_digest(str(value.get("signature")), _state_hmac(body))
    ):
        raise RuntimeError("paid ledger checkpoint authentication failed")
    return body


def _signed_report(report: Mapping[str, object]) -> dict[str, object]:
    body = {
        **dict(report),
        "state_schema_version": WHOSCORED_STATE_SCHEMA_VERSION,
        "state_id": WHOSCORED_STATE_ID,
    }
    return {**body, "report_hmac": _state_hmac(body)}


def _verify_signed_report(path: str) -> Mapping[str, object]:
    raw = _require_private_regular_file(path, allow_empty=False)
    try:
        value = strict_json_loads(raw)
    except (json.JSONDecodeError, UnicodeDecodeError, ProxyCampaignError) as exc:
        raise RuntimeError("WhoScored byte report is corrupt") from exc
    if not isinstance(value, Mapping) or set(value) != {
        "total_mb",
        "daily",
        "leases",
        "dagruns",
        "allowed_hosts",
        "blocked_hosts",
        "state_schema_version",
        "state_id",
        "report_hmac",
    }:
        raise RuntimeError("WhoScored byte report fields are invalid")
    body = {name: item for name, item in value.items() if name != "report_hmac"}
    if (
        body.get("state_schema_version") != WHOSCORED_STATE_SCHEMA_VERSION
        or body.get("state_id") != WHOSCORED_STATE_ID
        or not hmac.compare_digest(str(value.get("report_hmac")), _state_hmac(body))
    ):
        raise RuntimeError("WhoScored byte report authentication failed")
    daily = body.get("daily")
    if not isinstance(daily, Mapping) or set(daily) != {
        "day",
        "up_bytes",
        "down_bytes",
        "total_bytes",
        "budget_bytes",
    }:
        raise RuntimeError("WhoScored byte report daily state is invalid")
    for name in ("up_bytes", "down_bytes", "total_bytes", "budget_bytes"):
        counter = daily.get(name)
        if isinstance(counter, bool) or not isinstance(counter, int) or counter < 0:
            raise RuntimeError("WhoScored byte report counter is invalid")
    if daily["total_bytes"] != daily["up_bytes"] + daily["down_bytes"]:
        raise RuntimeError("WhoScored byte report counters differ")
    return body


def _whoscored_state_binding() -> tuple[str, str]:
    """Return the exact provider namespace configured by protected startup."""

    order_id = WHOSCORED_PROVIDER_ORDER_ID
    policy_sha256 = WHOSCORED_PROVIDER_POLICY_SHA256
    if (
        type(order_id) is not str
        or _CANONICAL_TOKEN_RE.fullmatch(order_id) is None
        or type(policy_sha256) is not str
        or _LOWER_SHA256_RE.fullmatch(policy_sha256) is None
    ):
        raise RuntimeError("WhoScored provider state binding is invalid")
    return order_id, policy_sha256


def _assert_whoscored_approval_state_binding(
    approval: ProxyCampaignApproval,
) -> None:
    """Never let scheduled-v3 authority use another order's counters."""

    if WHOSCORED_LEGACY_STATE_MARKER_LOADED:
        if approval.schema_version == SCHEDULED_PROXY_CAMPAIGN_SCHEMA_VERSION:
            raise ProxyCampaignValidationError(
                "scheduled WhoScored authority requires a provider-bound state marker"
            )
        return
    order_id, policy_sha256 = _whoscored_state_binding()
    if approval.schema_version != SCHEDULED_PROXY_CAMPAIGN_SCHEMA_VERSION:
        return
    authority = approval.scheduled_authority
    if (
        authority is None
        or not hmac.compare_digest(authority.order_id, order_id)
        or not hmac.compare_digest(
            authority.provider_policy_sha256, policy_sha256
        )
    ):
        raise ProxyCampaignValidationError(
            "scheduled WhoScored authority differs from provider state binding"
        )


def _initialize_whoscored_state(out_path: str) -> None:
    global WHOSCORED_STATE_ID, WHOSCORED_LEGACY_STATE_MARKER_LOADED
    order_id, policy_sha256 = _whoscored_state_binding()
    checkpoint_path = _paid_ledger_checkpoint_path()
    paths = (
        out_path,
        LEDGER_PATH,
        WHOSCORED_CAMPAIGN_LEDGER_PATH,
        checkpoint_path,
        WHOSCORED_STATE_MARKER_PATH,
    )
    if any(os.path.exists(path) or os.path.islink(path) for path in paths):
        raise RuntimeError("WhoScored state initialization requires an empty state")
    WHOSCORED_STATE_ID = secrets.token_hex(32)
    WHOSCORED_LEGACY_STATE_MARKER_LOADED = False
    empty_report = _signed_report(
        {
            "total_mb": 0.0,
            "daily": {
                "day": _utc_day(),
                "up_bytes": 0,
                "down_bytes": 0,
                "total_bytes": 0,
                "budget_bytes": DAILY_BUDGET_BYTES,
            },
            "leases": [],
            "dagruns": [],
            "allowed_hosts": [],
            "blocked_hosts": [],
        }
    )
    _atomic_private_bytes(LEDGER_PATH, b"", replace=False)
    _whoscored_campaign_ledger().initialize_empty()
    _write_paid_ledger_checkpoint(
        event_count=0,
        byte_length=0,
        tail_hmac=_paid_ledger_seed(WHOSCORED_STATE_ID),
        replace=False,
    )
    _atomic_private_bytes(
        out_path, canonical_json_bytes(empty_report) + b"\n", replace=False
    )
    marker_body = {
        "schema_version": WHOSCORED_STATE_MARKER_SCHEMA_VERSION,
        "state_id": WHOSCORED_STATE_ID,
        "order_id": order_id,
        "provider_policy_sha256": policy_sha256,
        "created_at": datetime.now(timezone.utc).isoformat(),
        "path_sha256": {
            "byte_report": _state_path_sha256(out_path),
            "paid_ledger": _state_path_sha256(LEDGER_PATH),
            "campaign_ledger": _state_path_sha256(WHOSCORED_CAMPAIGN_LEDGER_PATH),
            "paid_ledger_checkpoint": _state_path_sha256(checkpoint_path),
        },
    }
    marker = {**marker_body, "signature": _state_hmac(marker_body)}
    _atomic_private_bytes(
        WHOSCORED_STATE_MARKER_PATH,
        canonical_json_bytes(marker) + b"\n",
        replace=False,
    )


def _load_whoscored_state_marker(
    out_path: str, *, allow_legacy_marker: bool = False
) -> None:
    global WHOSCORED_STATE_ID, WHOSCORED_LEGACY_STATE_MARKER_LOADED
    raw = _require_private_regular_file(WHOSCORED_STATE_MARKER_PATH, allow_empty=False)
    try:
        value = strict_json_loads(raw)
    except (json.JSONDecodeError, UnicodeDecodeError, ProxyCampaignError) as exc:
        raise RuntimeError("WhoScored state marker is corrupt") from exc
    if not isinstance(value, Mapping):
        raise RuntimeError("WhoScored state marker fields are invalid")
    schema_version = value.get("schema_version")
    common_fields = {
        "schema_version",
        "state_id",
        "created_at",
        "path_sha256",
        "signature",
    }
    current_fields = common_fields | {"order_id", "provider_policy_sha256"}
    legacy = schema_version == WHOSCORED_STATE_SCHEMA_VERSION
    if (
        (schema_version == WHOSCORED_STATE_MARKER_SCHEMA_VERSION and frozenset(value) != current_fields)
        or (legacy and (not allow_legacy_marker or frozenset(value) != common_fields))
        or schema_version
        not in {WHOSCORED_STATE_SCHEMA_VERSION, WHOSCORED_STATE_MARKER_SCHEMA_VERSION}
    ):
        raise RuntimeError("WhoScored state marker fields are invalid")
    body = {name: item for name, item in value.items() if name != "signature"}
    state_id = body.get("state_id")
    expected_paths = {
        "byte_report": _state_path_sha256(out_path),
        "paid_ledger": _state_path_sha256(LEDGER_PATH),
        "campaign_ledger": _state_path_sha256(WHOSCORED_CAMPAIGN_LEDGER_PATH),
        "paid_ledger_checkpoint": _state_path_sha256(_paid_ledger_checkpoint_path()),
    }
    if (
        not isinstance(state_id, str)
        or re.fullmatch(r"[0-9a-f]{64}", state_id) is None
        or body.get("path_sha256") != expected_paths
        or not hmac.compare_digest(str(value.get("signature")), _state_hmac(body))
    ):
        raise RuntimeError("WhoScored state marker authentication failed")
    if not legacy:
        order_id, policy_sha256 = _whoscored_state_binding()
        if (
            not hmac.compare_digest(str(body.get("order_id")), order_id)
            or not hmac.compare_digest(
                str(body.get("provider_policy_sha256")), policy_sha256
            )
        ):
            raise RuntimeError("WhoScored state marker provider binding differs")
    WHOSCORED_STATE_ID = state_id
    WHOSCORED_LEGACY_STATE_MARKER_LOADED = legacy


def _verify_paid_ledger_chain(path: str) -> None:
    global _PAID_LEDGER_CHAIN_COUNT
    global _PAID_LEDGER_CHAIN_OFFSET
    global _PAID_LEDGER_CHAIN_TAIL
    _require_private_regular_file(
        path,
        allow_empty=True,
        max_bytes=None,
        read=False,
    )
    checkpoint = _read_paid_ledger_checkpoint()
    checkpoint_count = int(checkpoint["event_count"])
    checkpoint_offset = int(checkpoint["byte_length"])
    checkpoint_tail = str(checkpoint["tail_hmac"])
    count = 0
    offset = 0
    tail = _paid_ledger_seed(WHOSCORED_STATE_ID)
    checkpoint_observed = checkpoint_count == 0 and checkpoint_offset == 0
    with open(path, "rb") as stream:
        while True:
            raw = stream.readline(MAX_LEDGER_EVENT_BYTES + 1)
            if not raw:
                break
            if len(raw) > MAX_LEDGER_EVENT_BYTES or not raw.endswith(b"\n"):
                raise RuntimeError("paid byte ledger chain record is truncated")
            try:
                event = strict_json_loads(raw)
            except (
                json.JSONDecodeError,
                UnicodeDecodeError,
                ProxyCampaignError,
            ) as exc:
                raise RuntimeError("paid byte ledger chain is corrupt") from exc
            if not isinstance(event, Mapping):
                raise RuntimeError("paid byte ledger chain record is invalid")
            body = dict(event)
            supplied_hmac = body.pop("event_hmac", None)
            count += 1
            if (
                body.get("chain_schema_version")
                != WHOSCORED_PAID_LEDGER_CHAIN_SCHEMA_VERSION
                or body.get("chain_sequence") != count
                or body.get("previous_event_hmac") != tail
                or not isinstance(supplied_hmac, str)
            ):
                raise RuntimeError("paid byte ledger chain identity differs")
            expected_hmac = hmac.new(
                _whoscored_state_secret(),
                canonical_json_bytes(body),
                hashlib.sha256,
            ).hexdigest()
            if not hmac.compare_digest(supplied_hmac, expected_hmac):
                raise RuntimeError("paid byte ledger chain authentication failed")
            tail = expected_hmac
            offset += len(raw)
            if count == checkpoint_count:
                checkpoint_observed = (
                    offset == checkpoint_offset and tail == checkpoint_tail
                )
    if count < checkpoint_count or not checkpoint_observed:
        raise RuntimeError("paid byte ledger was truncated or differs from checkpoint")
    if count == checkpoint_count and (
        offset != checkpoint_offset or tail != checkpoint_tail
    ):
        raise RuntimeError("paid byte ledger tail differs from checkpoint")
    if count > checkpoint_count:
        _write_paid_ledger_checkpoint(
            event_count=count,
            byte_length=offset,
            tail_hmac=tail,
        )
    _PAID_LEDGER_CHAIN_COUNT = count
    _PAID_LEDGER_CHAIN_OFFSET = offset
    _PAID_LEDGER_CHAIN_TAIL = tail


def _verify_whoscored_state(
    out_path: str, *, allow_legacy_marker: bool = False
) -> None:
    _load_whoscored_state_marker(
        out_path, allow_legacy_marker=allow_legacy_marker
    )
    _verify_signed_report(out_path)
    _verify_paid_ledger_chain(LEDGER_PATH)
    _whoscored_campaign_ledger().verify_integrity()


def _append_budget_event(event_type: str, lease: Lease, **values: Any) -> None:
    """Append and fsync exact paid accounting before another lease is served."""
    global _PAID_LEDGER_CHAIN_COUNT
    global _PAID_LEDGER_CHAIN_OFFSET
    global _PAID_LEDGER_CHAIN_TAIL
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
        "proxy_campaign_id": (
            lease.proxy_campaign_approval.campaign_id
            if lease.proxy_campaign_approval
            else ""
        ),
        "proxy_approval_id": (
            lease.proxy_campaign_approval.approval_id
            if lease.proxy_campaign_approval
            else ""
        ),
        "proxy_approval_sha256": (
            lease.proxy_campaign_approval.approval_sha256
            if lease.proxy_campaign_approval
            else ""
        ),
        "proxy_attempt_id": lease.proxy_attempt_id,
        "proxy_work_item_id": lease.proxy_work_item_id,
        "provider_meter": (
            PROXY_CAMPAIGN_METER if lease.proxy_campaign_approval else ""
        ),
        "base_run_id": lease.base_run_id,
        "workload_phase": lease.workload_phase,
        "parent_run_cap_bytes": lease.parent_run_cap_bytes,
        "parent_run_spent_provider_bytes": (lease.parent_run_spent_provider_bytes),
        **values,
    }
    if SOURCE_MODE == "whoscored-only":
        if not WHOSCORED_STATE_ID or not _PAID_LEDGER_CHAIN_TAIL:
            raise RuntimeError("WhoScored paid ledger chain is not initialized")
        chain_body = {
            **event,
            "chain_schema_version": WHOSCORED_PAID_LEDGER_CHAIN_SCHEMA_VERSION,
            "chain_sequence": _PAID_LEDGER_CHAIN_COUNT + 1,
            "previous_event_hmac": _PAID_LEDGER_CHAIN_TAIL,
        }
        event = {
            **chain_body,
            "event_hmac": hmac.new(
                _whoscored_state_secret(),
                canonical_json_bytes(chain_body),
                hashlib.sha256,
            ).hexdigest(),
        }
    os.makedirs(os.path.dirname(LEDGER_PATH) or ".", exist_ok=True)
    payload = (json.dumps(event, sort_keys=True, separators=(",", ":")) + "\n").encode(
        "utf-8"
    )
    if len(payload) > MAX_LEDGER_EVENT_BYTES:
        raise RuntimeError(f"paid byte event exceeds {MAX_LEDGER_EVENT_BYTES} bytes")
    flags = os.O_APPEND | os.O_WRONLY | getattr(os, "O_NOFOLLOW", 0)
    if SOURCE_MODE != "whoscored-only":
        flags |= os.O_CREAT
    descriptor = os.open(LEDGER_PATH, flags, 0o600)
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
    if SOURCE_MODE == "whoscored-only":
        _PAID_LEDGER_CHAIN_COUNT += 1
        _PAID_LEDGER_CHAIN_OFFSET += len(payload)
        _PAID_LEDGER_CHAIN_TAIL = str(event["event_hmac"])
        _write_paid_ledger_checkpoint(
            event_count=_PAID_LEDGER_CHAIN_COUNT,
            byte_length=_PAID_LEDGER_CHAIN_OFFSET,
            tail_hmac=_PAID_LEDGER_CHAIN_TAIL,
        )


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
    if SOURCE_MODE == "whoscored-only":
        daily = dict(_verify_signed_report(out_path).get("daily") or {})
    else:
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


def _whoscored_campaign_ledger() -> ProxyCampaignLedger:
    """Return the durable WhoScored ledger bound only to its ledger key."""

    global WHOSCORED_CAMPAIGN_LEDGER, _WHOSCORED_CAMPAIGN_LEDGER_KEY
    if len(WHOSCORED_PROXY_LEDGER_HMAC_SECRET.encode("utf-8")) < 32:
        raise RuntimeError("WhoScored campaign ledger has no HMAC secret")
    key = (
        WHOSCORED_CAMPAIGN_LEDGER_PATH,
        hashlib.sha256(WHOSCORED_PROXY_LEDGER_HMAC_SECRET.encode("utf-8")).hexdigest(),
        hashlib.sha256(
            WHOSCORED_PROXY_APPROVAL_HMAC_SECRET.encode("utf-8")
        ).hexdigest(),
        SOURCE_MODE == "whoscored-only",
    )
    if WHOSCORED_CAMPAIGN_LEDGER is None or key != _WHOSCORED_CAMPAIGN_LEDGER_KEY:
        WHOSCORED_CAMPAIGN_LEDGER = ProxyCampaignLedger(
            WHOSCORED_CAMPAIGN_LEDGER_PATH,
            secret=WHOSCORED_PROXY_LEDGER_HMAC_SECRET,
            approval_secret=WHOSCORED_PROXY_APPROVAL_HMAC_SECRET,
            require_existing=SOURCE_MODE == "whoscored-only",
        )
        _WHOSCORED_CAMPAIGN_LEDGER_KEY = key
    return WHOSCORED_CAMPAIGN_LEDGER


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
    if require_context and not inferred_source:
        raise ValueError("paid lease dag_id is not in the closed source allowlist")
    if requested_source and requested_source != inferred_source:
        raise ValueError("paid lease source does not match dag_id")
    source = inferred_source or requested_source
    if source == "whoscored" and SOURCE_MODE == "shared-no-whoscored":
        raise ProxyCampaignValidationError(
            "WhoScored leases require the dedicated provider service"
        )
    if source != "whoscored" and SOURCE_MODE == "whoscored-only":
        raise ValueError("dedicated WhoScored service rejects every other source")
    proxy_campaign_approval: ProxyCampaignApproval | None = None
    proxy_work_allocation: ProxyWorkAllocation | None = None
    proxy_attempt_id = ""
    proxy_campaign_ledger: ProxyCampaignLedger | None = None
    whoscored_global_available = 0
    if source == "whoscored":
        (
            proxy_campaign_approval,
            proxy_work_allocation,
            proxy_attempt_id,
        ) = approval_from_context(metadata, secret=WHOSCORED_PROXY_APPROVAL_HMAC_SECRET)
        _assert_whoscored_approval_state_binding(proxy_campaign_approval)
        if proxy_campaign_approval.allowed_dag_ids == (WHOSCORED_CANARY_DAG_ID,):
            if not proxy_campaign_approval.is_exact_canary:
                raise ProxyCampaignValidationError(
                    "WhoScored canary approval must match the exact 1 GB contract"
                )
        elif not (
            (
                proxy_campaign_approval.allowed_dag_ids
                and all(
                    daily_ingest_paid_crawl_allowed(dag_id)
                    for dag_id in proxy_campaign_approval.allowed_dag_ids
                )
            )
            or WHOSCORED_FULL_PAID_CRAWL_AVAILABLE
        ):
            raise ProxyCampaignValidationError(
                "WhoScored full paid crawl is disabled pending exact reconciliation"
            )
        if not WHOSCORED_PROVIDER_INVOICE_HARD_CAP_AVAILABLE:
            raise ProxyCampaignValidationError(
                "WhoScored paid traffic has no provider-side invoice hard cap"
            )
        if not WHOSCORED_PAID_APPLICATION_GATEWAY_AVAILABLE:
            raise ProxyCampaignValidationError(
                "WhoScored paid traffic has no authenticated isolated "
                "application gateway"
            )
        if not WHOSCORED_PROXY_RUNTIME_SHA256:
            raise ProxyCampaignValidationError(
                "WhoScored proxy startup runtime fingerprint is unavailable"
            )
        if proxy_campaign_approval.runtime_sha256 != WHOSCORED_PROXY_RUNTIME_SHA256:
            raise ProxyCampaignValidationError(
                "WhoScored approval runtime differs from the loaded proxy release"
            )
        proxy_campaign_ledger = _whoscored_campaign_ledger()
        whoscored_global_available, _ = _whoscored_budget_availability(
            proxy_campaign_ledger
        )
        if whoscored_global_available <= 0:
            raise RuntimeError("WhoScored global daily/provider-order budget exhausted")
    elif _daily_total_bytes() >= DAILY_BUDGET_BYTES:
        raise RuntimeError("daily paid-proxy budget exhausted")
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
    if source == "transfermarkt" and TRANSFERMARKT_DAGRUN_BUDGET_BYTES <= 0:
        raise RuntimeError(
            "Transfermarkt paid-proxy budget unavailable: explicit source-scoped "
            "authorization required"
        )
    active_leases = [
        item
        for item in LEASES.values()
        if (
            (not item.closed and not item.expired)
            or item.active_tunnels > 0
            or item.reserved_bytes > 0
            or item.global_budget_escrow_bytes > 0
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
    # Keep collision risk negligible even at the signed 100k-lease ceiling.
    # Completed IDs are forensic join keys and must not be silently reusable.
    lease_id = uuid_hex(32)
    run_id = str(metadata.get("run_id") or "").strip()
    run_key = f"{dag_id}/{run_id}" if dag_id and run_id else f"standalone/{lease_id}"
    canonical_url = _canonical_url(metadata.get("canonical_url"))
    workload_plan: SignedDagRunPlan | None = None
    allocation: WorkloadAllocation | None = None
    allocation_claim: AllocationClaim | None = None
    proxy_campaign_claim: ProxyCampaignClaim | None = None
    proxy_campaign_escrowed = False
    parent_envelope: ParentRunEnvelope | None = None
    if source == "sofascore":
        workload_plan, allocation = _signed_allocation_from_request(
            metadata,
            max_bytes=max_bytes,
        )
        parent_envelope = _parent_envelope_ledger().register(workload_plan)
        dagrun_budget = workload_plan.run_cap_bytes
    elif source == "whoscored":
        assert proxy_campaign_approval is not None
        assert proxy_work_allocation is not None
        approval_expires_at = datetime.fromisoformat(
            proxy_campaign_approval.expires_at.replace("Z", "+00:00")
        ).timestamp()
        effective_expires_at = min(now + ttl_seconds, approval_expires_at)
        assert proxy_campaign_ledger is not None
        proxy_campaign_claim = proxy_campaign_ledger.claim(
            proxy_campaign_approval,
            proxy_work_allocation.allocation_id,
            dag_id=dag_id,
            run_id=run_id,
            task_id=str(metadata.get("task_id") or "").strip(),
            attempt_id=proxy_attempt_id,
            lease_id=lease_id,
            expires_at=datetime.fromtimestamp(effective_expires_at, timezone.utc),
            canonical_url=canonical_url,
            target_manifest_sha256=metadata.get("target_manifest_sha256"),
            logical_target_units=metadata.get("logical_target_units", 1),
            expected_endpoint_labels=metadata.get("expected_endpoint_labels", ()),
        )
        dagrun_budget = proxy_campaign_approval.caps.total_provider_bytes
    else:
        effective_expires_at = now + ttl_seconds
        dagrun_budget = _dagrun_budget_bytes(dag_id)
    if source != "whoscored":
        effective_expires_at = now + ttl_seconds
    url_budget = (
        dagrun_budget
        if source
        in (
            "fbref",
            "sofascore",
            "sofascore_canary",
            "sofascore_discovery",
            "whoscored",
        )
        else URL_BUDGET_BYTES
    )
    if proxy_campaign_claim is not None:
        available = min(
            proxy_campaign_claim.remaining_provider_bytes,
            whoscored_global_available,
        )
    else:
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
            expires_at=effective_expires_at,
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
            proxy_campaign_approval=proxy_campaign_approval,
            proxy_campaign_claim=proxy_campaign_claim,
            proxy_work_allocation=proxy_work_allocation,
            proxy_attempt_id=proxy_attempt_id,
            proxy_work_item_id=(
                proxy_work_allocation.work_item_id
                if proxy_work_allocation is not None
                else ""
            ),
            allocation_id=(
                allocation.allocation_id
                if allocation
                else (
                    proxy_work_allocation.allocation_id if proxy_work_allocation else ""
                )
            ),
            workload_class=(
                allocation.workload_class
                if allocation
                else (
                    proxy_work_allocation.workload_class
                    if proxy_work_allocation
                    else ""
                )
            ),
            allocation_batch_index=allocation.batch_index if allocation else -1,
            allocation_units=allocation.units if allocation else (),
            allocation_budget_bytes=(
                allocation.budget_bytes
                if allocation
                else (
                    proxy_work_allocation.budget_bytes if proxy_work_allocation else 0
                )
            ),
            run_cap_bytes=(
                workload_plan.run_cap_bytes
                if workload_plan
                else (
                    proxy_campaign_approval.caps.total_provider_bytes
                    if proxy_campaign_approval
                    else 0
                )
            ),
            base_run_id=(parent_envelope.base_run_id if parent_envelope else ""),
            workload_phase=(parent_envelope.phase if parent_envelope else ""),
            parent_run_cap_bytes=(
                parent_envelope.parent_cap_bytes if parent_envelope else 0
            ),
            parent_run_spent_provider_bytes=(
                parent_envelope.parent_spent_provider_bytes if parent_envelope else 0
            ),
            target_manifest_sha256=(
                proxy_campaign_claim.target_manifest_sha256
                if proxy_campaign_claim is not None
                else ""
            ),
            logical_target_units=(
                proxy_campaign_claim.logical_target_units
                if proxy_campaign_claim is not None
                else 1
            ),
            expected_endpoint_labels=(
                proxy_campaign_claim.expected_endpoint_labels
                if proxy_campaign_claim is not None
                else ()
            ),
        )
        if proxy_campaign_approval is not None and proxy_campaign_claim is not None:
            assert proxy_campaign_ledger is not None
            proxy_campaign_ledger.reserve_provider_bytes(
                proxy_campaign_approval,
                proxy_campaign_claim,
                lease.max_bytes,
                provider_order_cap_bytes=_whoscored_provider_order_cap_bytes(),
                global_daily_cap_bytes=DAILY_BUDGET_BYTES,
            )
            proxy_campaign_escrowed = True
            lease.global_budget_escrow_bytes = lease.max_bytes
            _assert_whoscored_provider_order_bound(proxy_campaign_ledger)
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
        if proxy_campaign_approval is not None and proxy_campaign_claim is not None:
            assert proxy_campaign_ledger is not None
            if proxy_campaign_escrowed:
                released = proxy_campaign_ledger.release_provider_reservation(
                    proxy_campaign_approval,
                    proxy_campaign_claim,
                )
                _release_whoscored_global_escrow(lease, released)
            proxy_campaign_ledger.finish(
                proxy_campaign_approval,
                proxy_campaign_claim,
                provider_billed_bytes=0,
                completed=False,
            )
        raise
    LEASES[lease.lease_id] = lease
    LEASE_TOKENS[lease.token] = lease.lease_id
    try:
        lease_created_fields: dict[str, object] = {"max_bytes": lease.max_bytes}
        if lease.target_manifest_sha256:
            lease_created_fields.update(
                target_manifest_sha256=lease.target_manifest_sha256,
                logical_target_units=lease.logical_target_units,
                expected_endpoint_labels=list(lease.expected_endpoint_labels),
            )
        _append_budget_event("lease_created", lease, **lease_created_fields)
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
        if proxy_campaign_approval is not None and proxy_campaign_claim is not None:
            assert proxy_campaign_ledger is not None
            if proxy_campaign_escrowed:
                released = proxy_campaign_ledger.release_provider_reservation(
                    proxy_campaign_approval,
                    proxy_campaign_claim,
                )
                _release_whoscored_global_escrow(lease, released)
            proxy_campaign_ledger.finish(
                proxy_campaign_approval,
                proxy_campaign_claim,
                provider_billed_bytes=0,
                completed=False,
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


def _fbref_lease_extension_ceiling(lease: Lease) -> int:
    """Largest safe absolute cap for one drained FBref lease.

    Shared counters already contain this lease's spend.  Add that spend back to
    the remaining shared allowance because ``max_bytes`` is an absolute cap for
    this lease, not a delta.  Reservations from every lease stay subtracted.
    """

    _refresh_daily_counter()
    run_key = lease.dagrun_key
    url_key = (run_key, lease.canonical_url)
    daily_remaining = max(
        0,
        DAILY_BUDGET_BYTES - _daily_total_bytes() - _daily_reserved_bytes,
    )
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
    return min(
        MAX_LEASE_BYTES,
        lease.total_bytes + min(daily_remaining, run_remaining, url_remaining),
    )


def _extend_fbref_lease(lease: Lease, new_max_bytes: int) -> dict[str, Any]:
    """Durably raise one idle FBref lease cap without changing its identity."""

    if isinstance(new_max_bytes, bool) or not isinstance(new_max_bytes, int):
        raise ValueError("max_bytes must be an integer")
    if lease.source != "fbref":
        raise RuntimeError("only FBref leases may be extended")
    if lease.closed or lease.expired or lease.budget_exceeded:
        raise RuntimeError("FBref lease is not open and usable")
    if (
        lease.active_tunnels != 0
        or lease.reserved_bytes != 0
        or bool(lease.current_request_id)
        or bool(lease.current_endpoint)
    ):
        raise RuntimeError("FBref lease still has active provider work")
    if new_max_bytes <= lease.max_bytes:
        raise ValueError("max_bytes must increase the current lease cap")
    if new_max_bytes > MAX_LEASE_BYTES:
        raise ValueError(f"max_bytes must not exceed {MAX_LEASE_BYTES}")
    ceiling = _fbref_lease_extension_ceiling(lease)
    if new_max_bytes > ceiling:
        raise RuntimeError("FBref lease extension exceeds remaining shared budget")

    previous_max_bytes = lease.max_bytes
    # The event is the commit point.  A write/fsync failure leaves the live
    # data-plane cap unchanged and no success response is emitted.
    _append_budget_event(
        "lease_extended",
        lease,
        previous_max_bytes=previous_max_bytes,
        max_bytes=new_max_bytes,
        lease_total_bytes=lease.total_bytes,
    )
    lease.max_bytes = new_max_bytes
    return _control_report(lease)


def _lease_remaining(lease: Lease) -> int:
    if not lease.usable:
        return 0
    if (
        lease.source == "whoscored"
        and lease.proxy_campaign_approval is not None
        and lease.proxy_campaign_claim is not None
        and lease.proxy_work_allocation is not None
    ):
        # ``_create_lease`` fsyncs one escrow equal to ``lease.max_bytes`` only
        # after taking the minimum of allocation/phase/day/campaign and the
        # lifetime provider-order cap across *all* approvals.  The local mirror
        # is reduced for every observed provider byte.  Socket concurrency is
        # separately serialized by ``lease.reserved_bytes``.
        return min(
            max(0, lease.max_bytes - lease.total_bytes - lease.reserved_bytes),
            max(0, lease.global_budget_escrow_bytes - lease.reserved_bytes),
        )
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
    if (
        count
        and lease.proxy_campaign_approval is not None
        and lease.proxy_work_allocation is not None
    ):
        campaign_id = lease.proxy_campaign_approval.campaign_id
        _campaign_reserved_bytes[campaign_id] += count
        _campaign_phase_reserved_bytes[
            (campaign_id, lease.proxy_work_allocation.phase)
        ] += count
        _campaign_allocation_reserved_bytes[
            (campaign_id, lease.proxy_work_allocation.allocation_id)
        ] += count
    return count


def _release_lease_reservation(lease: Lease, count: int) -> None:
    global _daily_reserved_bytes
    lease.reserved_bytes = max(0, lease.reserved_bytes - count)
    _daily_reserved_bytes = max(0, _daily_reserved_bytes - count)
    run_key = lease.dagrun_key
    url_key = (run_key, lease.canonical_url)
    _run_reserved_bytes[run_key] = max(0, _run_reserved_bytes[run_key] - count)
    _url_reserved_bytes[url_key] = max(0, _url_reserved_bytes[url_key] - count)
    if (
        count
        and lease.proxy_campaign_approval is not None
        and lease.proxy_work_allocation is not None
    ):
        campaign_id = lease.proxy_campaign_approval.campaign_id
        phase_key = (campaign_id, lease.proxy_work_allocation.phase)
        allocation_key = (
            campaign_id,
            lease.proxy_work_allocation.allocation_id,
        )
        _campaign_reserved_bytes[campaign_id] = max(
            0, _campaign_reserved_bytes[campaign_id] - count
        )
        _campaign_phase_reserved_bytes[phase_key] = max(
            0, _campaign_phase_reserved_bytes[phase_key] - count
        )
        _campaign_allocation_reserved_bytes[allocation_key] = max(
            0, _campaign_allocation_reserved_bytes[allocation_key] - count
        )


def _pending_whoscored_provider_bytes(lease: Lease) -> int:
    return sum(lease.pending_whoscored_bytes.values())


def _flush_whoscored_metering(lease: Lease) -> int:
    """Persist all exact observed WhoScored deltas as one bounded checkpoint.

    The campaign escrow is the pre-I/O authority.  This function only converts
    an observed prefix of that escrow to spend and mirrors the same prefix to
    the append-only proxy ledger.  If either durable side is ambiguous, callers
    must revoke the campaign and retain the remaining escrow; retrying the same
    batch could double-charge a campaign whose state write actually completed.
    """

    if lease.source != "whoscored":
        return 0
    if lease.metering_flush_failed:
        raise RuntimeError("WhoScored metering is already uncertain")
    if (
        lease.proxy_campaign_approval is None
        or lease.proxy_campaign_claim is None
        or lease.proxy_work_allocation is None
    ):
        raise RuntimeError(
            "WhoScored provider bytes have no signed campaign allocation"
        )
    entries = [
        (host, direction, count)
        for (host, direction), count in sorted(lease.pending_whoscored_bytes.items())
        if count
    ]
    amount = sum(count for _host, _direction, count in entries)
    if amount == 0:
        return 0
    if (
        any(
            direction not in {"up", "down"} or count <= 0
            for _host, direction, count in entries
        )
        or lease.settled_whoscored_bytes + amount != lease.total_bytes
    ):
        lease.metering_flush_failed = True
        raise RuntimeError("WhoScored pending provider accounting is inconsistent")
    try:
        # One authenticated campaign transaction covers the entire exact
        # prefix.  Proxy WAL events retain direction/host detail for the
        # independent terminal reconciliation join.
        _whoscored_campaign_ledger().consume(
            lease.proxy_campaign_approval,
            lease.proxy_campaign_claim,
            amount,
        )
        emitted = 0
        for host, direction, count in entries:
            emitted += count
            _append_budget_event(
                "bytes",
                lease,
                host=host,
                direction=direction,
                endpoint=lease.current_endpoint,
                bytes=count,
                lease_total_bytes=lease.settled_whoscored_bytes + emitted,
                dagrun_total_bytes=_run_total_bytes(lease.dagrun_key),
                url_total_bytes=_url_total_bytes(lease.dagrun_key, lease.canonical_url),
            )
    except BaseException:
        lease.metering_flush_failed = True
        lease.budget_exceeded = True
        raise
    lease.settled_whoscored_bytes += amount
    lease.pending_whoscored_bytes.clear()
    return amount


def _latch_lease_accounting_uncertainty(lease: Lease) -> None:
    """Revoke uncertain provider accounting without returning any escrow."""

    if lease.accounting_uncertain:
        return
    if lease.source == "whoscored" and not lease.metering_flush_failed:
        try:
            # The observed prefix is exact even when provider read-ahead is
            # not.  Persist that prefix, then retain every unknown/unconsumed
            # escrow byte under the terminal revocation below.
            _flush_whoscored_metering(lease)
        except Exception:  # noqa: BLE001 - ambiguity is handled by revocation
            log.exception(
                "could not flush exact provider prefix for lease %s",
                lease.lease_id,
            )
    lease.accounting_uncertain = True
    lease.closed = True
    lease.budget_exceeded = True
    for tunnel_writer in tuple(lease.tunnel_writers):
        try:
            tunnel_writer.close()
        except Exception:  # noqa: BLE001 - uncertainty already revokes the lease
            pass
    if lease.source == "whoscored" and lease.proxy_campaign_approval is not None:
        try:
            # Preserve the active claim and all of its remaining durable escrow.
            # A ledger I/O failure may also make this write fail; in that case
            # the pre-I/O escrow remains active and expiry revokes it on restart.
            _whoscored_campaign_ledger().revoke(
                lease.proxy_campaign_approval.campaign_id,
                reason="provider byte accounting became uncertain",
            )
        except Exception:  # noqa: BLE001 - escrow is still fail-closed
            log.exception(
                "could not persist accounting-uncertainty revocation for lease %s",
                lease.lease_id,
            )
    log.critical(
        "provider byte accounting is uncertain; lease %s escrow is retained",
        lease.lease_id,
    )


def _settle_observed_lease_bytes(
    lease: Lease,
    *,
    reservation: int,
    host: str,
    direction: str,
    count: int,
    force_uncertain: bool = False,
) -> None:
    """Convert a local I/O reservation to durable spend, or retain it forever."""

    if count <= 0:
        if force_uncertain:
            _latch_lease_accounting_uncertainty(lease)
        else:
            _release_lease_reservation(lease, reservation)
        return
    try:
        _account_lease_bytes(lease, host, direction, count)
    except BaseException:
        # Bytes were already queued to or read from the provider. Never release
        # either the local reservation or the lease-wide durable escrow when
        # exact accounting cannot be proven.
        _latch_lease_accounting_uncertainty(lease)
        raise
    if force_uncertain:
        # The observed prefix is exact, but cancellation/read failure can leave
        # additional provider bytes inside transport read-ahead. Charge the
        # prefix and retain every unconsumed escrow byte as unknown.
        _latch_lease_accounting_uncertainty(lease)
        return
    _release_lease_reservation(lease, reservation)


def _account_lease_bytes(lease: Lease, host: str, direction: str, count: int) -> None:
    """Account bytes that were actually written across the paid upstream leg."""
    global _daily_up_bytes, _daily_down_bytes
    if count <= 0:
        return
    if direction not in {"up", "down"}:
        raise ValueError("provider byte direction must be up or down")
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
    elif lease.source == "whoscored":
        if (
            lease.proxy_campaign_approval is None
            or lease.proxy_campaign_claim is None
            or lease.proxy_work_allocation is None
        ):
            raise RuntimeError(
                "WhoScored provider bytes have no signed campaign allocation"
            )
        if SOURCE_MODE == "whoscored-only" and (
            not lease.current_request_id or not lease.current_endpoint
        ):
            # Any background/browser byte without an active gateway-installed
            # owner is unattributable. Retain the full escrow and durably revoke
            # the campaign before refusing the chunk.
            _latch_lease_accounting_uncertainty(lease)
            raise RuntimeError(
                "WhoScored provider bytes have no active endpoint owner"
            )
        if WHOSCORED_METER_BATCH_BYTES <= 0:
            raise RuntimeError("WhoScored metering batch must be positive")
        if count > lease.global_budget_escrow_bytes:
            lease.budget_exceeded = True
            raise RuntimeError(
                "WhoScored provider bytes exceed global provider-order escrow"
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
    if lease.source == "whoscored":
        lease.global_budget_escrow_bytes -= count
        pending_key = (host, direction)
        lease.pending_whoscored_bytes[pending_key] = (
            lease.pending_whoscored_bytes.get(pending_key, 0) + count
        )
        if (
            _pending_whoscored_provider_bytes(lease) >= WHOSCORED_METER_BATCH_BYTES
            or lease.total_bytes >= lease.max_bytes
        ):
            _flush_whoscored_metering(lease)
    else:
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
    if lease.total_bytes >= lease.max_bytes or (
        lease.source != "whoscored"
        and (
            _daily_total_bytes() >= DAILY_BUDGET_BYTES
            or _run_total_bytes(lease.dagrun_key) >= _lease_dagrun_budget_bytes(lease)
            or _url_total_bytes(lease.dagrun_key, lease.canonical_url)
            >= _lease_url_budget_bytes(lease)
            or (
                lease.source == "sofascore"
                and lease.parent_run_spent_provider_bytes >= lease.parent_run_cap_bytes
            )
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


def _lease_operation_timeout(
    lease: Lease,
    *,
    ceiling_seconds: float | None = None,
) -> float:
    """Return an await timeout that can never cross the signed lease TTL."""

    remaining = lease.expires_at - _wall_time()
    if ceiling_seconds is not None:
        remaining = min(remaining, float(ceiling_seconds))
    if remaining <= 0:
        raise asyncio.TimeoutError("paid lease expired before provider operation")
    return remaining


def _normalise_client_hello_hostname(value: bytes | str) -> str:
    """Return one canonical DNS hostname, rejecting IP literals and ambiguity."""

    try:
        text = value.decode("ascii") if isinstance(value, bytes) else value
        if (
            not text
            or text != text.strip()
            or text.endswith(".")
            or len(text) > 253
            or _contains_control_character(text)
        ):
            raise ClientHelloValidationError("ClientHello SNI is not a DNS name")
        ascii_host = text.encode("idna").decode("ascii").lower().rstrip(".")
    except (UnicodeError, AttributeError) as exc:
        raise ClientHelloValidationError("ClientHello SNI is not a DNS name") from exc
    if not ascii_host or len(ascii_host) > 253:
        raise ClientHelloValidationError("ClientHello SNI is not a DNS name")
    try:
        ipaddress.ip_address(ascii_host)
    except ValueError:
        pass
    else:
        raise ClientHelloValidationError("ClientHello SNI must not be an IP literal")
    labels = ascii_host.split(".")
    if any(not _DNS_LABEL.fullmatch(label) for label in labels):
        raise ClientHelloValidationError("ClientHello SNI is not a DNS name")
    return ascii_host


def _parse_whoscored_client_hello_sni(handshake: bytes) -> str:
    """Parse one complete plaintext ClientHello and return its sole SNI name."""

    if len(handshake) < 4 or handshake[0] != _TLS_CLIENT_HELLO_HANDSHAKE_TYPE:
        raise ClientHelloValidationError("first TLS handshake is not ClientHello")
    declared_length = int.from_bytes(handshake[1:4], "big")
    if declared_length != len(handshake) - 4:
        raise ClientHelloValidationError("malformed ClientHello length")
    body = memoryview(handshake)[4:]
    cursor = 0

    def take(size: int, label: str) -> memoryview:
        nonlocal cursor
        if size < 0 or cursor + size > len(body):
            raise ClientHelloValidationError(f"malformed ClientHello {label}")
        value = body[cursor : cursor + size]
        cursor += size
        return value

    legacy_version = bytes(take(2, "legacy version"))
    if legacy_version not in (b"\x03\x01", b"\x03\x02", b"\x03\x03"):
        raise ClientHelloValidationError("unsupported ClientHello legacy version")
    take(32, "random")
    session_id_length = int(take(1, "session id length")[0])
    if session_id_length > 32:
        raise ClientHelloValidationError("malformed ClientHello session id")
    take(session_id_length, "session id")
    cipher_suites_length = int.from_bytes(take(2, "cipher suites length"), "big")
    if cipher_suites_length < 2 or cipher_suites_length % 2:
        raise ClientHelloValidationError("malformed ClientHello cipher suites")
    take(cipher_suites_length, "cipher suites")
    compression_length = int(take(1, "compression methods length")[0])
    compression_methods = bytes(take(compression_length, "compression methods"))
    if not compression_methods or 0 not in compression_methods:
        raise ClientHelloValidationError("malformed ClientHello compression methods")
    extensions_length = int.from_bytes(take(2, "extensions length"), "big")
    if extensions_length != len(body) - cursor:
        raise ClientHelloValidationError("malformed ClientHello extensions length")
    extensions_end = cursor + extensions_length
    seen_extensions: set[int] = set()
    sni_extension: bytes | None = None
    while cursor < extensions_end:
        extension_type = int.from_bytes(take(2, "extension type"), "big")
        extension_length = int.from_bytes(take(2, "extension length"), "big")
        extension_data = bytes(take(extension_length, "extension data"))
        if extension_type in seen_extensions:
            raise ClientHelloValidationError("duplicate ClientHello extension")
        seen_extensions.add(extension_type)
        if extension_type in _TLS_ECH_EXTENSION_TYPES:
            raise ClientHelloValidationError("encrypted ClientHello is not allowed")
        if extension_type == _TLS_SERVER_NAME_EXTENSION:
            sni_extension = extension_data
    if cursor != extensions_end or sni_extension is None:
        raise ClientHelloValidationError("ClientHello must contain one SNI extension")
    if len(sni_extension) < 2:
        raise ClientHelloValidationError("malformed ClientHello SNI list")
    names_length = int.from_bytes(sni_extension[:2], "big")
    if names_length != len(sni_extension) - 2:
        raise ClientHelloValidationError("malformed ClientHello SNI list length")
    names = memoryview(sni_extension)[2:]
    names_cursor = 0
    parsed_names: list[tuple[int, bytes]] = []
    while names_cursor < len(names):
        if names_cursor + 3 > len(names):
            raise ClientHelloValidationError("malformed ClientHello SNI entry")
        name_type = int(names[names_cursor])
        name_length = int.from_bytes(names[names_cursor + 1 : names_cursor + 3], "big")
        names_cursor += 3
        if name_length <= 0 or names_cursor + name_length > len(names):
            raise ClientHelloValidationError("malformed ClientHello SNI entry")
        parsed_names.append(
            (name_type, bytes(names[names_cursor : names_cursor + name_length]))
        )
        names_cursor += name_length
    if names_cursor != len(names) or len(parsed_names) != 1:
        raise ClientHelloValidationError(
            "ClientHello must contain exactly one SNI name"
        )
    name_type, raw_name = parsed_names[0]
    if name_type != 0:
        raise ClientHelloValidationError("ClientHello SNI is not a host_name")
    return _normalise_client_hello_hostname(raw_name)


async def _read_and_validate_whoscored_client_hello(
    reader: asyncio.StreamReader,
    lease: Lease,
    *,
    connect_host: str,
    connect_port: int,
) -> bytes:
    """Buffer a bounded ClientHello and authorize it before any provider dial."""

    if connect_port != 443:
        raise ClientHelloValidationError("WhoScored CONNECT must use port 443")
    normalized_target = _normalise_client_hello_hostname(connect_host)
    if normalized_target not in WHOSCORED_PROXY_ALLOWED_HOSTS:
        raise ClientHelloValidationError("WhoScored CONNECT host is not allowed")
    deadline = time.monotonic() + min(
        WHOSCORED_CLIENT_HELLO_TIMEOUT_SECONDS,
        _lease_operation_timeout(lease),
    )

    async def read_exact(size: int) -> bytes:
        chunks: list[bytes] = []
        remaining = size
        while remaining:
            timeout = min(
                deadline - time.monotonic(),
                _lease_operation_timeout(lease),
            )
            if timeout <= 0:
                raise ClientHelloValidationError("ClientHello timed out")
            try:
                chunk = await asyncio.wait_for(reader.read(remaining), timeout=timeout)
            except (asyncio.TimeoutError, TimeoutError) as exc:
                raise ClientHelloValidationError("ClientHello timed out") from exc
            if not chunk:
                raise ClientHelloValidationError("ClientHello ended early")
            if len(chunk) > remaining:
                raise ClientHelloValidationError(
                    "ClientHello reader exceeded its bound"
                )
            chunks.append(chunk)
            remaining -= len(chunk)
        return b"".join(chunks)

    wire = bytearray()
    handshake = bytearray()
    expected_handshake_bytes: int | None = None
    while True:
        record_header = await read_exact(5)
        wire.extend(record_header)
        if (
            record_header[0] != _TLS_HANDSHAKE_CONTENT_TYPE
            or record_header[1] != 3
            or record_header[2] not in (1, 2, 3)
        ):
            raise ClientHelloValidationError("ClientHello is not plaintext TLS")
        record_length = int.from_bytes(record_header[3:5], "big")
        if not 0 < record_length <= MAX_TLS_PLAINTEXT_RECORD_BYTES:
            raise ClientHelloValidationError("TLS record length is invalid")
        if len(wire) + record_length > MAX_WHOSCORED_CLIENT_HELLO_BYTES:
            raise ClientHelloValidationError("ClientHello exceeds the buffer limit")
        fragment = await read_exact(record_length)
        wire.extend(fragment)
        handshake.extend(fragment)
        if expected_handshake_bytes is None and len(handshake) >= 4:
            if handshake[0] != _TLS_CLIENT_HELLO_HANDSHAKE_TYPE:
                raise ClientHelloValidationError(
                    "first TLS handshake is not ClientHello"
                )
            expected_handshake_bytes = 4 + int.from_bytes(handshake[1:4], "big")
            if not 4 < expected_handshake_bytes <= MAX_WHOSCORED_CLIENT_HELLO_BYTES:
                raise ClientHelloValidationError("ClientHello length is invalid")
        if expected_handshake_bytes is None:
            continue
        if len(handshake) > expected_handshake_bytes:
            raise ClientHelloValidationError("ClientHello has trailing handshake data")
        if len(handshake) == expected_handshake_bytes:
            sni = _parse_whoscored_client_hello_sni(bytes(handshake))
            if sni != normalized_target:
                raise ClientHelloValidationError(
                    "ClientHello SNI does not match CONNECT"
                )
            return bytes(wire)


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
    provider_eof_observed = False
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
                elif lease is not None:
                    chunk = await asyncio.wait_for(
                        reader.read(read_size),
                        timeout=_lease_operation_timeout(lease),
                    )
                else:
                    chunk = await reader.read(read_size)
            except (asyncio.TimeoutError, TimeoutError):
                if lease is not None:
                    # At the hard TTL boundary the paired provider stream may
                    # already contain billed read-ahead, even when this is the
                    # client->provider pump. Revoke the lease, close both ends,
                    # and retain every unproven escrow byte.
                    _latch_lease_accounting_uncertainty(lease)
                raise
            except BaseException:
                if lease is not None:
                    if direction == "down":
                        # A cancelled provider read can leave unobservable
                        # transport-buffered bytes. Retain the reservation.
                        _latch_lease_accounting_uncertainty(lease)
                    else:
                        _release_lease_reservation(lease, reservation)
                raise
            if lease is not None and lease.expired:
                # A cancellation-resistant reader or an event-loop stall can
                # return after wait_for's deadline. Never turn those bytes into
                # ordinary post-expiry settlement or forward them downstream.
                _latch_lease_accounting_uncertainty(lease)
                raise asyncio.TimeoutError("paid lease expired during read")
            if lease is not None and direction == "down" and not chunk:
                # StreamReader returns EOF only after its internal buffer has
                # drained. This is the sole proof that no paid response
                # read-ahead remains when the downstream pump exits cleanly.
                provider_eof_observed = True
            if lease is not None:
                assert direction in ("up", "down")
                _settle_observed_lease_bytes(
                    lease,
                    reservation=reservation,
                    host=host,
                    direction=direction,
                    count=len(chunk),
                )
            elif budget_guard is not None and chunk and not precharged:
                budget_guard.consume(len(chunk))
            if not chunk:
                break
            try:
                writer.write(chunk)
                if lease is None:
                    await writer.drain()
                else:
                    await asyncio.wait_for(
                        writer.drain(),
                        timeout=_lease_operation_timeout(lease),
                    )
            except BaseException:
                if lease is not None and direction == "down":
                    # The returned chunk is exact and already durable, but the
                    # provider StreamReader may hold additional billed
                    # read-ahead that can no longer be observed once the
                    # downstream client fails or this task is cancelled.
                    _latch_lease_accounting_uncertainty(lease)
                raise
            if lease is None:
                counter[host] += len(chunk)
    except Exception:  # noqa: BLE001 — proxy must never crash a flow
        pass
    finally:
        if lease is not None and direction == "down" and not provider_eof_observed:
            # Includes TTL/closed/budget refusal before a read, reservation
            # errors swallowed by the proxy boundary, and cancellation between
            # chunks. Only an observed provider EOF may release the lifecycle.
            _latch_lease_accounting_uncertainty(lease)
        try:
            writer.close()
        except Exception:  # noqa: BLE001
            pass


async def _run_tunnel_pumps(
    client_r: asyncio.StreamReader,
    client_w: asyncio.StreamWriter,
    srv_r: asyncio.StreamReader,
    srv_w: asyncio.StreamWriter,
    host: str,
    *,
    lease: Lease | None,
) -> None:
    """Hand a provider stream to both pumps without an unowned read-ahead gap."""

    guard = provider_budget_guard if lease is None else None
    try:
        await asyncio.gather(
            _pump(
                client_r,
                srv_w,
                host,
                up_bytes,
                guard,
                lease=lease,
                direction="up",
            ),
            _pump(
                srv_r,
                client_w,
                host,
                down_bytes,
                guard,
                lease=lease,
                direction="down",
            ),
        )
    except BaseException:
        if lease is not None:
            # Cancellation can reach gather before the down coroutine executes
            # its first provider read, while the transport already owns billed
            # response bytes. Retain all remaining escrow in that handoff gap.
            _latch_lease_accounting_uncertainty(lease)
        raise


async def _read_headers(reader: asyncio.StreamReader) -> list[bytes]:
    lines: list[bytes] = []
    while True:
        h = await reader.readline()
        if h in (b"\r\n", b"\n", b""):
            return lines
        lines.append(h)


class ClientHeadError(ValueError):
    """A client request head that must be rejected before authentication."""

    status = b"400 Bad Request"


class ClientHeadTooLarge(ClientHeadError):
    status = b"431 Request Header Fields Too Large"


class ClientHeadTimeout(ClientHeadError):
    status = b"408 Request Timeout"


class ClientHeadIncomplete(ClientHeadError):
    pass


async def _read_client_request_head(
    reader: asyncio.StreamReader,
) -> tuple[bytes, list[bytes]]:
    """Read one complete, strictly bounded unauthenticated HTTP request head."""

    deadline = time.monotonic() + CLIENT_HEAD_TIMEOUT_SECONDS

    async def read_line(max_bytes: int) -> bytes:
        remaining = deadline - time.monotonic()
        if remaining <= 0:
            raise ClientHeadTimeout("client request head timed out")
        try:
            line = await asyncio.wait_for(reader.readline(), timeout=remaining)
        except (asyncio.TimeoutError, TimeoutError) as exc:
            raise ClientHeadTimeout("client request head timed out") from exc
        except (asyncio.LimitOverrunError, ValueError) as exc:
            # StreamReader.readline() converts its configured-buffer overflow
            # into ValueError on supported Python versions.
            raise ClientHeadTooLarge("client request head line is too large") from exc
        if len(line) > max_bytes:
            raise ClientHeadTooLarge("client request head line is too large")
        return line

    first = await read_line(MAX_CLIENT_REQUEST_LINE_BYTES)
    if not first:
        return b"", []
    if not first.endswith(b"\n"):
        raise ClientHeadIncomplete("incomplete client request line")

    total_bytes = len(first)
    if total_bytes > MAX_CLIENT_HEADER_BYTES:
        raise ClientHeadTooLarge("client request head is too large")

    lines: list[bytes] = []
    while True:
        line = await read_line(MAX_CLIENT_HEADER_LINE_BYTES)
        if not line:
            raise ClientHeadIncomplete("client disconnected before end of headers")
        total_bytes += len(line)
        if total_bytes > MAX_CLIENT_HEADER_BYTES:
            raise ClientHeadTooLarge("client request head is too large")
        if line in (b"\r\n", b"\n"):
            return first, lines
        if not line.endswith(b"\n"):
            raise ClientHeadIncomplete("incomplete client header line")
        if len(lines) >= MAX_CLIENT_HEADER_COUNT:
            raise ClientHeadTooLarge("too many client request headers")
        lines.append(line)


async def _reject_client_head(
    writer: asyncio.StreamWriter, error: ClientHeadError
) -> None:
    writer.write(b"HTTP/1.1 " + error.status + b"\r\nConnection: close\r\n\r\n")
    await writer.drain()
    writer.close()


async def _read_metered_provider_head(
    reader: asyncio.StreamReader,
    lease: Lease,
    host: str,
    timeout_seconds: float | None = None,
) -> tuple[bytes, list[bytes]]:
    """Read an upstream HTTP response head without crossing a paid budget.

    ``StreamReader.readline`` has no per-call byte limit. Reading the provider
    response one byte at a time under one pre-reserved window is deliberate:
    CONNECT heads are small, and this prevents the proxy's observed-stream
    counter from crossing the local daily/DagRun/lease boundary. It is not an
    invoice guarantee; only a provider-side quota can bound billed bytes.

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
    deadline = None if timeout_seconds is None else time.monotonic() + timeout_seconds

    def settle_payload(*, force_uncertain: bool = False) -> None:
        _settle_observed_lease_bytes(
            lease,
            reservation=reservation,
            host=host,
            direction="down",
            count=len(payload),
            force_uncertain=force_uncertain,
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
    except BaseException:
        settle_payload(force_uncertain=True)
        raise
    # A timeout (including one before the first visible byte) or exhausting the
    # bounded head window can leave provider bytes in StreamReader/transport
    # read-ahead.  Charge the exact visible prefix, revoke the lease and retain
    # every remaining escrow byte instead of treating the exit as retryable.
    settle_payload(
        force_uncertain=timed_out or (not complete and len(payload) >= reservation)
    )
    if timed_out:
        raise UpstreamHeadTimeout("provider response head timed out")
    if not complete:
        if len(payload) >= reservation:
            lease.budget_exceeded = True
            raise RuntimeError("incomplete or over-budget provider response head")
        raise UpstreamHeadIncomplete("provider closed before a complete response head")
    lines = bytes(payload).splitlines(keepends=True)
    if not lines:
        raise RuntimeError("empty provider response head")
    return lines[0], lines[1:-1]


def _provider_connect_status_code(status: bytes) -> int | None:
    """Return an exact CONNECT status code, rejecting ambiguous status lines."""

    match = _PROVIDER_HTTP_STATUS_LINE.fullmatch(status)
    if match is None:
        return None
    return int(match.group(1))


def _header_map(lines: list[bytes]) -> dict[str, str]:
    headers: dict[str, str] = {}
    for line in lines:
        try:
            name, value = line.decode("latin1").split(":", 1)
        except ValueError:
            continue
        headers[name.strip().lower()] = value.strip()
    return headers


def _strict_control_header_map(lines: list[bytes]) -> dict[str, str]:
    """Reject duplicate/ambiguous headers before any control body is read."""

    headers: dict[str, str] = {}
    for line in lines:
        try:
            name, value = line.decode("latin1").split(":", 1)
        except (UnicodeDecodeError, ValueError) as exc:
            raise ValueError("invalid control header") from exc
        lowered = name.strip().lower()
        rendered = value.strip()
        if (
            not lowered
            or lowered in headers
            or any(
                ord(character) <= 32 or ord(character) == 127 for character in lowered
            )
            or "\r" in rendered
            or "\n" in rendered
        ):
            raise ValueError("ambiguous control header")
        headers[lowered] = rendered
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
    body = canonical_json_bytes(payload)
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
    if lease.expected_endpoint_labels and (
        endpoint not in lease.expected_endpoint_labels
        or endpoint in lease.endpoint_request_provider_bytes
        or (
            not lease.endpoint_request_provider_bytes
            and endpoint != lease.expected_endpoint_labels[0]
        )
    ):
        raise ValueError("endpoint is absent, duplicate, or stale for this lease")
    request_id = uuid_hex(24)
    if lease.source == "sofascore":
        _append_allocation_wal(
            "endpoint_started",
            lease.lease_id,
            request_id=request_id,
            endpoint=endpoint,
        )
    elif lease.source == "whoscored" and SOURCE_MODE == "whoscored-only":
        _append_budget_event(
            "endpoint_started",
            lease,
            request_id=request_id,
            endpoint=endpoint,
            lease_total_bytes=lease.total_bytes,
        )
    lease.current_request_id = request_id
    lease.current_endpoint = endpoint
    lease.current_request_start_bytes = lease.total_bytes
    return request_id


def _switch_endpoint_request(
    lease: Lease, request_id: str, endpoint: str
) -> str:
    """Durably replace the browser byte owner without an event-loop gap."""

    endpoint = str(endpoint or "").strip()
    if not lease.current_request_id or not secrets.compare_digest(
        lease.current_request_id, str(request_id or "")
    ):
        raise ValueError("endpoint request id is stale or invalid")
    if not endpoint or len(endpoint) > 200 or endpoint == lease.current_endpoint:
        raise ValueError("next endpoint must be a different bounded name")
    if lease.closed or lease.expired:
        raise RuntimeError("lease cannot switch endpoint ownership")
    if lease.expected_endpoint_labels and (
        endpoint not in lease.expected_endpoint_labels
        or endpoint in lease.endpoint_request_provider_bytes
    ):
        raise ValueError("next endpoint is absent, duplicate, or stale for this lease")
    if lease.source != "whoscored" or SOURCE_MODE != "whoscored-only":
        _finish_endpoint_request(lease, request_id)
        return _begin_endpoint_request(lease, endpoint)

    _flush_whoscored_metering(lease)
    amount = lease.total_bytes - lease.current_request_start_bytes
    if amount < 0:
        raise RuntimeError("lease provider counter moved backwards")
    next_request_id = uuid_hex(24)
    # One fsync-protected transition is committed before either in-memory
    # owner changes. The async event loop cannot account a provider chunk in
    # the middle of this synchronous transition.
    _append_budget_event(
        "endpoint_switched",
        lease,
        request_id=lease.current_request_id,
        endpoint=lease.current_endpoint,
        provider_bytes=amount,
        next_request_id=next_request_id,
        next_endpoint=endpoint,
        lease_total_bytes=lease.total_bytes,
    )
    lease.endpoint_request_provider_bytes.setdefault(
        lease.current_endpoint, []
    ).append(amount)
    lease.current_request_id = next_request_id
    lease.current_endpoint = endpoint
    lease.current_request_start_bytes = lease.total_bytes
    return next_request_id


def _finish_endpoint_request(lease: Lease, request_id: str) -> int:
    if not lease.current_request_id or not secrets.compare_digest(
        lease.current_request_id, str(request_id or "")
    ):
        raise ValueError("endpoint request id is stale or invalid")
    endpoint = lease.current_endpoint
    if lease.source == "whoscored":
        # Convert every observed byte to durable campaign spend before sealing
        # the endpoint boundary in the independent HMAC-chained proxy ledger.
        _flush_whoscored_metering(lease)
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
    elif lease.source == "whoscored" and SOURCE_MODE == "whoscored-only":
        _append_budget_event(
            "endpoint_finished",
            lease,
            request_id=lease.current_request_id,
            endpoint=endpoint,
            provider_bytes=amount,
            lease_total_bytes=lease.total_bytes,
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
        if lease.accounting_uncertain:
            # Never let a future refactor/manual recovery turn uncertainty into
            # a normal drained lease, even if its in-process reservation was
            # accidentally cleared. Durable campaign escrow is forensic state.
            lease.closed = True
            for tunnel_writer in tuple(lease.tunnel_writers):
                try:
                    tunnel_writer.close()
                except Exception:  # noqa: BLE001 - lease is already revoked
                    pass
            continue
        if not lease.expired:
            continue
        if lease.active_tunnels or lease.reserved_bytes:
            # TTL is a wall-clock data-plane boundary, not merely a refusal for
            # the next chunk. Force-close orphan browser/provider sockets now.
            # A provider StreamReader can contain unobservable read-ahead, so
            # production leases retain all remaining escrow and claims.
            if lease.source in {"sofascore", "whoscored"}:
                _latch_lease_accounting_uncertainty(lease)
            else:
                lease.closed = True
                for tunnel_writer in tuple(lease.tunnel_writers):
                    try:
                        tunnel_writer.close()
                    except Exception:  # noqa: BLE001 - lease is already expired
                        pass
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
                endpoint_request_provider_bytes=(lease.endpoint_request_provider_bytes),
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
        elif lease.source == "whoscored" and not lease.proxy_campaign_finished:
            if (
                lease.proxy_campaign_approval is None
                or lease.proxy_campaign_claim is None
            ):
                raise RuntimeError(
                    "expired WhoScored lease has no signed campaign claim"
                )
            lease.closed = True
            for tunnel_writer in tuple(lease.tunnel_writers):
                try:
                    tunnel_writer.close()
                except Exception:  # noqa: BLE001 - lease is already revoked
                    pass
            if lease.current_request_id:
                _finish_endpoint_request(lease, lease.current_request_id)
            try:
                _flush_whoscored_metering(lease)
            except Exception:  # noqa: BLE001 - retain escrow and revoke
                _latch_lease_accounting_uncertainty(lease)
                continue
            if (
                _pending_whoscored_provider_bytes(lease)
                or lease.settled_whoscored_bytes != lease.total_bytes
            ):
                _latch_lease_accounting_uncertainty(lease)
                continue
            released = _whoscored_campaign_ledger().release_provider_reservation(
                lease.proxy_campaign_approval,
                lease.proxy_campaign_claim,
            )
            _release_whoscored_global_escrow(lease, released)
            _whoscored_campaign_ledger().finish(
                lease.proxy_campaign_approval,
                lease.proxy_campaign_claim,
                provider_billed_bytes=lease.total_bytes,
                completed=False,
            )
            lease.proxy_campaign_finished = True
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
                    endpoint_request_provider_bytes=(
                        lease.endpoint_request_provider_bytes
                        if lease.source == "whoscored"
                        else {}
                    ),
                    expired=True,
                )
                lease.close_recorded = True
            except Exception:  # noqa: BLE001 - byte deltas are already durable
                log.exception("could not persist expiry for lease %s", lease.lease_id)
    _prune_finalized_leases()
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
    drained = (
        lease.active_tunnels == 0
        and lease.reserved_bytes == 0
        and not lease.accounting_uncertain
    )
    if drained and lease.source == "whoscored":
        try:
            _flush_whoscored_metering(lease)
        except Exception:  # noqa: BLE001 - terminal response must stay red
            _latch_lease_accounting_uncertainty(lease)
            drained = False
        if drained and (
            _pending_whoscored_provider_bytes(lease)
            or lease.settled_whoscored_bytes != lease.total_bytes
        ):
            _latch_lease_accounting_uncertainty(lease)
            drained = False
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
    elif lease.source == "whoscored" and SOURCE_MODE == "whoscored-only":
        try:
            normalized_internal_map = _normalize_endpoint_map(
                lease.endpoint_request_provider_bytes
            )
        except ValueError:
            normalized_internal_map = {}
            client_map_matches = False
        if (
            not normalized_internal_map
            or normalized_internal_map != lease.endpoint_request_provider_bytes
            or sum(
                sum(observations)
                for observations in normalized_internal_map.values()
            )
            != lease.total_bytes
        ):
            client_map_matches = False
        if lease.expected_endpoint_labels and (
            set(normalized_internal_map) != set(lease.expected_endpoint_labels)
            or any(
                len(normalized_internal_map.get(endpoint, ())) != 1
                for endpoint in lease.expected_endpoint_labels
            )
        ):
            client_map_matches = False
    if lease.source == "whoscored" and drained and not lease.proxy_campaign_finished:
        if lease.proxy_campaign_approval is None or lease.proxy_campaign_claim is None:
            raise RuntimeError("WhoScored lease has no signed campaign claim")
        released = _whoscored_campaign_ledger().release_provider_reservation(
            lease.proxy_campaign_approval,
            lease.proxy_campaign_claim,
        )
        _release_whoscored_global_escrow(lease, released)
        _whoscored_campaign_ledger().finish(
            lease.proxy_campaign_approval,
            lease.proxy_campaign_claim,
            provider_billed_bytes=lease.total_bytes,
            completed=bool(completed and client_map_matches),
        )
        lease.proxy_campaign_finished = True
    if drained and not lease.close_recorded:
        try:
            _append_budget_event(
                "lease_closed",
                lease,
                total_bytes=lease.total_bytes,
                endpoint_request_provider_bytes=(
                    lease.endpoint_request_provider_bytes
                    if lease.source == "whoscored"
                    else {}
                ),
            )
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
        and (lease.source != "whoscored" or lease.proxy_campaign_finished)
        and (lease.source != "whoscored" or not lease.global_budget_escrow_bytes)
        and (
            lease.source != "whoscored"
            or (
                not lease.pending_whoscored_bytes
                and lease.settled_whoscored_bytes == lease.total_bytes
                and not lease.metering_flush_failed
            )
        )
        and client_map_matches
    )
    if lease.accounting_uncertain:
        report["close_error"] = (
            "provider byte accounting is uncertain; durable escrow retained"
        )
    elif not client_map_matches:
        report["close_error"] = "endpoint provider map mismatch"
    if report["close_complete"]:
        if lease.finalized_at <= 0:
            lease.finalized_at = _wall_time()
        _prune_finalized_leases()
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
        "sofascore_discovery_enabled": (SOFASCORE_DISCOVERY_DAGRUN_BUDGET_BYTES > 0),
        "sofascore_discovery_dagrun_budget_bytes": (
            SOFASCORE_DISCOVERY_DAGRUN_BUDGET_BYTES
        ),
        "transfermarkt_paid_enabled": (
            TRANSFERMARKT_DAGRUN_BUDGET_BYTES > 0 and bool(TRANSFERMARKT_CONTROL_TOKEN)
        ),
        "transfermarkt_dagrun_budget_bytes": TRANSFERMARKT_DAGRUN_BUDGET_BYTES,
        "whoscored_default_paid_cap_bytes": DEFAULT_WHOSCORED_PAID_CAP_BYTES,
        "whoscored_signed_campaigns_required": True,
        "whoscored_provider_invoice_hard_cap_available": (
            WHOSCORED_PROVIDER_INVOICE_HARD_CAP_AVAILABLE
        ),
        "whoscored_paid_application_gateway_available": (
            WHOSCORED_PAID_APPLICATION_GATEWAY_AVAILABLE
        ),
        "whoscored_paid_enabled": (
            WHOSCORED_PROVIDER_INVOICE_HARD_CAP_AVAILABLE
            and WHOSCORED_PAID_APPLICATION_GATEWAY_AVAILABLE
            and SOURCE_MODE == "whoscored-only"
        ),
        "source_mode": SOURCE_MODE,
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

    if path == "/v1/whoscored/campaign-control":
        if SOURCE_MODE != "whoscored-only":
            await _send_json(writer, 404, {"error": "unknown control endpoint"})
            return True
        if method != "POST":
            await _send_json(writer, 404, {"error": "unknown control endpoint"})
            return True
        if not _control_token_valid(headers, source="whoscored"):
            await _send_json(writer, 401, {"error": "invalid control token"})
            return True
        if headers.get("transfer-encoding"):
            await _send_json(writer, 400, {"error": "invalid request framing"})
            return True
        try:
            raw_length = headers.get("content-length", "")
            if (
                not raw_length.isascii()
                or not raw_length.isdigit()
                or raw_length != str(int(raw_length or "0"))
            ):
                raise ValueError("campaign control content length is invalid")
            length = int(raw_length)
            if not 0 < length <= MAX_CONTROL_BODY_BYTES:
                raise ValueError("campaign control body size is invalid")
            raw = await reader.readexactly(length)
            request = strict_json_loads(raw)
            if not isinstance(request, Mapping) or frozenset(request) != {
                "schema_version",
                "operation",
                "context",
                "arguments",
            }:
                raise ValueError("campaign control request fields are invalid")
            if canonical_json_bytes(dict(request)) != raw:
                raise ValueError("campaign control request is not canonical JSON")
            if request.get("schema_version") != PROXY_CAMPAIGN_CONTROL_SCHEMA_VERSION:
                raise ValueError("campaign control schema is invalid")
            operation = request.get("operation")
            arguments = request.get("arguments")
            context = request.get("context")
            expected_arguments = PROXY_CAMPAIGN_CONTROL_ARGUMENT_FIELDS.get(operation)
            if (
                expected_arguments is None
                or not isinstance(arguments, Mapping)
                or frozenset(arguments) != expected_arguments
                or not isinstance(context, Mapping)
            ):
                raise ValueError("campaign control operation is invalid")
            require_active = operation not in {
                "seal_for_reconciliation",
                "sealed_snapshot",
            }
            approval = approval_from_campaign_authority_context(
                context,
                secret=WHOSCORED_PROXY_APPROVAL_HMAC_SECRET,
                require_active=require_active,
            )
            _assert_whoscored_approval_state_binding(approval)
            if operation in {"complete_allocation", "seal_for_reconciliation"} and (
                arguments.get("dag_id") != approval.allowed_dag_ids[0]
                or arguments.get("run_id") != approval.run_id
            ):
                raise ProxyCampaignValidationError(
                    "campaign control DAG/run differs from signed authority"
                )
            ledger = _whoscored_campaign_ledger()
            if operation == "snapshot":
                result = {"campaign": ledger.snapshot(approval)}
            elif operation == "complete_allocation":
                result = {
                    "allocation": ledger.complete_allocation(
                        approval,
                        arguments["allocation_id"],
                        dag_id=arguments["dag_id"],
                        run_id=arguments["run_id"],
                        task_id=arguments["task_id"],
                        attempt_id=arguments["attempt_id"],
                        report_sha256=arguments["report_sha256"],
                        request_ledger_sha256=arguments["request_ledger_sha256"],
                    )
                }
            elif operation == "assert_exact_accounting":
                result = {
                    "provider_billed_bytes": ledger.assert_exact_accounting(
                        approval,
                        task_report_provider_bytes=arguments[
                            "task_report_provider_bytes"
                        ],
                        request_ledger_provider_bytes=arguments[
                            "request_ledger_provider_bytes"
                        ],
                        proxy_ledger_provider_bytes=arguments[
                            "proxy_ledger_provider_bytes"
                        ],
                        require_complete=arguments["require_complete"],
                    )
                }
            elif operation == "seal_for_reconciliation":
                result = {
                    "campaign": ledger.seal_for_reconciliation(
                        approval,
                        dag_id=arguments["dag_id"],
                        run_id=arguments["run_id"],
                        provider_billed_bytes=arguments["provider_billed_bytes"],
                        attempt_accounting_sha256=arguments[
                            "attempt_accounting_sha256"
                        ],
                    )
                }
            else:
                result = {"campaign": ledger.sealed_snapshot(approval)}
            if frozenset(result) != PROXY_CAMPAIGN_CONTROL_RESULT_FIELDS[operation]:
                raise RuntimeError("campaign control result schema mismatch")
        except (
            ValueError,
            TypeError,
            json.JSONDecodeError,
            UnicodeDecodeError,
            ProxyCampaignError,
        ):
            await _send_json(
                writer,
                409,
                {"code": "campaign_control_rejected", "error": "campaign rejected"},
            )
            return True
        except (OSError, asyncio.IncompleteReadError, RuntimeError):
            await _send_json(
                writer,
                503,
                {
                    "code": "campaign_control_unavailable",
                    "error": "control unavailable",
                },
            )
            return True
        response_document = {
            "schema_version": PROXY_CAMPAIGN_CONTROL_SCHEMA_VERSION,
            "operation": operation,
            "result": result,
        }
        if len(canonical_json_bytes(response_document)) > MAX_CONTROL_BODY_BYTES:
            await _send_json(
                writer,
                503,
                {
                    "code": "campaign_control_unavailable",
                    "error": "control response is oversized",
                },
            )
            return True
        await _send_json(writer, 200, response_document)
        return True

    if path.startswith("/v1/leases") and not _any_control_token_valid(headers):
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
            request_source = _source_for_dag(str(request.get("dag_id") or "").strip())
            if not _control_token_valid(headers, source=request_source):
                await _send_json(writer, 401, {"error": "invalid control token"})
                return True
            lease = _create_lease(
                mgr,
                max_bytes=int(request.get("max_bytes", DEFAULT_LEASE_BYTES)),
                ttl_seconds=int(request.get("ttl_seconds", DEFAULT_LEASE_TTL_SECONDS)),
                metadata=request,
                require_context=True,
            )
        except ProxyCampaignConcurrencyLimited as exc:
            await _send_json(
                writer,
                429,
                {"code": "concurrency_limited", "error": str(exc)},
            )
            return True
        except ProxyCampaignBudgetExceeded as exc:
            await _send_json(
                writer,
                429,
                {"code": "budget_exceeded", "error": str(exc)},
            )
            return True
        except ProxyCampaignError as exc:
            await _send_json(
                writer,
                409,
                {"code": "campaign_rejected", "error": str(exc)},
            )
            return True
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
                "proxy_campaign_id": (
                    lease.proxy_campaign_approval.campaign_id
                    if lease.proxy_campaign_approval
                    else ""
                ),
                "proxy_approval_id": (
                    lease.proxy_campaign_approval.approval_id
                    if lease.proxy_campaign_approval
                    else ""
                ),
                "proxy_approval_sha256": (
                    lease.proxy_campaign_approval.approval_sha256
                    if lease.proxy_campaign_approval
                    else ""
                ),
                "provider_meter": (
                    PROXY_CAMPAIGN_METER if lease.proxy_campaign_approval else ""
                ),
            },
        )
        return True

    parts = path.strip("/").split("/")
    if len(parts) == 4 and parts[:2] == ["v1", "leases"] and parts[3] == "endpoints":
        lease = _authorized_control_lease(parts[2], headers.get("authorization"))
        if lease is None or not _control_token_valid(headers, source=lease.source):
            await _send_json(writer, 401, {"error": "invalid control or lease token"})
            return True
        if method != "POST":
            await _send_json(writer, 404, {"error": "unknown lease endpoint"})
            return True
        try:
            length = int(headers.get("content-length", "0"))
            if length <= 0 or length > 4096:
                raise ValueError("endpoint request body must be in 1..4096 bytes")
            body = json.loads((await reader.readexactly(length)).decode("utf-8"))
            if not isinstance(body, dict) or frozenset(body) != {"endpoint"}:
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
    if (
        len(parts) == 6
        and parts[:2] == ["v1", "leases"]
        and parts[3] == "endpoints"
        and parts[5] == "switch"
    ):
        lease = _authorized_control_lease(parts[2], headers.get("authorization"))
        if lease is None or not _control_token_valid(headers, source=lease.source):
            await _send_json(writer, 401, {"error": "invalid control or lease token"})
            return True
        if method != "POST":
            await _send_json(writer, 404, {"error": "unknown lease endpoint"})
            return True
        try:
            length = int(headers.get("content-length", "0"))
            if length <= 0 or length > 4096:
                raise ValueError("endpoint switch body must be in 1..4096 bytes")
            body = json.loads((await reader.readexactly(length)).decode("utf-8"))
            if not isinstance(body, dict) or frozenset(body) != {"endpoint"}:
                raise ValueError("endpoint switch body must contain only endpoint")
            next_request_id = _switch_endpoint_request(
                lease, parts[4], body["endpoint"]
            )
        except (ValueError, TypeError, json.JSONDecodeError, UnicodeDecodeError) as exc:
            await _send_json(writer, 400, {"error": str(exc)})
            return True
        except RuntimeError as exc:
            await _send_json(
                writer,
                503,
                {"code": "endpoint_accounting_unavailable", "error": str(exc)},
            )
            return True
        await _send_json(writer, 201, {"request_id": next_request_id})
        return True
    if len(parts) == 5 and parts[:2] == ["v1", "leases"] and parts[3] == "endpoints":
        lease = _authorized_control_lease(parts[2], headers.get("authorization"))
        if lease is None or not _control_token_valid(headers, source=lease.source):
            await _send_json(writer, 401, {"error": "invalid control or lease token"})
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
    if lease is None or not _control_token_valid(headers, source=lease.source):
        await _send_json(writer, 401, {"error": "invalid control or lease token"})
        return True
    if method == "POST" and action == "extend":
        try:
            length = int(headers.get("content-length", "0"))
            if length <= 0 or length > MAX_CONTROL_BODY_BYTES:
                raise ValueError(
                    f"lease extension body must be in 1..{MAX_CONTROL_BODY_BYTES} bytes"
                )
            request = json.loads((await reader.readexactly(length)).decode("utf-8"))
            if not isinstance(request, dict):
                raise ValueError("lease extension body must be an object")
            requested_max = request.get("max_bytes")
            if isinstance(requested_max, bool) or not isinstance(requested_max, int):
                raise ValueError("max_bytes must be an integer")
            report = _extend_fbref_lease(lease, requested_max)
        except (ValueError, TypeError, json.JSONDecodeError, UnicodeDecodeError) as exc:
            await _send_json(writer, 400, {"error": str(exc)})
            return True
        except RuntimeError as exc:
            await _send_json(
                writer,
                409,
                {"code": "lease_extend_rejected", "error": str(exc)},
            )
            return True
        await _send_json(writer, 200, report)
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
    lease = _authorized_control_lease(parts[2], headers.get("authorization"))
    if lease is None or not _control_token_valid(headers, source=lease.source):
        await _send_json(writer, 401, {"error": "invalid control or lease token"})
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
    try:
        writer.write(payload)
    except BaseException:
        if lease is not None:
            # A transport may have accepted a prefix before surfacing an error.
            # Treat the complete pre-I/O reservation as unknown provider spend.
            _latch_lease_accounting_uncertainty(lease)
        raise
    if lease is not None:
        _settle_observed_lease_bytes(
            lease,
            reservation=reservation,
            host=host,
            direction=direction,
            count=len(payload),
        )
    try:
        if lease is None:
            await writer.drain()
        else:
            await asyncio.wait_for(
                writer.drain(),
                timeout=_lease_operation_timeout(lease),
            )
    except BaseException:
        if lease is not None:
            # ``write`` may have reached the provider and a response may already
            # be buffered on the paired reader even though drain reports failure.
            # Outbound bytes are exact above; inbound read-ahead is not, so the
            # lease cannot safely fail over or return its remaining escrow.
            _latch_lease_accounting_uncertainty(lease)
        raise
    return True


def _record_whoscored_provider_dial(lease: Lease) -> None:
    """Charge one signed request slot before every provider TCP dial."""

    if lease.source != "whoscored":
        return
    if lease.proxy_campaign_approval is None or lease.proxy_campaign_claim is None:
        raise ProxyCampaignValidationError(
            "WhoScored provider dial has no signed campaign claim"
        )
    lease.provider_request_count = _whoscored_campaign_ledger().record_request(
        lease.proxy_campaign_approval,
        lease.proxy_campaign_claim,
    )


async def _open_lease_upstream_tunnel(
    lease: Lease,
    mgr,
    *,
    target: str,
    host: str,
) -> tuple[asyncio.StreamReader, asyncio.StreamWriter, bytes, list[bytes]]:
    """Open a lease CONNECT tunnel, failing over only a proven empty EOF/reset.

    A dead exit accepts the TCP connection and then never answers the CONNECT.
    Each attempt dials the lease's currently-pinned exit, sends the CONNECT head
    (its up-bytes are metered even against a dead exit) and reads the metered
    response head under a deadline.  A connect failure or an immediate empty
    EOF may be re-pinned before the first down byte, except that FBref's
    one-attempt contract never re-pins even after a zero-byte TCP failure.  A
    head timeout is never retryable for any source: transport read-ahead makes
    its provider-byte total unknowable, so the reader latches accounting
    uncertainty and makes the lease unusable.  Every failed attempt closes its
    socket and unregisters it from ``tunnel_writers``. Credentials and
    ``host:port`` are never logged — only non-reversible fingerprint hashes.
    """
    last_error: BaseException | None = None
    for _attempt in range(1 + LEASE_UPSTREAM_FAILOVER_ATTEMPTS):
        up_host, up_port, up_user, up_pass = lease.upstream
        srv_w = None
        try:
            # A failover is another provider-bound session, not a free retry.
            # Charge its signed request slot before the TCP dial performs I/O.
            _record_whoscored_provider_dial(lease)
            srv_r, srv_w = await asyncio.wait_for(
                _open_upstream_connection(up_host, up_port),
                _lease_operation_timeout(
                    lease,
                    ceiling_seconds=LEASE_UPSTREAM_CONNECT_TIMEOUT_SECONDS,
                ),
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
                timeout_seconds=_lease_operation_timeout(
                    lease,
                    ceiling_seconds=LEASE_PROVIDER_HEAD_TIMEOUT_SECONDS,
                ),
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
        # FBref must never spend a second paid CONNECT attempt. SofaScore's
        # separately bounded dead-exit policy remains response-byte based.
        failover_allowed = False if lease.source == "fbref" else lease.down_bytes == 0
        if (
            failover_allowed
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
        # Reject excess unauthenticated connections immediately.  asyncio's
        # semaphore acquisition does not yield while capacity is available, so
        # the locked check and acquire are atomic with respect to this event
        # loop; no waiter queue can itself become an attack surface.
        if _CLIENT_HEAD_SLOTS.locked():
            client_w.write(
                b"HTTP/1.1 503 Service Unavailable\r\nConnection: close\r\n\r\n"
            )
            await client_w.drain()
            client_w.close()
            return
        await _CLIENT_HEAD_SLOTS.acquire()
        try:
            try:
                first, raw_headers = await _read_client_request_head(client_r)
            except ClientHeadError as exc:
                await _reject_client_head(client_w, exc)
                return
        finally:
            _CLIENT_HEAD_SLOTS.release()
        if not first:
            client_w.close()
            return
        parts = first.decode("latin1").split()
        if len(parts) < 2:
            client_w.close()
            return
        method, target = parts[0].upper(), parts[1]
        control_path = urlsplit(target).path
        if control_path == "/health" or control_path.startswith("/v1/"):
            try:
                headers = _strict_control_header_map(raw_headers)
            except ValueError:
                await _send_json(
                    client_w, 400, {"error": "invalid control request headers"}
                )
                return
        else:
            headers = _header_map(raw_headers)
        if await _handle_control_delete_short(method, target, headers, client_w):
            return
        if await _handle_control(method, target, headers, client_r, client_w, mgr):
            return
        host, port = _proxy_target_host_port(method, target)

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
            and (
                lease.source == "sofascore"
                or (
                    lease.source == "whoscored"
                    and SOURCE_MODE == "whoscored-only"
                )
            )
            and not lease.current_endpoint
        ):
            # A signed allocation is necessary but not sufficient: production
            # traffic also needs an active endpoint boundary so every billed
            # byte has exact table provenance after a crash.
            client_w.write(b"HTTP/1.1 409 Conflict\r\n\r\n")
            await client_w.drain()
            client_w.close()
            return
        if not _lease_host_allowed(lease, host, port):
            blocked_count[host] += 1
            client_w.write(b"HTTP/1.1 403 Forbidden\r\n\r\n")
            await client_w.drain()
            client_w.close()
            return
        if lease is not None and lease.source == "whoscored" and method != "CONNECT":
            # A raw upstream HTTP proxy connection can carry additional
            # keep-alive/pipelined absolute-form requests after the validated
            # first head. Those bytes would bypass URL/host/request accounting.
            # WhoScored authority is HTTPS-only: require one CONNECT tunnel and
            # validate its plaintext ClientHello SNI before the provider dial.
            # Exact URL-path enforcement still belongs to the disabled app gateway.
            client_w.write(
                b"HTTP/1.1 405 Method Not Allowed\r\n"
                b"Allow: CONNECT\r\nConnection: close\r\n\r\n"
            )
            await client_w.drain()
            client_w.close()
            return
        # Explicit callers use the immutable lease upstream.  The no-lease
        # branch is reachable only when the process was deliberately started
        # with the development-only ``--allow-legacy-noauth`` flag.
        if lease is not None:
            up_host, up_port, up_user, up_pass = lease.upstream
            lease.active_tunnels += 1
            lease.tunnel_writers.add(client_w)
        else:
            up_host, up_port, up_user, up_pass = _acquire_upstream(mgr)
            _active += 1
        auth = base64.b64encode(f"{up_user}:{up_pass}".encode()).decode()
        srv_w = None
        local_connect_established = False
        buffered_client_hello = b""
        provider_connect_target = target
        try:
            if method == "CONNECT":
                conn_count[host] += 1
                if lease is not None and lease.source == "whoscored":
                    # A proxy client sends TLS only after receiving the local
                    # CONNECT success.  Accept locally, validate a bounded
                    # plaintext ClientHello, and only then spend a provider
                    # request. Invalid/missing/ECH SNI therefore costs zero.
                    if (
                        lease.pending_client_hellos
                        >= MAX_PENDING_WHOSCORED_CLIENT_HELLOS_PER_LEASE
                    ):
                        client_w.write(
                            b"HTTP/1.1 429 Too Many Requests\r\n"
                            b"Connection: close\r\n\r\n"
                        )
                        await client_w.drain()
                        client_w.close()
                        return
                    if _WHOSCORED_CLIENT_HELLO_SLOTS.locked():
                        client_w.write(
                            b"HTTP/1.1 503 Service Unavailable\r\n"
                            b"Connection: close\r\n\r\n"
                        )
                        await client_w.drain()
                        client_w.close()
                        return
                    hello_slot_acquired = False
                    lease_hello_slot_acquired = False
                    try:
                        try:
                            await _WHOSCORED_CLIENT_HELLO_SLOTS.acquire()
                        except asyncio.CancelledError:
                            try:
                                client_w.close()
                            except Exception:  # noqa: BLE001
                                pass
                            raise
                        hello_slot_acquired = True
                        # Recheck after acquisition so this remains strict if a
                        # future semaphore implementation ever yields on its
                        # uncontended fast path.
                        if (
                            lease.pending_client_hellos
                            >= MAX_PENDING_WHOSCORED_CLIENT_HELLOS_PER_LEASE
                        ):
                            client_w.write(
                                b"HTTP/1.1 429 Too Many Requests\r\n"
                                b"Connection: close\r\n\r\n"
                            )
                            await client_w.drain()
                            client_w.close()
                            return
                        lease.pending_client_hellos += 1
                        lease_hello_slot_acquired = True
                        client_w.write(b"HTTP/1.1 200 Connection established\r\n\r\n")
                        await asyncio.wait_for(
                            client_w.drain(),
                            timeout=_lease_operation_timeout(lease),
                        )
                        local_connect_established = True
                        buffered_client_hello = (
                            await _read_and_validate_whoscored_client_hello(
                                client_r,
                                lease,
                                connect_host=host,
                                connect_port=port,
                            )
                        )
                        # Never forward the caller's raw authority after parsing
                        # it: userinfo, trailing dots, or other parser differences
                        # must not make the provider connect anywhere except the
                        # normalized host whose SNI was just authenticated.
                        provider_connect_target = (
                            f"{_normalise_client_hello_hostname(host)}:443"
                        )
                    except asyncio.CancelledError:
                        try:
                            client_w.close()
                        except Exception:  # noqa: BLE001
                            pass
                        raise
                    except (
                        ClientHelloValidationError,
                        asyncio.TimeoutError,
                        TimeoutError,
                    ):
                        blocked_count[host] += 1
                        client_w.close()
                        return
                    finally:
                        if lease_hello_slot_acquired:
                            lease.pending_client_hellos -= 1
                        if hello_slot_acquired:
                            _WHOSCORED_CLIENT_HELLO_SLOTS.release()
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
                            lease,
                            mgr,
                            target=provider_connect_target,
                            host=host,
                        )
                    except _LeaseBudgetRefused:
                        if not local_connect_established:
                            client_w.write(b"HTTP/1.1 429 Too Many Requests\r\n\r\n")
                            await client_w.drain()
                        else:
                            client_w.close()
                        return
                    except ProxyCampaignError:
                        if not local_connect_established:
                            client_w.write(b"HTTP/1.1 429 Too Many Requests\r\n\r\n")
                            await client_w.drain()
                        else:
                            client_w.close()
                        return
                    except (
                        asyncio.TimeoutError,
                        TimeoutError,
                        OSError,
                        UpstreamHeadTimeout,
                        UpstreamHeadIncomplete,
                    ):
                        # Timeout/accounting-uncertainty paths already revoke;
                        # a proven empty EOF/reset remains a normal 502 after
                        # bounded failover attempts. The finally drains the slot.
                        if not local_connect_established:
                            client_w.write(b"HTTP/1.1 502 Bad Gateway\r\n\r\n")
                            await client_w.drain()
                        client_w.close()
                        return
                else:
                    srv_r, srv_w = await _open_upstream_connection(up_host, up_port)
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
                if _provider_connect_status_code(status) != 200:
                    if lease is not None:
                        try:
                            # For other leases the HTTP tunnel is not established
                            # yet, so queue the local error before the uncertainty
                            # latch closes every writer. WhoScored already sent
                            # its local 200 to obtain SNI and can only close.
                            if not local_connect_established:
                                client_w.write(b"HTTP/1.1 502 Bad Gateway\r\n\r\n")
                        finally:
                            # A complete non-200/invalid head can already have a
                            # response body in StreamReader read-ahead.  Retain
                            # the whole remaining escrow rather than discarding
                            # those unobservable provider bytes on close.
                            _latch_lease_accounting_uncertainty(lease)
                        return
                    client_w.write(b"HTTP/1.1 502 Bad Gateway\r\n\r\n")
                    await client_w.drain()
                    client_w.close()
                    srv_w.close()
                    return
                if not local_connect_established:
                    try:
                        client_w.write(b"HTTP/1.1 200 Connection established\r\n\r\n")
                        if lease is None:
                            await client_w.drain()
                        else:
                            await asyncio.wait_for(
                                client_w.drain(),
                                timeout=_lease_operation_timeout(lease),
                            )
                    except BaseException:
                        if lease is not None:
                            # The provider reader may hold tunnel bytes beyond its
                            # accounted response head, but no down-pump owns it yet.
                            _latch_lease_accounting_uncertainty(lease)
                        raise
                if buffered_client_hello and not await _write_upstream(
                    srv_w,
                    buffered_client_hello,
                    lease=lease,
                    host=host,
                    direction="up",
                ):
                    # Provider CONNECT already succeeded. Retain the remaining
                    # escrow because its reader has no pump owning read-ahead.
                    _latch_lease_accounting_uncertainty(lease)
                    client_w.close()
                    return
                await _run_tunnel_pumps(
                    client_r,
                    client_w,
                    srv_r,
                    srv_w,
                    host,
                    lease=lease,
                )
            else:
                conn_count[host] += 1
                try:
                    if lease is not None:
                        _record_whoscored_provider_dial(lease)
                    srv_r, srv_w = await asyncio.wait_for(
                        _open_upstream_connection(up_host, up_port),
                        (
                            LEASE_UPSTREAM_CONNECT_TIMEOUT_SECONDS
                            if lease is None
                            else _lease_operation_timeout(
                                lease,
                                ceiling_seconds=LEASE_UPSTREAM_CONNECT_TIMEOUT_SECONDS,
                            )
                        ),
                    )
                except ProxyCampaignError:
                    client_w.write(b"HTTP/1.1 429 Too Many Requests\r\n\r\n")
                    await client_w.drain()
                    client_w.close()
                    return
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
                await _run_tunnel_pumps(
                    client_r,
                    client_w,
                    srv_r,
                    srv_w,
                    host,
                    lease=lease,
                )
        finally:
            if lease is not None:
                lease.tunnel_writers.discard(client_w)
                if srv_w is not None:
                    lease.tunnel_writers.discard(srv_w)
                lease.active_tunnels = max(0, lease.active_tunnels - 1)
            else:
                _active -= 1
    except asyncio.CancelledError:
        # Cancellation is control flow, not a proxy error. Close the local leg
        # explicitly, but preserve cancellation for server shutdown semantics.
        try:
            client_w.close()
        except Exception:  # noqa: BLE001
            pass
        raise
    except Exception:  # noqa: BLE001
        try:
            client_w.close()
        except Exception:  # noqa: BLE001
            pass


def _dump(out_path: str, quiet: bool = False) -> None:
    _prune_finalized_leases()
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
    if SOURCE_MODE == "whoscored-only":
        report = _signed_report(report)
        _atomic_private_bytes(
            out_path,
            canonical_json_bytes(report) + b"\n",
            replace=True,
        )
        if not quiet:
            log.info("wrote protected WhoScored state report")
        return
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
    global TRANSFERMARKT_CONTROL_TOKEN
    global WHOSCORED_PROXY_APPROVAL_HMAC_SECRET
    global WHOSCORED_PROXY_LEDGER_HMAC_SECRET
    global LEASE_UPSTREAM_CONNECT_TIMEOUT_SECONDS, LEASE_PROVIDER_HEAD_TIMEOUT_SECONDS
    global LEASE_UPSTREAM_FAILOVER_ATTEMPTS
    global SOFASCORE_ALLOCATION_LEDGER_PATH, SOFASCORE_ALLOCATION_WAL_PATH
    global SOFASCORE_ALLOCATION_LEDGER, _SOFASCORE_ALLOCATION_LEDGER_KEY
    global SOFASCORE_PARENT_ENVELOPE_PATH, SOFASCORE_PARENT_ENVELOPE_LEDGER
    global _SOFASCORE_PARENT_ENVELOPE_LEDGER_PATH
    global WHOSCORED_CAMPAIGN_LEDGER_PATH, WHOSCORED_CAMPAIGN_LEDGER
    global _WHOSCORED_CAMPAIGN_LEDGER_KEY
    global WHOSCORED_PROXY_RUNTIME_SHA256
    global WHOSCORED_STATE_MARKER_PATH, WHOSCORED_STATE_ID
    global WHOSCORED_PROVIDER_ORDER_ID, WHOSCORED_PROVIDER_POLICY_SHA256
    global WHOSCORED_LEGACY_STATE_MARKER_LOADED
    global _PAID_LEDGER_CHAIN_COUNT, _PAID_LEDGER_CHAIN_OFFSET
    global _PAID_LEDGER_CHAIN_TAIL
    global SOURCE_MODE
    global _daily_day, _daily_up_bytes, _daily_down_bytes
    global provider_budget_guard, provider_budget_endpoint
    ap = argparse.ArgumentParser()
    ap.add_argument("--listen", default="0.0.0.0:8899")
    ap.add_argument(
        "--source-mode",
        choices=("shared-no-whoscored", "whoscored-only"),
        default="shared-no-whoscored",
    )
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
        "--allow-legacy-noauth",
        action="store_true",
        help=(
            "development-only credential-less proxy data plane on --listen; "
            "production defaults to lease authentication"
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
        "--daily-budget-bytes",
        type=int,
        default=(
            int(os.environ["WHOSCORED_PROXY_FILTER_DAILY_BUDGET_BYTES"])
            if os.environ.get("WHOSCORED_PROXY_FILTER_DAILY_BUDGET_BYTES")
            else None
        ),
        help=(
            "exact decimal-byte outer cap; required and authoritative in "
            "whoscored-only mode"
        ),
    )
    ap.add_argument(
        "--max-lease-bytes",
        type=int,
        default=(
            int(os.environ["WHOSCORED_PROXY_FILTER_MAX_LEASE_BYTES"])
            if os.environ.get("WHOSCORED_PROXY_FILTER_MAX_LEASE_BYTES")
            else None
        ),
        help=(
            "exact decimal-byte lease cap; required and authoritative in "
            "whoscored-only mode"
        ),
    )
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
        "--whoscored-campaign-ledger",
        default=os.environ.get(
            "WHOSCORED_PROXY_CAMPAIGN_LEDGER_PATH",
            WHOSCORED_CAMPAIGN_LEDGER_PATH,
        ),
        help="HMAC-protected WhoScored campaign/allocation state",
    )
    ap.add_argument(
        "--whoscored-state-marker",
        default=os.environ.get(
            "WHOSCORED_PROXY_STATE_MARKER_PATH",
            WHOSCORED_STATE_MARKER_PATH,
        ),
        help="filter-only authenticated state initialization marker",
    )
    ap.add_argument(
        "--whoscored-provider-order-id",
        default=os.environ.get("WHOSCORED_PROVIDER_ORDER_ID", ""),
        help="exact provider order owning the protected WhoScored state",
    )
    ap.add_argument(
        "--whoscored-provider-policy-sha256",
        default=os.environ.get("WHOSCORED_PROVIDER_POLICY_SHA256", ""),
        help="exact signed provider-policy digest owning the protected state",
    )
    ap.add_argument(
        "--allow-legacy-whoscored-state-marker-v1",
        action="store_true",
        help=(
            "explicit manual/schema-v2 compatibility for a legacy unbound marker; "
            "scheduled schema-v3 approvals remain forbidden"
        ),
    )
    ap.add_argument(
        "--initialize-whoscored-state",
        action="store_true",
        help="one-shot creation of a new empty protected WhoScored state",
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

    if str(args.source_mode) == "whoscored-only" and bool(
        getattr(args, "allow_legacy_noauth", False)
    ):
        # Reject the unsafe combination before runtime validation, protected
        # state initialization, pool loading, or any listener/network effect.
        # Shared legacy sources retain their explicitly requested compatibility.
        raise SystemExit(
            "--allow-legacy-noauth is forbidden with --source-mode=whoscored-only"
        )

    if str(args.source_mode) == "whoscored-only":
        try:
            _WHOSCORED_RUNTIME_CONTRACT.require_production_runtime_class(
                operation="WhoScored-only filtering proxy",
            )
        except Exception as exc:
            raise SystemExit(
                f"WhoScored-only filtering proxy runtime class rejected: {exc}"
            ) from exc

    CONTROL_TOKEN = os.environ.get("PROXY_FILTER_CONTROL_TOKEN", "")
    if len(CONTROL_TOKEN) < 32:
        raise SystemExit(
            "PROXY_FILTER_CONTROL_TOKEN must contain at least 32 characters"
        )
    TRANSFERMARKT_CONTROL_TOKEN = str(
        os.environ.get("TM_PROXY_CONTROL_TOKEN", "")
    ).strip()
    WHOSCORED_PROXY_APPROVAL_HMAC_SECRET = str(
        os.environ.get("WHOSCORED_PROXY_APPROVAL_HMAC_SECRET", "")
    ).strip()
    WHOSCORED_PROXY_LEDGER_HMAC_SECRET = str(
        os.environ.get("WHOSCORED_PROXY_LEDGER_HMAC_SECRET", "")
    ).strip()
    if str(args.source_mode) == "whoscored-only":
        if len(WHOSCORED_PROXY_APPROVAL_HMAC_SECRET.encode("utf-8")) < 32:
            raise SystemExit(
                "WHOSCORED_PROXY_APPROVAL_HMAC_SECRET must contain at least "
                "32 bytes for the WhoScored filter"
            )
        if len(WHOSCORED_PROXY_LEDGER_HMAC_SECRET.encode("utf-8")) < 32:
            raise SystemExit(
                "WHOSCORED_PROXY_LEDGER_HMAC_SECRET must contain at least "
                "32 bytes for the WhoScored filter"
            )
    try:
        runtime = _WHOSCORED_RUNTIME_CONTRACT.validate_runtime_contract(
            report_schema_version=3
        )
    except Exception as exc:
        raise SystemExit(
            f"WhoScored proxy runtime contract validation failed: {exc}"
        ) from exc
    WHOSCORED_PROXY_RUNTIME_SHA256 = str(runtime["code_tree_sha256"])
    SOURCE_MODE = str(args.source_mode)

    daily_budget_mb = float(getattr(args, "daily_budget_mb", 100.0))
    max_lease_mb = float(getattr(args, "max_lease_mb", 24.0))
    exact_daily_budget_bytes = getattr(args, "daily_budget_bytes", None)
    exact_max_lease_bytes = getattr(args, "max_lease_bytes", None)
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
    sofascore_artifact = getattr(args, "sofascore_budget_artifact", None)
    if SOURCE_MODE == "whoscored-only" and (
        isinstance(exact_daily_budget_bytes, bool)
        or not isinstance(exact_daily_budget_bytes, int)
        or exact_daily_budget_bytes <= 0
        or isinstance(exact_max_lease_bytes, bool)
        or not isinstance(exact_max_lease_bytes, int)
        or exact_max_lease_bytes <= 0
        or exact_daily_budget_bytes > WHOSCORED_PROVIDER_ORDER_SAFETY_CAP_BYTES
        or exact_max_lease_bytes > WHOSCORED_MAX_LEASE_SAFETY_CAP_BYTES
    ):
        raise SystemExit(
            "whoscored-only exact byte caps are missing or exceed the protected "
            "provider/lease ceilings"
        )
    if SOURCE_MODE == "whoscored-only" and (
        transfermarkt_budget_bytes != 0
        or sofascore_canary_hard_cap_bytes != 0
        or sofascore_discovery_budget_bytes != 0
        or sofascore_artifact
    ):
        raise SystemExit(
            "whoscored-only rejects every cross-source paid budget or artifact"
        )
    if (
        (SOURCE_MODE != "whoscored-only" and daily_budget_mb <= 0)
        or (SOURCE_MODE != "whoscored-only" and max_lease_mb <= 0)
        or max_lease_ttl_seconds <= 0
        or dagrun_budget_bytes <= 0
        or transfermarkt_budget_bytes < 0
        or url_budget_bytes <= 0
        or max_active_leases <= 0
        or sofascore_canary_hard_cap_bytes < 0
        or sofascore_discovery_budget_bytes < 0
    ):
        raise SystemExit(
            "proxy byte budgets must be positive; disabled source caps may be zero"
        )
    if transfermarkt_budget_bytes > 0 and len(TRANSFERMARKT_CONTROL_TOKEN) < 32:
        raise SystemExit(
            "TM_PROXY_CONTROL_TOKEN must contain at least 32 characters when "
            "Transfermarkt paid proxying is enabled"
        )
    if TRANSFERMARKT_CONTROL_TOKEN and len(TRANSFERMARKT_CONTROL_TOKEN) < 32:
        raise SystemExit(
            "TM_PROXY_CONTROL_TOKEN must be empty or contain at least 32 characters"
        )
    # ``inf``/``nan`` pass a bare ``<= 0`` check but would disable the dead-exit
    # bound entirely, so the timeouts require strictly finite positive values.
    if (
        not (
            math.isfinite(lease_connect_timeout_seconds)
            and lease_connect_timeout_seconds > 0
        )
        or not (
            math.isfinite(lease_head_timeout_seconds) and lease_head_timeout_seconds > 0
        )
        or lease_failover_attempts < 0
    ):
        raise SystemExit(
            "lease upstream timeouts must be finite positive seconds and "
            "failover attempts must be >= 0"
        )
    DAILY_BUDGET_BYTES = (
        int(exact_daily_budget_bytes)
        if SOURCE_MODE == "whoscored-only"
        else int(daily_budget_mb * 1024 * 1024)
    )
    MAX_LEASE_BYTES = (
        int(exact_max_lease_bytes)
        if SOURCE_MODE == "whoscored-only"
        else int(max_lease_mb * 1024 * 1024)
    )
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
    WHOSCORED_CAMPAIGN_LEDGER_PATH = str(
        getattr(
            args,
            "whoscored_campaign_ledger",
            WHOSCORED_CAMPAIGN_LEDGER_PATH,
        )
    )
    WHOSCORED_CAMPAIGN_LEDGER = None
    _WHOSCORED_CAMPAIGN_LEDGER_KEY = None
    WHOSCORED_STATE_MARKER_PATH = str(
        getattr(args, "whoscored_state_marker", WHOSCORED_STATE_MARKER_PATH)
    )
    WHOSCORED_PROVIDER_ORDER_ID = str(
        getattr(args, "whoscored_provider_order_id", "") or ""
    ).strip()
    WHOSCORED_PROVIDER_POLICY_SHA256 = str(
        getattr(args, "whoscored_provider_policy_sha256", "") or ""
    ).strip()
    WHOSCORED_STATE_ID = ""
    WHOSCORED_LEGACY_STATE_MARKER_LOADED = False
    _PAID_LEDGER_CHAIN_COUNT = 0
    _PAID_LEDGER_CHAIN_OFFSET = 0
    _PAID_LEDGER_CHAIN_TAIL = ""
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
    initialize_state = bool(getattr(args, "initialize_whoscored_state", False))
    allow_legacy_state_marker = bool(
        getattr(args, "allow_legacy_whoscored_state_marker_v1", False)
    )
    if SOURCE_MODE != "whoscored-only" and initialize_state:
        raise SystemExit(
            "--initialize-whoscored-state requires --source-mode=whoscored-only"
        )
    if SOURCE_MODE != "whoscored-only" and allow_legacy_state_marker:
        raise SystemExit(
            "--allow-legacy-whoscored-state-marker-v1 requires "
            "--source-mode=whoscored-only"
        )
    if SOURCE_MODE == "whoscored-only":
        try:
            if initialize_state and allow_legacy_state_marker:
                raise RuntimeError(
                    "new WhoScored state cannot use a legacy unbound marker"
                )
            if not allow_legacy_state_marker:
                _whoscored_state_binding()
            if initialize_state:
                _initialize_whoscored_state(out_path)
                log.info("initialized protected WhoScored filter state")
                return
            _verify_whoscored_state(
                out_path, allow_legacy_marker=allow_legacy_state_marker
            )
        except (OSError, RuntimeError, ProxyCampaignError) as exc:
            raise SystemExit(f"WhoScored protected state rejected: {exc}") from None
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
    allow_legacy_noauth = bool(getattr(args, "allow_legacy_noauth", False))
    log.info(
        "residential pool = %d proxies from %s (legacy no-auth=%s)",
        mgr.total_count,
        pool_source,
        "enabled" if allow_legacy_noauth else "disabled",
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

    server = await asyncio.start_server(
        lambda r, w: handle(
            r,
            w,
            mgr,
            require_lease=not allow_legacy_noauth,
        ),
        host,
        int(port),
    )
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
        "listening on %s (lease API + authenticated proxy; legacy no-auth=%s)",
        listen,
        "enabled" if allow_legacy_noauth else "disabled",
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
