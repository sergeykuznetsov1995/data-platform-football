"""Unit tests for scripts/proxy_filter/filter_proxy.py (#652).

``filter_proxy`` is a standalone script (not a package). Its only container-only
import (``scrapers.utils.proxy_manager``) is lazy — inside ``_residential`` — so the
module loads on the host with no stubbing and no network.

What we cover (the safety-critical, pure logic):
  - ``_is_blocked``: dot-boundary suffix matching, case-insensitivity, and the
    invariant that the Cloudflare challenge + target sites are NEVER blocked.
  - ``_load_blocklist``: comment/blank stripping, lowercasing, None -> empty.
  - ``_dump``: report shape (total_mb / allowed_hosts / blocked_hosts).
  - the SHIPPED ``configs/proxy_filter/blocklist.txt`` does not footgun CF/the sites.
"""

from __future__ import annotations

import importlib.util
import asyncio
import base64
import hashlib
import json
import time
from collections import defaultdict, deque
from datetime import datetime, timedelta, timezone
from pathlib import Path
from types import SimpleNamespace

import pytest

from scrapers.sofascore.workload_plan import (
    WorkloadAllocation,
    _signed_plan,
    match_workload_class,
    player_workload_class,
)
from scrapers.whoscored.proxy_campaign import (
    PROXY_CAMPAIGN_METER,
    WHOSCORED_CANARY_ALLOWED_PATH_FAMILIES,
    WHOSCORED_CANARY_CAP_BYTES,
    WHOSCORED_CANARY_CAPTURE_ALLOCATION_ID,
    WHOSCORED_CANARY_CAPTURE_CAP_BYTES,
    WHOSCORED_CANARY_CAPTURE_WORK_ITEM_ID,
    WHOSCORED_CANARY_DAG_ID,
    WHOSCORED_CANARY_DISCOVERY_ALLOCATION_ID,
    WHOSCORED_CANARY_DISCOVERY_CAP_BYTES,
    WHOSCORED_CANARY_DISCOVERY_PATH_FAMILIES,
    WHOSCORED_CANARY_DISCOVERY_WORK_ITEM_ID,
    WHOSCORED_CANARY_TASK_ID,
    ProxyCampaignApproval,
    ProxyCampaignBudgetExceeded,
    ProxyCampaignError,
    ProxyCampaignLedger,
    ProxyCampaignValidationError,
    deterministic_proxy_attempt_id,
    sign_proxy_campaign_approval,
    whoscored_canary_run_id,
)

# The class names are now derived from the measured production workload shape
# rather than hard-coded, but every assertion below still means "the class this
# deployment actually signs".
MATCH_WORKLOAD_CLASS = match_workload_class()
PLAYER_WORKLOAD_CLASS = player_workload_class()

REPO_ROOT = Path(__file__).resolve().parents[3]
_SCRIPT_PATH = REPO_ROOT / "scripts" / "proxy_filter" / "filter_proxy.py"
_BLOCKLIST_PATH = REPO_ROOT / "configs" / "proxy_filter" / "blocklist.txt"
_COMPOSE_PATH = REPO_ROOT / "compose.yaml"
# #951 (инцидент 2026-07-17): выделенный SofaScore-шлюз вынесен в СВОЙ
# compose-проект, чтобы чужой `docker compose up` его не пересоздавал.
_SOFASCORE_GATEWAY_COMPOSE_PATH = (
    REPO_ROOT / "deploy/sofascore/gateway.compose.yaml"
)
_FBREF_ACCEPTANCE_COMPOSE_PATH = (
    REPO_ROOT / "deploy/fbref/acceptance.compose.yaml"
)
_ENV_EXAMPLE_PATH = REPO_ROOT / ".env.example"
_SCHEDULER_DOCKERFILE_PATH = (
    # master consolidated the scheduler runtime overlay into the single
    # multi-stage Dockerfile (target airflow-scheduler); the pinned Windows
    # fontconfig assertion lives there now.
    REPO_ROOT / "docker/images/airflow/Dockerfile"
)
_ACCEPTANCE_DOCKERFILE_PATH = (
    REPO_ROOT / "docker/images/airflow/Dockerfile.fbref-acceptance"
)
_ACCEPTANCE_BUILD_SCRIPT_PATH = (
    REPO_ROOT / "scripts/build_fbref_acceptance_image.sh"
)


def _load_module():
    spec = importlib.util.spec_from_file_location("filter_proxy", _SCRIPT_PATH)
    assert spec is not None and spec.loader is not None, f"cannot load {_SCRIPT_PATH}"
    mod = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(mod)
    return mod


@pytest.fixture()
def mod(tmp_path):
    loaded = _load_module()
    loaded.LEDGER_PATH = str(tmp_path / "paid_requests.jsonl")
    loaded.CONTROL_TOKEN = "c" * 32
    loaded.TRANSFERMARKT_CONTROL_TOKEN = "t" * 32
    loaded.WHOSCORED_PROXY_APPROVAL_HMAC_SECRET = "c" * 32
    loaded.WHOSCORED_PROXY_LEDGER_HMAC_SECRET = "c" * 32
    loaded.SOFASCORE_BUDGET_ARTIFACT_ID = "a" * 64
    loaded.SOFASCORE_ALLOCATION_LEDGER_PATH = str(tmp_path / "allocations.json")
    loaded.SOFASCORE_ALLOCATION_WAL_PATH = str(tmp_path / "allocation-wal.jsonl")
    loaded.SOFASCORE_ALLOCATION_LEDGER = None
    loaded._SOFASCORE_ALLOCATION_LEDGER_KEY = None
    loaded.SOFASCORE_PARENT_ENVELOPE_PATH = str(tmp_path / "parent-envelopes.json")
    loaded.SOFASCORE_PARENT_ENVELOPE_LEDGER = None
    loaded._SOFASCORE_PARENT_ENVELOPE_LEDGER_PATH = ""
    loaded.WHOSCORED_CAMPAIGN_LEDGER_PATH = str(tmp_path / "whoscored-campaigns.json")
    loaded.WHOSCORED_CAMPAIGN_LEDGER = None
    loaded._WHOSCORED_CAMPAIGN_LEDGER_KEY = None
    loaded.WHOSCORED_PROXY_RUNTIME_SHA256 = "a" * 64
    loaded.WHOSCORED_PROVIDER_ORDER_ID = "proxysio-order-38950"
    loaded.WHOSCORED_PROVIDER_POLICY_SHA256 = "b" * 64
    loaded.WHOSCORED_LEGACY_STATE_MARKER_LOADED = False
    # Individual tests cover each production mode; the general accounting
    # fixture exercises both source families without a service boundary.
    loaded.SOURCE_MODE = "test-all"
    # Unit tests below exercise the internal accounting state machine behind an
    # already-proven provider guard. Production keeps this code-owned gate false.
    loaded.WHOSCORED_PROVIDER_INVOICE_HARD_CAP_AVAILABLE = True
    loaded.WHOSCORED_PAID_APPLICATION_GATEWAY_AVAILABLE = True
    loaded.WHOSCORED_FULL_PAID_CRAWL_AVAILABLE = True
    return loaded


# --- _is_blocked --------------------------------------------------------------


def test_blocks_exact_domain(mod):
    # Arrange
    mod.BLOCKLIST = {"doubleclick.net"}
    # Act / Assert
    assert mod._is_blocked("doubleclick.net") is True


def test_blocks_subdomain_via_dot_suffix(mod):
    # Arrange
    mod.BLOCKLIST = {"doubleclick.net"}
    # Act / Assert — a real ad subdomain must be caught by the suffix rule
    assert mod._is_blocked("securepubads.g.doubleclick.net") is True


def test_does_not_block_lookalike_without_dot_boundary(mod):
    # Arrange — "notdoubleclick.net" merely ends with the string, not ".doubleclick.net"
    mod.BLOCKLIST = {"doubleclick.net"}
    # Act / Assert
    assert mod._is_blocked("notdoubleclick.net") is False


def test_matching_is_case_insensitive(mod):
    # Arrange
    mod.BLOCKLIST = {"doubleclick.net"}
    # Act / Assert
    assert mod._is_blocked("SecurePubAds.G.DoubleClick.NET") is True


@pytest.mark.parametrize(
    "host",
    [
        "challenges.cloudflare.com",  # CF Turnstile — blocking it breaks the bypass
        "sofifa.com",
        "cdn.sofifa.net",
        "www.whoscored.com",
        "cdn.whoscored.com",
    ],
)
def test_never_blocks_cf_or_target_sites(mod, host):
    # Arrange — even with a broad ad blocklist, these must always pass
    mod.BLOCKLIST = {"doubleclick.net", "googletagmanager.com", "adnxs.com"}
    # Act / Assert
    assert mod._is_blocked(host) is False


def test_empty_blocklist_blocks_nothing(mod):
    # Arrange — observe mode: no blocklist
    mod.BLOCKLIST = set()
    # Act / Assert
    assert mod._is_blocked("securepubads.g.doubleclick.net") is False


@pytest.mark.parametrize(
    "host",
    sorted(
        {
            "www.whoscored.com",
            "cdn.whoscored.com",
            "challenges.cloudflare.com",
            "turnstile.cloudflare.com",
        }
    ),
)
def test_whoscored_paid_lease_allows_only_exact_signed_hosts(mod, host):
    lease = SimpleNamespace(source="whoscored")
    assert mod._lease_host_allowed(lease, host)
    assert mod._lease_host_allowed(lease, host.upper() + ".")


@pytest.mark.parametrize(
    "host",
    [
        "whoscored.com",
        "evil.whoscored.com",
        "www.whoscored.com.evil.test",
        "sub.challenges.cloudflare.com",
        "api.ipify.org",
    ],
)
def test_whoscored_paid_lease_rejects_host_before_provider_dial(mod, host):
    assert not mod._lease_host_allowed(SimpleNamespace(source="whoscored"), host)


@pytest.mark.parametrize(
    "host",
    ["www.transfermarkt.com", "www.transfermarkt.us"],
)
def test_transfermarkt_paid_lease_has_an_exact_https_host_scope(mod, host):
    lease = SimpleNamespace(source="transfermarkt")
    assert mod._lease_host_allowed(lease, host, 443)
    assert not mod._lease_host_allowed(lease, host, 80)
    assert not mod._lease_host_allowed(lease, f"evil.{host}", 443)


def test_unknown_paid_lease_source_has_no_host_scope(mod):
    assert not mod._lease_host_allowed(
        SimpleNamespace(source="caller_supplied"), "evil.example", 443
    )


def test_all_whoscored_paid_dags_are_recognized_but_default_to_zero(mod):
    for dag_id in (
        "dag_ingest_whoscored",
        "dag_backfill_whoscored",
        "dag_canary_whoscored_proxy",
    ):
        assert mod._source_for_dag(dag_id) == "whoscored"
        assert mod._dagrun_budget_bytes(dag_id) == 0


def test_whoscored_paid_canary_boundaries_are_code_owned_and_available():
    loaded = _load_module()
    assert loaded.WHOSCORED_PROVIDER_INVOICE_HARD_CAP_AVAILABLE is True
    assert loaded.WHOSCORED_PAID_APPLICATION_GATEWAY_AVAILABLE is True
    assert loaded.WHOSCORED_FULL_PAID_CRAWL_AVAILABLE is False
    # Daily ingest has its own code-owned gate; backfill stays behind full crawl.
    assert loaded.daily_ingest_paid_crawl_allowed("dag_ingest_whoscored") is True
    assert loaded.daily_ingest_paid_crawl_allowed("dag_backfill_whoscored") is False


def _whoscored_campaign_context(
    mod,
    *,
    cap: int = 1_000,
    canonical_url: str = "https://www.whoscored.com/Matches/1/Live",
    attempt_id: str | None = None,
    dag_id: str = "dag_backfill_whoscored",
    requests: int = 10,
    leases: int = 1,
    approval_id: str = "approval-one",
    campaign_id: str = "campaign-one",
    run_id: str = "manual__campaign-one",
) -> dict[str, object]:
    now = datetime.now(timezone.utc)
    allocation = {
        "allocation_id": "allocation-one",
        "phase": "capture",
        "workload_class": "measurement_cohort",
        "work_item_id": "cohort-one",
        "task_id": "run_whoscored_proxy_canary",
        "budget_bytes": cap,
        "request_limit": requests,
        "lease_limit": leases,
        "allowed_path_families": ["/Matches"],
    }
    unsigned = {
        "schema_version": 2,
        "source": "whoscored",
        "approval_id": approval_id,
        "campaign_id": campaign_id,
        "run_id": run_id,
        "issued_at": (now - timedelta(minutes=1)).isoformat(),
        "expires_at": (now + timedelta(hours=1)).isoformat(),
        "transport_policy": "direct_then_paid",
        "runtime_sha256": "a" * 64,
        "classifier_sha256": "b" * 64,
        "caps": {
            "total_provider_bytes": cap,
            "discovery_provider_bytes": 0,
            "capture_provider_bytes": cap,
            "daily_provider_bytes": cap,
        },
        "limits": {"requests": requests, "leases": leases, "concurrency": 1},
        "allowed_dag_ids": [dag_id],
        "allowed_hosts": sorted(mod.WHOSCORED_PROXY_ALLOWED_HOSTS),
        "allowed_path_families": ["/Matches"],
        "allocations": [allocation],
        "meter": PROXY_CAMPAIGN_METER,
        "signature_algorithm": "hmac-sha256",
    }
    approval = ProxyCampaignApproval.from_dict(
        sign_proxy_campaign_approval(unsigned, mod.WHOSCORED_PROXY_APPROVAL_HMAC_SECRET)
    )
    resolved_attempt_id = attempt_id or deterministic_proxy_attempt_id(
        dag_id=dag_id,
        run_id=run_id,
        task_id="run_whoscored_proxy_canary",
        map_index=-1,
        try_number=1,
    )
    return {
        "dag_id": dag_id,
        "run_id": run_id,
        "task_id": "run_whoscored_proxy_canary",
        "map_index": -1,
        "try_number": 1,
        "canonical_url": canonical_url,
        "source": "whoscored",
        "transport_policy": "direct_then_paid",
        "proxy_campaign_approval": approval.to_dict(),
        "proxy_campaign_id": approval.campaign_id,
        "proxy_approval_id": approval.approval_id,
        "proxy_approval_sha256": approval.approval_sha256,
        "proxy_allocation": allocation,
        "proxy_allocation_id": allocation["allocation_id"],
        "proxy_work_item_id": allocation["work_item_id"],
        "proxy_attempt_id": resolved_attempt_id,
    }


def _exact_whoscored_canary_context(mod) -> dict[str, object]:
    now = datetime.now(timezone.utc)
    campaign_id = "exact-filter-canary"
    run_id = whoscored_canary_run_id(campaign_id)
    allocations = [
        {
            "allocation_id": WHOSCORED_CANARY_DISCOVERY_ALLOCATION_ID,
            "phase": "discovery",
            "workload_class": "catalog_discovery",
            "work_item_id": WHOSCORED_CANARY_DISCOVERY_WORK_ITEM_ID,
            "task_id": WHOSCORED_CANARY_TASK_ID,
            "budget_bytes": WHOSCORED_CANARY_DISCOVERY_CAP_BYTES,
            "request_limit": 1,
            "lease_limit": 1,
            "allowed_path_families": list(WHOSCORED_CANARY_DISCOVERY_PATH_FAMILIES),
        },
        {
            "allocation_id": WHOSCORED_CANARY_CAPTURE_ALLOCATION_ID,
            "phase": "capture",
            "workload_class": "representative_cohort",
            "work_item_id": WHOSCORED_CANARY_CAPTURE_WORK_ITEM_ID,
            "task_id": WHOSCORED_CANARY_TASK_ID,
            "budget_bytes": WHOSCORED_CANARY_CAPTURE_CAP_BYTES,
            "request_limit": 2,
            "lease_limit": 2,
            "allowed_path_families": list(WHOSCORED_CANARY_ALLOWED_PATH_FAMILIES),
        },
    ]
    unsigned = {
        "schema_version": 2,
        "source": "whoscored",
        "approval_id": "approval-exact-filter-canary",
        "campaign_id": campaign_id,
        "run_id": run_id,
        "issued_at": (now - timedelta(minutes=1)).isoformat(),
        "expires_at": (now + timedelta(hours=1)).isoformat(),
        "transport_policy": "direct_then_paid",
        "runtime_sha256": "a" * 64,
        "classifier_sha256": "b" * 64,
        "caps": {
            "total_provider_bytes": WHOSCORED_CANARY_CAP_BYTES,
            "discovery_provider_bytes": WHOSCORED_CANARY_DISCOVERY_CAP_BYTES,
            "capture_provider_bytes": WHOSCORED_CANARY_CAPTURE_CAP_BYTES,
            "daily_provider_bytes": WHOSCORED_CANARY_CAP_BYTES,
        },
        "limits": {"requests": 3, "leases": 3, "concurrency": 1},
        "allowed_dag_ids": [WHOSCORED_CANARY_DAG_ID],
        "allowed_hosts": sorted(mod.WHOSCORED_PROXY_ALLOWED_HOSTS),
        "allowed_path_families": list(WHOSCORED_CANARY_ALLOWED_PATH_FAMILIES),
        "allocations": allocations,
        "meter": PROXY_CAMPAIGN_METER,
        "signature_algorithm": "hmac-sha256",
    }
    approval = ProxyCampaignApproval.from_dict(
        sign_proxy_campaign_approval(unsigned, mod.WHOSCORED_PROXY_APPROVAL_HMAC_SECRET)
    )
    allocation = allocations[1]
    attempt_id = deterministic_proxy_attempt_id(
        dag_id=WHOSCORED_CANARY_DAG_ID,
        run_id=run_id,
        task_id=WHOSCORED_CANARY_TASK_ID,
        map_index=-1,
        try_number=1,
    )
    return {
        "dag_id": WHOSCORED_CANARY_DAG_ID,
        "run_id": run_id,
        "task_id": WHOSCORED_CANARY_TASK_ID,
        "map_index": -1,
        "try_number": 1,
        "canonical_url": "https://www.whoscored.com/Matches/1/Live",
        "source": "whoscored",
        "transport_policy": "direct_then_paid",
        "proxy_campaign_approval": approval.to_dict(),
        "proxy_campaign_id": approval.campaign_id,
        "proxy_approval_id": approval.approval_id,
        "proxy_approval_sha256": approval.approval_sha256,
        "proxy_allocation": allocation,
        "proxy_allocation_id": allocation["allocation_id"],
        "proxy_work_item_id": allocation["work_item_id"],
        "proxy_attempt_id": attempt_id,
    }


@pytest.mark.parametrize("wrong_field", ["cap", "path", "allocation"])
def test_signed_non_exact_canary_is_rejected_before_provider_selection(
    mod, wrong_field
):
    context = _exact_whoscored_canary_context(mod)
    exact = ProxyCampaignApproval.from_dict(context["proxy_campaign_approval"])
    unsigned = exact.unsigned_dict()
    capture = unsigned["allocations"][1]
    if wrong_field == "cap":
        unsigned["caps"]["total_provider_bytes"] -= 1
        unsigned["caps"]["capture_provider_bytes"] -= 1
        unsigned["caps"]["daily_provider_bytes"] -= 1
        capture["budget_bytes"] -= 1
    elif wrong_field == "path":
        unsigned["allowed_path_families"].remove("/Players")
        capture["allowed_path_families"].remove("/Players")
    else:
        capture["allocation_id"] = "signed-but-wrong-canary-allocation"
    approval = ProxyCampaignApproval.from_dict(
        sign_proxy_campaign_approval(unsigned, mod.CONTROL_TOKEN)
    )
    context.update(
        proxy_campaign_approval=approval.to_dict(),
        proxy_approval_sha256=approval.approval_sha256,
        proxy_allocation=capture,
        proxy_allocation_id=capture["allocation_id"],
    )
    mgr = _FakeManager(["http://u:p@pool.proxys.io:10000"])

    with pytest.raises(ProxyCampaignValidationError, match="exact 1 GB"):
        mod._create_lease(
            mgr,
            max_bytes=1_000,
            ttl_seconds=30,
            metadata=context,
            require_context=True,
        )

    assert mgr.calls == 0
    assert mod.LEASES == {}
    assert not Path(mod.WHOSCORED_CAMPAIGN_LEDGER_PATH).exists()


def test_proxy_service_source_modes_are_mutually_exclusive(mod):
    mgr = _FakeManager(["http://u:p@pool.proxys.io:10000"])
    context = _whoscored_campaign_context(mod, cap=1_000)
    mod.SOURCE_MODE = "shared-no-whoscored"
    with pytest.raises(ProxyCampaignValidationError, match="dedicated provider"):
        mod._create_lease(
            mgr,
            max_bytes=1_000,
            ttl_seconds=30,
            metadata=context,
            require_context=True,
        )

    mod.SOURCE_MODE = "whoscored-only"
    with pytest.raises(ValueError, match="every other source"):
        mod._create_lease(
            mgr,
            max_bytes=1_000,
            ttl_seconds=30,
            metadata={
                "dag_id": "dag_ingest_sofascore",
                "run_id": "run-one",
                "task_id": "task-one",
                "canonical_url": "https://www.sofascore.com/",
                "source": "sofascore",
            },
            require_context=True,
        )


def test_whoscored_signed_campaign_is_required_and_caps_provider_bytes(mod):
    mgr = _FakeManager(["http://u:p@pool.proxys.io:10000"])
    context = _whoscored_campaign_context(mod, cap=1_000)

    unsigned = dict(context)
    unsigned.pop("proxy_campaign_approval")
    with pytest.raises(ProxyCampaignValidationError, match="approval"):
        mod._create_lease(
            mgr,
            max_bytes=1_000,
            ttl_seconds=30,
            metadata=unsigned,
            require_context=True,
        )

    lease = mod._create_lease(
        mgr,
        max_bytes=1_000,
        ttl_seconds=30,
        metadata=context,
        require_context=True,
    )
    assert lease.source == "whoscored"
    assert lease.max_bytes == 1_000
    created = mod._whoscored_campaign_ledger().snapshot(lease.proxy_campaign_approval)
    assert created["spent_provider_bytes"] == 0
    assert created["active_claims"][lease.lease_id]["reserved_provider_bytes"] == 1_000
    mod._whoscored_campaign_ledger().record_request(
        lease.proxy_campaign_approval,
        lease.proxy_campaign_claim,
    )
    mod._account_lease_bytes(lease, "www.whoscored.com", "down", 1_000)
    report = asyncio.run(mod._close_lease(lease, completed=False))

    assert report["provider_billed_bytes"] == 1_000
    assert report["provider_meter"] == PROXY_CAMPAIGN_METER
    assert report["close_complete"] is True
    closed = mod._whoscored_campaign_ledger().snapshot(lease.proxy_campaign_approval)
    assert closed["active_claims"] == {}
    assert closed["spent_provider_bytes"] == 1_000
    retry_context = {**context, "try_number": 2}
    retry_context["proxy_attempt_id"] = deterministic_proxy_attempt_id(
        dag_id=str(retry_context["dag_id"]),
        run_id=str(retry_context["run_id"]),
        task_id=str(retry_context["task_id"]),
        map_index=int(retry_context["map_index"]),
        try_number=2,
    )
    with pytest.raises(ProxyCampaignBudgetExceeded):
        mod._create_lease(
            mgr,
            max_bytes=1,
            ttl_seconds=30,
            metadata=retry_context,
            require_context=True,
        )


def test_whoscored_global_cap_bounds_create_remaining_consume_and_release(mod):
    mgr = _FakeManager(
        [
            "http://u:p@pool.proxys.io:10000",
            "http://u:p@pool.proxys.io:10001",
            "http://u:p@pool.proxys.io:10002",
        ]
    )
    mod.DAILY_BUDGET_BYTES = 850
    first = mod._create_lease(
        mgr,
        max_bytes=700,
        ttl_seconds=30,
        metadata=_whoscored_campaign_context(mod, cap=1_000),
        require_context=True,
    )
    second = mod._create_lease(
        mgr,
        max_bytes=1_000,
        ttl_seconds=30,
        metadata=_whoscored_campaign_context(
            mod,
            cap=1_000,
            approval_id="approval-two",
            campaign_id="campaign-two",
            run_id="manual__campaign-two",
        ),
        require_context=True,
    )

    assert first.max_bytes == 700
    assert second.max_bytes == 150
    assert mod._reserve_lease_bytes(first, 1_000) == 700
    assert mod._reserve_lease_bytes(second, 1_000) == 150
    assert mod._lease_remaining(first) == 0
    assert mod._lease_remaining(second) == 0
    mod._release_lease_reservation(first, 700)
    mod._release_lease_reservation(second, 150)

    mod._whoscored_campaign_ledger().record_request(
        first.proxy_campaign_approval,
        first.proxy_campaign_claim,
    )
    mod._account_lease_bytes(first, "www.whoscored.com", "down", 600)
    assert mod._lease_remaining(first) == 100
    with pytest.raises(RuntimeError, match="global provider-order escrow"):
        mod._account_lease_bytes(first, "www.whoscored.com", "down", 101)
    assert first.total_bytes == 600
    assert first.global_budget_escrow_bytes == 100

    closed = asyncio.run(mod._close_lease(first, completed=False))
    assert closed["close_complete"] is True
    assert first.global_budget_escrow_bytes == 0
    third = mod._create_lease(
        mgr,
        max_bytes=1_000,
        ttl_seconds=30,
        metadata=_whoscored_campaign_context(
            mod,
            cap=1_000,
            approval_id="approval-three",
            campaign_id="campaign-three",
            run_id="manual__campaign-three",
        ),
        require_context=True,
    )
    assert third.max_bytes == 100
    accounting = mod._whoscored_campaign_ledger().provider_order_accounting()
    assert accounting["spent_provider_bytes"] == 600
    assert accounting["reserved_provider_bytes"] == 250
    assert accounting["exposure_provider_bytes"] == 850

    with pytest.raises(RuntimeError, match="provider-order budget exhausted"):
        mod._create_lease(
            mgr,
            max_bytes=1,
            ttl_seconds=30,
            metadata=_whoscored_campaign_context(
                mod,
                cap=1_000,
                approval_id="approval-four",
                campaign_id="campaign-four",
                run_id="manual__campaign-four",
            ),
            require_context=True,
        )


def test_whoscored_order_cap_survives_utc_reset_restart_and_new_approval(mod):
    mod.DAILY_BUDGET_BYTES = 850
    previous_day = datetime.now(timezone.utc).replace(
        hour=0,
        minute=0,
        second=0,
        microsecond=0,
    ) - timedelta(minutes=30)
    first_context = _whoscored_campaign_context(mod, cap=1_000)
    first_unsigned = ProxyCampaignApproval.from_dict(
        first_context["proxy_campaign_approval"]
    ).unsigned_dict()
    first_unsigned["issued_at"] = (previous_day - timedelta(minutes=1)).isoformat()
    first_unsigned["expires_at"] = (previous_day + timedelta(hours=1)).isoformat()
    first_approval = ProxyCampaignApproval.from_dict(
        sign_proxy_campaign_approval(
            first_unsigned,
            mod.WHOSCORED_PROXY_APPROVAL_HMAC_SECRET,
        )
    )
    first_allocation = first_approval.allocation("allocation-one")
    ledger = ProxyCampaignLedger(
        mod.WHOSCORED_CAMPAIGN_LEDGER_PATH,
        secret=mod.WHOSCORED_PROXY_LEDGER_HMAC_SECRET,
        approval_secret=mod.WHOSCORED_PROXY_APPROVAL_HMAC_SECRET,
    )
    claim = ledger.claim(
        first_approval,
        first_allocation.allocation_id,
        dag_id="dag_backfill_whoscored",
        run_id=first_approval.run_id,
        task_id=first_allocation.task_id,
        attempt_id="previous-day-attempt",
        lease_id="previous-day-lease",
        expires_at=previous_day + timedelta(minutes=10),
        canonical_url="https://www.whoscored.com/Matches/1/Live",
        now=previous_day,
    )
    ledger.reserve_provider_bytes(first_approval, claim, 800, now=previous_day)
    ledger.record_request(first_approval, claim, now=previous_day)
    ledger.consume(first_approval, claim, 800, now=previous_day)
    ledger.finish(
        first_approval,
        claim,
        provider_billed_bytes=800,
        completed=False,
        now=previous_day,
    )

    # Simulate both a process restart and the normal UTC daily-counter reset.
    mod.LEASES.clear()
    mod.LEASE_TOKENS.clear()
    mod.WHOSCORED_CAMPAIGN_LEDGER = None
    mod._WHOSCORED_CAMPAIGN_LEDGER_KEY = None
    mod._daily_day = mod._utc_day()
    mod._daily_up_bytes = mod._daily_down_bytes = mod._daily_reserved_bytes = 0
    restarted = mod._whoscored_campaign_ledger()
    before = restarted.provider_order_accounting()
    assert before["spent_provider_bytes"] == 800
    assert before["current_day_spent_provider_bytes"] == 0

    mgr = _FakeManager(["http://u:p@pool.proxys.io:10000"])
    current = mod._create_lease(
        mgr,
        max_bytes=1_000,
        ttl_seconds=30,
        metadata=_whoscored_campaign_context(
            mod,
            cap=1_000,
            approval_id="approval-after-midnight",
            campaign_id="campaign-after-midnight",
            run_id="manual__campaign-after-midnight",
        ),
        require_context=True,
    )
    assert current.max_bytes == 50
    after = restarted.provider_order_accounting()
    assert after["spent_provider_bytes"] == 800
    assert after["reserved_provider_bytes"] == 50
    assert after["exposure_provider_bytes"] == 850
    assert after["current_day_reserved_provider_bytes"] == 50


def test_whoscored_provider_guard_rejects_before_lease_or_claim(mod):
    mgr = _FakeManager(["http://u:p@pool.proxys.io:10000"])
    context = _whoscored_campaign_context(mod, cap=1_000)
    mod.WHOSCORED_PROVIDER_INVOICE_HARD_CAP_AVAILABLE = False

    with pytest.raises(ProxyCampaignValidationError, match="invoice hard cap"):
        mod._create_lease(
            mgr,
            max_bytes=1_000,
            ttl_seconds=30,
            metadata=context,
            require_context=True,
        )

    assert mod.LEASES == {}
    assert not Path(mod.WHOSCORED_CAMPAIGN_LEDGER_PATH).exists()


def test_whoscored_application_gateway_guard_is_independent_of_invoice_guard(mod):
    mgr = _FakeManager(["http://u:p@pool.proxys.io:10000"])
    context = _whoscored_campaign_context(mod, cap=1_000)
    mod.WHOSCORED_PROVIDER_INVOICE_HARD_CAP_AVAILABLE = True
    mod.WHOSCORED_PAID_APPLICATION_GATEWAY_AVAILABLE = False

    with pytest.raises(ProxyCampaignValidationError, match="application gateway"):
        mod._create_lease(
            mgr,
            max_bytes=1_000,
            ttl_seconds=30,
            metadata=context,
            require_context=True,
        )

    assert mod.LEASES == {}
    assert mgr.calls == 0
    assert not Path(mod.WHOSCORED_CAMPAIGN_LEDGER_PATH).exists()


def test_control_lease_rejects_unknown_dag_before_provider_selection(mod):
    mgr = _FakeManager(["http://u:p@pool.proxys.io:10000"])

    with pytest.raises(ValueError, match="closed source allowlist"):
        mod._create_lease(
            mgr,
            max_bytes=1_000,
            ttl_seconds=30,
            metadata={
                "dag_id": "attacker_dag",
                "run_id": "run-1",
                "task_id": "task-1",
                "canonical_url": "https://evil.example/",
            },
            require_context=True,
        )

    assert mgr.calls == 0
    assert mod.LEASES == {}


def test_whoscored_proxy_restart_keeps_unknown_lease_escrow_fail_closed(mod):
    mgr = _FakeManager(["http://u:p@pool.proxys.io:10000"])
    context = _whoscored_campaign_context(mod, cap=1_000)
    lease = mod._create_lease(
        mgr,
        max_bytes=800,
        ttl_seconds=30,
        metadata=context,
        require_context=True,
    )
    mod._whoscored_campaign_ledger().record_request(
        lease.proxy_campaign_approval,
        lease.proxy_campaign_claim,
    )
    mod._account_lease_bytes(lease, "www.whoscored.com", "down", 100)

    # Simulate a proxy SIGKILL: in-memory leases vanish without the clean-close
    # path which releases proven-unused escrow.
    mod.LEASES.clear()
    mod.LEASE_TOKENS.clear()
    after_expiry = datetime.now(timezone.utc) + timedelta(seconds=31)
    snapshot = mod._whoscored_campaign_ledger().snapshot(
        lease.proxy_campaign_approval,
        now=after_expiry,
    )
    assert snapshot["status"] == "revoked"
    active = snapshot["active_claims"][lease.lease_id]
    # The 100-byte observed prefix had not reached the bounded flush threshold.
    # A SIGKILL consequently retains the complete durable escrow as unknown;
    # it never guesses spend or returns the pending prefix as fresh allowance.
    assert active["spent_provider_bytes"] == 0
    assert active["reserved_provider_bytes"] == 800


def test_whoscored_accounting_failure_after_provider_write_never_releases_escrow(
    mod, monkeypatch
):
    mgr = _FakeManager(["http://u:p@pool.proxys.io:10000"])
    context = _whoscored_campaign_context(mod, cap=1_000)
    lease = mod._create_lease(
        mgr,
        max_bytes=1_000,
        ttl_seconds=30,
        metadata=context,
        require_context=True,
    )
    ledger = mod._whoscored_campaign_ledger()
    ledger.record_request(
        lease.proxy_campaign_approval,
        lease.proxy_campaign_claim,
    )

    class Writer:
        def __init__(self):
            self.payload = bytearray()
            self.closed = False

        def write(self, value):
            self.payload.extend(value)

        async def drain(self):
            return None

        def close(self):
            self.closed = True

    def fail_consume(*args, **kwargs):
        raise OSError("simulated durable accounting failure")

    writer = Writer()
    lease.tunnel_writers.add(writer)
    monkeypatch.setattr(ledger, "consume", fail_consume)
    payload = b"already-queued-provider-request"

    assert asyncio.run(
        mod._write_upstream(
            writer,
            payload,
            lease=lease,
            host="www.whoscored.com",
            direction="up",
        )
    )
    # The already-fsynced full-lease escrow permits a bounded in-memory
    # observed prefix. Terminal close must flush it and turns red/revoked when
    # that durable conversion fails.
    assert lease.pending_whoscored_bytes
    closed = asyncio.run(mod._close_lease(lease, completed=False))

    assert bytes(writer.payload) == payload
    assert writer.closed is True
    assert lease.accounting_uncertain is True
    assert lease.reserved_bytes == 0
    assert closed["close_complete"] is False
    state = json.loads(Path(mod.WHOSCORED_CAMPAIGN_LEDGER_PATH).read_text())
    campaign = state["campaigns"][lease.proxy_campaign_approval.campaign_id]
    active = campaign["active_claims"][lease.lease_id]
    assert campaign["status"] == "revoked"
    assert campaign["revocation_reason"] == (
        "provider byte accounting became uncertain"
    )
    assert active["spent_provider_bytes"] == 0
    assert active["reserved_provider_bytes"] == 1_000

    assert closed["accounting_uncertain"] is True
    assert "escrow retained" in closed["close_error"]
    retained = json.loads(Path(mod.WHOSCORED_CAMPAIGN_LEDGER_PATH).read_text())
    retained_claim = retained["campaigns"][lease.proxy_campaign_approval.campaign_id][
        "active_claims"
    ][lease.lease_id]
    assert retained_claim["reserved_provider_bytes"] == 1_000

    # Simulate a process restart: only the signed durable ledger survives.
    mod.LEASES.clear()
    mod.LEASE_TOKENS.clear()
    mod.WHOSCORED_CAMPAIGN_LEDGER = None
    mod._WHOSCORED_CAMPAIGN_LEDGER_KEY = None
    retry_context = {**context, "try_number": 2}
    retry_context["proxy_attempt_id"] = deterministic_proxy_attempt_id(
        dag_id=str(retry_context["dag_id"]),
        run_id=str(retry_context["run_id"]),
        task_id=str(retry_context["task_id"]),
        map_index=int(retry_context["map_index"]),
        try_number=2,
    )
    with pytest.raises(ProxyCampaignError, match="revoked"):
        mod._create_lease(
            mgr,
            max_bytes=1,
            ttl_seconds=30,
            metadata=retry_context,
            require_context=True,
        )
    retry_context["proxy_attempt_id"] = deterministic_proxy_attempt_id(
        dag_id=str(retry_context["dag_id"]),
        run_id=str(retry_context["run_id"]),
        task_id=str(retry_context["task_id"]),
        map_index=int(retry_context["map_index"]),
        try_number=2,
    )
    with pytest.raises(ProxyCampaignError, match="revoked"):
        mod._create_lease(
            mgr,
            max_bytes=1,
            ttl_seconds=30,
            metadata=retry_context,
            require_context=True,
        )


def test_whoscored_order_safety_cap_metering_is_batched_exactly(mod, monkeypatch):
    """The exact order safety window must not fsync once per 64 KiB."""

    total = mod.WHOSCORED_PROVIDER_ORDER_SAFETY_CAP_BYTES
    batch = 4 * 1024 * 1024
    mod.DAILY_BUDGET_BYTES = total
    mod.WHOSCORED_METER_BATCH_BYTES = batch
    approval = SimpleNamespace(campaign_id="synthetic-one-gb")
    lease = mod.Lease(
        lease_id="synthetic-lease",
        token="synthetic-token",
        upstream=("provider.invalid", 443, "u", "p"),
        created_at=1.0,
        expires_at=time.time() + 60,
        max_bytes=total,
        source="whoscored",
        dag_id="dag_canary_whoscored_proxy",
        run_id="manual__synthetic-one-gb",
        task_id="run_whoscored_proxy_canary",
        canonical_url="https://www.whoscored.com/Matches/1/Live",
        proxy_campaign_approval=approval,
        proxy_campaign_claim=object(),
        proxy_work_allocation=object(),
        global_budget_escrow_bytes=total,
    )

    consumed = []
    events = []

    class Ledger:
        def consume(self, _approval, _claim, amount):
            consumed.append(amount)

    monkeypatch.setattr(mod, "_whoscored_campaign_ledger", lambda: Ledger())
    monkeypatch.setattr(
        mod,
        "_append_budget_event",
        lambda event_type, _lease, **values: events.append(
            (event_type, values["direction"], values["bytes"])
        ),
    )

    observed = 0
    index = 0
    while observed < total:
        amount = min(65_536, total - observed)
        mod._account_lease_bytes(
            lease,
            "www.whoscored.com",
            "up" if index % 2 == 0 else "down",
            amount,
        )
        observed += amount
        index += 1

    expected_batches = -(-total // batch)
    assert sum(consumed) == total
    assert len(consumed) == expected_batches
    assert sum(item[2] for item in events) == total
    assert len(events) <= expected_batches * 2
    assert lease.total_bytes == total
    assert lease.settled_whoscored_bytes == total
    assert lease.pending_whoscored_bytes == {}
    assert lease.budget_exceeded is True


def test_whoscored_proxy_wal_failure_after_campaign_consume_revokes(mod, monkeypatch):
    mgr = _FakeManager(["http://u:p@pool.proxys.io:10000"])
    lease = mod._create_lease(
        mgr,
        max_bytes=1_000,
        ttl_seconds=30,
        metadata=_whoscored_campaign_context(mod, cap=1_000),
        require_context=True,
    )
    ledger = mod._whoscored_campaign_ledger()
    ledger.record_request(
        lease.proxy_campaign_approval,
        lease.proxy_campaign_claim,
    )
    mod._account_lease_bytes(lease, "www.whoscored.com", "down", 100)

    def fail_proxy_wal(*_args, **_kwargs):
        raise OSError("simulated proxy WAL fsync failure")

    monkeypatch.setattr(mod, "_append_budget_event", fail_proxy_wal)
    report = asyncio.run(mod._close_lease(lease, completed=False))

    assert report["close_complete"] is False
    assert report["accounting_uncertain"] is True
    assert lease.metering_flush_failed is True
    state = json.loads(Path(mod.WHOSCORED_CAMPAIGN_LEDGER_PATH).read_text())
    campaign = state["campaigns"][lease.proxy_campaign_approval.campaign_id]
    claim = campaign["active_claims"][lease.lease_id]
    assert campaign["status"] == "revoked"
    assert claim["spent_provider_bytes"] == 100
    assert claim["reserved_provider_bytes"] == 900


def test_finalized_lease_cache_is_count_bounded(mod):
    mod.MAX_FINALIZED_LEASES = 8
    mod.FINALIZED_LEASE_TTL_SECONDS = 10_000
    for index in range(100):
        lease = mod.Lease(
            lease_id=f"lease-{index:03d}",
            token=f"token-{index:03d}",
            upstream=("provider.invalid", 443, "u", "p"),
            created_at=float(index),
            expires_at=10_000.0,
            max_bytes=1,
            closed=True,
            close_recorded=True,
            finalized_at=float(index + 1),
        )
        mod.LEASES[lease.lease_id] = lease
        mod.LEASE_TOKENS[lease.token] = lease.lease_id

    assert mod._prune_finalized_leases(now=101.0) == 92
    assert len(mod.LEASES) == 8
    assert len(mod.LEASE_TOKENS) == 8
    assert set(mod.LEASES) == {f"lease-{index:03d}" for index in range(92, 100)}


def test_cancelled_provider_head_retains_unknown_read_ahead_escrow(mod):
    mgr = _FakeManager(["http://u:p@pool.proxys.io:10000"])
    lease = mod._create_lease(
        mgr,
        max_bytes=1_000,
        ttl_seconds=30,
        metadata=_whoscored_campaign_context(mod, cap=1_000),
        require_context=True,
    )
    ledger = mod._whoscored_campaign_ledger()
    ledger.record_request(
        lease.proxy_campaign_approval,
        lease.proxy_campaign_claim,
    )

    class Reader:
        def __init__(self):
            self.calls = 0

        async def read(self, size):
            self.calls += 1
            if self.calls == 1:
                return b"H"
            raise asyncio.CancelledError

    with pytest.raises(asyncio.CancelledError):
        asyncio.run(
            mod._read_metered_provider_head(
                Reader(),
                lease,
                "www.whoscored.com",
            )
        )

    assert lease.down_bytes == 1
    assert lease.accounting_uncertain is True
    assert lease.reserved_bytes == 1_000
    state = json.loads(Path(mod.WHOSCORED_CAMPAIGN_LEDGER_PATH).read_text())
    campaign = state["campaigns"][lease.proxy_campaign_approval.campaign_id]
    active = campaign["active_claims"][lease.lease_id]
    assert campaign["status"] == "revoked"
    assert active["spent_provider_bytes"] == 1
    assert active["reserved_provider_bytes"] == 999


def test_expiry_reaper_never_settles_an_uncertain_lease_with_zero_local_reserve(mod):
    mgr = _FakeManager(["http://u:p@pool.proxys.io:10000"])
    lease = mod._create_lease(
        mgr,
        max_bytes=1_000,
        ttl_seconds=30,
        metadata=_whoscored_campaign_context(mod, cap=1_000),
        require_context=True,
    )
    # Defensive regression case: even if a future/manual path clears the local
    # counter, the explicit uncertainty latch must dominate normal TTL finish.
    lease.accounting_uncertain = True
    lease.closed = False
    lease.reserved_bytes = 0
    lease.expires_at = time.time() - 1

    assert mod._reap_expired_leases() == 0
    assert lease.closed is True
    assert lease.proxy_campaign_finished is False
    state = json.loads(Path(mod.WHOSCORED_CAMPAIGN_LEDGER_PATH).read_text())
    campaign = state["campaigns"][lease.proxy_campaign_approval.campaign_id]
    assert lease.lease_id in campaign["active_claims"]
    assert campaign["active_claims"][lease.lease_id]["reserved_provider_bytes"] == 1_000


@pytest.mark.parametrize("failure", ["oserror", "cancelled"])
def test_downstream_pump_failure_retains_provider_read_ahead_escrow(mod, failure):
    mgr = _FakeManager(["http://u:p@pool.proxys.io:10000"])
    context = _whoscored_campaign_context(mod, cap=1_000)
    lease = mod._create_lease(
        mgr,
        max_bytes=1_000,
        ttl_seconds=30,
        metadata=context,
        require_context=True,
    )
    mod._whoscored_campaign_ledger().record_request(
        lease.proxy_campaign_approval,
        lease.proxy_campaign_claim,
    )

    class Reader:
        def __init__(self):
            # The second item represents bytes already buffered from the paid
            # provider but never returned after downstream delivery fails.
            self.chunks = [b"visible-provider-bytes", b"hidden-read-ahead", b""]
            self.read_calls = 0

        async def read(self, size):
            self.read_calls += 1
            return self.chunks.pop(0)

    class Writer:
        def __init__(self):
            self.payload = bytearray()
            self.closed = False

        def write(self, value):
            self.payload.extend(value)

        async def drain(self):
            if failure == "cancelled":
                raise asyncio.CancelledError
            raise OSError("downstream disconnected")

        def close(self):
            self.closed = True

    reader = Reader()
    writer = Writer()
    operation = mod._pump(
        reader,
        writer,
        "www.whoscored.com",
        defaultdict(int),
        lease=lease,
        direction="down",
    )
    if failure == "cancelled":
        with pytest.raises(asyncio.CancelledError):
            asyncio.run(operation)
    else:
        asyncio.run(operation)

    visible = len(b"visible-provider-bytes")
    assert reader.read_calls == 1
    assert bytes(writer.payload) == b"visible-provider-bytes"
    assert writer.closed is True
    assert lease.down_bytes == visible
    assert lease.accounting_uncertain is True
    assert lease.reserved_bytes == 0
    state = json.loads(Path(mod.WHOSCORED_CAMPAIGN_LEDGER_PATH).read_text())
    campaign = state["campaigns"][lease.proxy_campaign_approval.campaign_id]
    active = campaign["active_claims"][lease.lease_id]
    assert campaign["status"] == "revoked"
    assert active["spent_provider_bytes"] == visible
    assert active["reserved_provider_bytes"] == 1_000 - visible

    report = asyncio.run(mod._close_lease(lease, completed=False))
    assert report["close_complete"] is False
    assert "escrow retained" in report["close_error"]

    mod.LEASES.clear()
    mod.LEASE_TOKENS.clear()
    mod.WHOSCORED_CAMPAIGN_LEDGER = None
    mod._WHOSCORED_CAMPAIGN_LEDGER_KEY = None
    retry_context = {**context, "try_number": 2}
    retry_context["proxy_attempt_id"] = deterministic_proxy_attempt_id(
        dag_id=str(retry_context["dag_id"]),
        run_id=str(retry_context["run_id"]),
        task_id=str(retry_context["task_id"]),
        map_index=int(retry_context["map_index"]),
        try_number=2,
    )
    with pytest.raises(ProxyCampaignError, match="revoked"):
        mod._create_lease(
            mgr,
            max_bytes=1,
            ttl_seconds=30,
            metadata=retry_context,
            require_context=True,
        )


def test_downstream_pump_ttl_exit_without_provider_eof_retains_escrow(mod):
    mgr = _FakeManager(["http://u:p@pool.proxys.io:10000"])
    context = _whoscored_campaign_context(mod, cap=1_000)
    lease = mod._create_lease(
        mgr,
        max_bytes=1_000,
        ttl_seconds=30,
        metadata=context,
        require_context=True,
    )
    mod._whoscored_campaign_ledger().record_request(
        lease.proxy_campaign_approval,
        lease.proxy_campaign_claim,
    )

    class Reader:
        def __init__(self):
            self.chunks = [b"visible-prefix", b"hidden-after-ttl", b""]
            self.read_calls = 0

        async def read(self, size):
            self.read_calls += 1
            return self.chunks.pop(0)

    class Writer:
        def __init__(self):
            self.payload = bytearray()
            self.closed = False

        def write(self, value):
            self.payload.extend(value)

        async def drain(self):
            # Expire the lease after delivery of the first observed chunk but
            # before the pump can reserve/read the provider's buffered suffix.
            lease.expires_at = time.time() - 1

        def close(self):
            self.closed = True

    reader = Reader()
    writer = Writer()
    asyncio.run(
        mod._pump(
            reader,
            writer,
            "www.whoscored.com",
            defaultdict(int),
            lease=lease,
            direction="down",
        )
    )

    visible = len(b"visible-prefix")
    assert reader.read_calls == 1
    assert bytes(writer.payload) == b"visible-prefix"
    assert writer.closed is True
    assert lease.down_bytes == visible
    assert lease.accounting_uncertain is True
    assert lease.usable is False
    state = json.loads(Path(mod.WHOSCORED_CAMPAIGN_LEDGER_PATH).read_text())
    campaign = state["campaigns"][lease.proxy_campaign_approval.campaign_id]
    active = campaign["active_claims"][lease.lease_id]
    assert campaign["status"] == "revoked"
    assert active["spent_provider_bytes"] == visible
    assert active["reserved_provider_bytes"] == 1_000 - visible

    report = asyncio.run(mod._close_lease(lease, completed=False))
    assert report["close_complete"] is False
    assert "escrow retained" in report["close_error"]

    mod.LEASES.clear()
    mod.LEASE_TOKENS.clear()
    mod.WHOSCORED_CAMPAIGN_LEDGER = None
    mod._WHOSCORED_CAMPAIGN_LEDGER_KEY = None
    retry_context = {**context, "try_number": 2}
    retry_context["proxy_attempt_id"] = deterministic_proxy_attempt_id(
        dag_id=str(retry_context["dag_id"]),
        run_id=str(retry_context["run_id"]),
        task_id=str(retry_context["task_id"]),
        map_index=int(retry_context["map_index"]),
        try_number=2,
    )
    with pytest.raises(ProxyCampaignError, match="revoked"):
        mod._create_lease(
            mgr,
            max_bytes=1,
            ttl_seconds=30,
            metadata=retry_context,
            require_context=True,
        )


@pytest.mark.parametrize("direction", ["up", "down"])
def test_silent_paid_pump_is_cancelled_at_lease_expiry(mod, direction):
    mgr = _FakeManager(["http://u:p@pool.proxys.io:10000"])
    lease = mod._create_lease(
        mgr,
        max_bytes=1_000,
        ttl_seconds=30,
        metadata=_whoscored_campaign_context(mod, cap=1_000),
        require_context=True,
    )
    lease.expires_at = time.time() + 0.03

    class SilentReader:
        cancelled = False

        async def read(self, _size):
            try:
                await asyncio.Event().wait()
            except asyncio.CancelledError:
                self.cancelled = True
                raise

    class Writer:
        def __init__(self):
            self.closed = False
            self.payload = bytearray()

        def write(self, payload):
            self.payload.extend(payload)

        async def drain(self):
            return None

        def close(self):
            self.closed = True

    reader = SilentReader()
    writer = Writer()
    remote_peer = Writer()
    lease.tunnel_writers.update({writer, remote_peer})
    started = time.monotonic()

    asyncio.run(
        mod._pump(
            reader,
            writer,
            "www.whoscored.com",
            defaultdict(int),
            lease=lease,
            direction=direction,
        )
    )

    assert time.monotonic() - started < 0.5
    assert reader.cancelled is True
    assert writer.closed is True
    assert remote_peer.closed is True
    assert bytes(writer.payload) == b""
    assert lease.total_bytes == 0
    assert lease.accounting_uncertain is True
    state = json.loads(Path(mod.WHOSCORED_CAMPAIGN_LEDGER_PATH).read_text())
    campaign = state["campaigns"][lease.proxy_campaign_approval.campaign_id]
    assert campaign["status"] == "revoked"
    assert campaign["active_claims"][lease.lease_id]["reserved_provider_bytes"] == 1_000


def test_cancellation_resistant_read_cannot_settle_or_forward_after_expiry(mod):
    mgr = _FakeManager(["http://u:p@pool.proxys.io:10000"])
    lease = mod._create_lease(
        mgr,
        max_bytes=1_000,
        ttl_seconds=30,
        metadata=_whoscored_campaign_context(mod, cap=1_000),
        require_context=True,
    )
    lease.expires_at = time.time() + 0.02

    class LateReader:
        async def read(self, _size):
            try:
                await asyncio.Event().wait()
            except asyncio.CancelledError:
                return b"post-expiry-provider-bytes"

    class Writer:
        def __init__(self):
            self.payload = bytearray()
            self.closed = False

        def write(self, payload):
            self.payload.extend(payload)

        async def drain(self):
            return None

        def close(self):
            self.closed = True

    writer = Writer()
    asyncio.run(
        mod._pump(
            LateReader(),
            writer,
            "www.whoscored.com",
            defaultdict(int),
            lease=lease,
            direction="down",
        )
    )

    assert writer.payload == b""
    assert writer.closed is True
    assert lease.total_bytes == 0
    assert lease.settled_whoscored_bytes == 0
    assert lease.accounting_uncertain is True


def test_expiry_reaper_force_closes_remote_orphan_and_retains_claim(mod):
    mgr = _FakeManager(["http://u:p@pool.proxys.io:10000"])
    lease = mod._create_lease(
        mgr,
        max_bytes=1_000,
        ttl_seconds=30,
        metadata=_whoscored_campaign_context(mod, cap=1_000),
        require_context=True,
    )

    class Writer:
        def __init__(self):
            self.closed = False

        def close(self):
            self.closed = True

    client = Writer()
    provider = Writer()
    lease.tunnel_writers.update({client, provider})
    lease.active_tunnels = 1
    assert mod._reserve_lease_bytes(lease, 100) == 100
    lease.expires_at = time.time() - 1

    assert mod._reap_expired_leases() == 0

    assert client.closed is True
    assert provider.closed is True
    assert lease.closed is True
    assert lease.accounting_uncertain is True
    assert lease.proxy_campaign_finished is False
    state = json.loads(Path(mod.WHOSCORED_CAMPAIGN_LEDGER_PATH).read_text())
    campaign = state["campaigns"][lease.proxy_campaign_approval.campaign_id]
    assert campaign["status"] == "revoked"
    assert campaign["active_claims"][lease.lease_id]["reserved_provider_bytes"] == 1_000


def test_whoscored_signed_campaign_rejects_unsigned_path_expansion(mod):
    mgr = _FakeManager(["http://u:p@pool.proxys.io:10000"])
    context = _whoscored_campaign_context(
        mod,
        canonical_url="https://www.whoscored.com/Players/1/Show",
    )

    with pytest.raises(ProxyCampaignValidationError, match="canonical_url"):
        mod._create_lease(
            mgr,
            max_bytes=100,
            ttl_seconds=30,
            metadata=context,
            require_context=True,
        )


# --- _load_blocklist ----------------------------------------------------------


def test_load_blocklist_none_returns_empty(mod):
    assert mod._load_blocklist(None) == set()


def test_load_blocklist_strips_comments_blanks_and_lowercases(mod, tmp_path):
    # Arrange
    f = tmp_path / "bl.txt"
    f.write_text("# header comment\n\nDoubleClick.net\n  adnxs.com  \n# trailing\n")
    # Act
    result = mod._load_blocklist(str(f))
    # Assert
    assert result == {"doubleclick.net", "adnxs.com"}


# --- production proxy-pool secret --------------------------------------------


def _pool_json(**overrides):
    entry = {
        "host": "Pool.Example.COM",
        "port": 10000,
        "username": "account-zone-production",
        "password": "test-only:p@ssword",
    }
    entry.update(overrides)
    return json.dumps([entry])


def test_proxy_pool_json_is_strictly_parsed_and_normalised(mod):
    records = mod._parse_proxy_pool_json(_pool_json())

    assert records == (
        {
            "host": "pool.example.com",
            "port": 10000,
            "username": "account-zone-production",
            "password": "test-only:p@ssword",
        },
    )


@pytest.mark.parametrize(
    "payload, expected_field",
    [
        ("", "PROXY_POOL_JSON"),
        ("not-json", "PROXY_POOL_JSON"),
        ("{}", "PROXY_POOL_JSON"),
        ("[]", "PROXY_POOL_JSON"),
        (json.dumps(["not-an-object"]), "entry"),
        (json.dumps([{"host": "pool.example"}]), "fields"),
        (_pool_json(extra="not-allowed"), "fields"),
        (_pool_json(host=" bad.example"), "host"),
        (_pool_json(host="bad_host.example"), "host"),
        (_pool_json(port=True), "port"),
        (_pool_json(port=0), "port"),
        (_pool_json(username="bad:name"), "username"),
        (_pool_json(password="bad\npassword"), "password"),
        (_pool_json(password="\ud800"), "password"),
    ],
)
def test_proxy_pool_json_rejects_invalid_shapes_without_echoing_values(
    mod, payload, expected_field
):
    with pytest.raises(mod.ProxyPoolConfigurationError) as caught:
        mod._parse_proxy_pool_json(payload)

    message = str(caught.value)
    assert expected_field in message
    assert "not-allowed" not in message
    assert "bad:name" not in message
    assert "bad password" not in message


def test_proxy_pool_json_rejects_duplicate_json_fields_without_echoing_secret(mod):
    payload = (
        '[{"host":"pool.example","port":10000,"username":"user",'
        '"password":"test-secret-one","password":"test-secret-two"}]'
    )

    with pytest.raises(mod.ProxyPoolConfigurationError) as caught:
        mod._parse_proxy_pool_json(payload)

    assert "duplicate object field" in str(caught.value)
    assert "test-secret" not in str(caught.value)


def test_proxy_pool_json_rejects_duplicate_endpoint_identity(mod):
    first = json.loads(_pool_json())[0]
    payload = json.dumps([first, {**first, "password": "different-test-password"}])

    with pytest.raises(mod.ProxyPoolConfigurationError, match="duplicates"):
        mod._parse_proxy_pool_json(payload)


def test_whoscored_only_pool_requires_pinned_provider_dial_host(mod, tmp_path):
    mod.SOURCE_MODE = "whoscored-only"

    with pytest.raises(mod.ProxyPoolConfigurationError, match="pinned provider"):
        mod._residential_manager(
            proxy_pool_json=_pool_json(host="untrusted.example"),
            proxy_file=str(tmp_path / "unused"),
            allow_file_fallback=False,
        )


@pytest.mark.parametrize("pool_json", [None, "   "], ids=("absent", "blank"))
def test_whoscored_only_missing_json_rejects_without_reading_fallback_file(
    mod, monkeypatch, tmp_path, pool_json
):
    from scrapers.utils.proxy_manager import ProxyManager

    mod.SOURCE_MODE = "whoscored-only"

    def forbidden_file_read(*args, **kwargs):
        raise AssertionError("WhoScored-only mode must not read a proxy file")

    monkeypatch.setattr(
        ProxyManager, "load_from_file_custom_format", forbidden_file_read
    )
    with pytest.raises(mod.ProxyPoolConfigurationError, match="PROXY_POOL_JSON"):
        mod._residential_manager(
            proxy_pool_json=pool_json,
            proxy_file=str(tmp_path / "must-not-be-read"),
            allow_file_fallback=True,
        )


def test_whoscored_only_upstream_uses_verified_tls_and_pinned_sni(mod, monkeypatch):
    mod.SOURCE_MODE = "whoscored-only"
    opened = []

    async def fake_open(host, port, **kwargs):
        opened.append((host, port, kwargs))
        return object(), object()

    monkeypatch.setattr(mod.asyncio, "open_connection", fake_open)
    asyncio.run(mod._open_upstream_connection(mod.WHOSCORED_UPSTREAM_DIAL_HOST, 10000))

    assert len(opened) == 1
    host, port, kwargs = opened[0]
    assert (host, port) == ("pool.proxys.io", 10000)
    assert kwargs["server_hostname"] == "pool.infatica.io"
    context = kwargs["ssl"]
    assert context is mod._WHOSCORED_UPSTREAM_TLS_CONTEXT
    assert context.verify_mode == mod.ssl.CERT_REQUIRED
    assert context.check_hostname is True
    assert context.minimum_version >= mod.ssl.TLSVersion.TLSv1_2


def test_whoscored_only_upstream_rejects_unpinned_host_without_dial(mod, monkeypatch):
    mod.SOURCE_MODE = "whoscored-only"
    opened = []

    async def fake_open(*args, **kwargs):
        opened.append((args, kwargs))
        return object(), object()

    monkeypatch.setattr(mod.asyncio, "open_connection", fake_open)
    with pytest.raises(mod.ProxyPoolConfigurationError, match="pinned provider"):
        asyncio.run(mod._open_upstream_connection("untrusted.example", 10000))

    assert opened == []


def test_whoscored_tls_verification_failure_has_no_plaintext_fallback(mod, monkeypatch):
    mod.SOURCE_MODE = "whoscored-only"
    opened = []

    async def fake_open(host, port, **kwargs):
        opened.append((host, port, kwargs))
        raise mod.ssl.SSLCertVerificationError("certificate verify failed")

    monkeypatch.setattr(mod.asyncio, "open_connection", fake_open)
    with pytest.raises(mod.ssl.SSLCertVerificationError):
        asyncio.run(
            mod._open_upstream_connection(mod.WHOSCORED_UPSTREAM_DIAL_HOST, 10000)
        )

    assert len(opened) == 1
    assert opened[0][2]["ssl"] is mod._WHOSCORED_UPSTREAM_TLS_CONTEXT
    assert opened[0][2]["server_hostname"] == "pool.infatica.io"


def test_shared_upstream_transport_remains_plain_tcp(mod, monkeypatch):
    mod.SOURCE_MODE = "shared-no-whoscored"
    opened = []

    async def fake_open(host, port, **kwargs):
        opened.append((host, port, kwargs))
        return object(), object()

    monkeypatch.setattr(mod.asyncio, "open_connection", fake_open)
    asyncio.run(mod._open_upstream_connection("pool.example", 10000))

    assert opened == [("pool.example", 10000, {})]


def test_residential_manager_loads_env_secret_without_file_access(mod, tmp_path):
    mgr, source = mod._residential_manager(
        proxy_pool_json=_pool_json(),
        proxy_file=str(tmp_path / "does-not-exist"),
        allow_file_fallback=True,
    )

    assert source == "PROXY_POOL_JSON"
    assert mgr.total_count == 1
    assert mod._pick_upstream(mgr) == (
        "pool.example.com",
        10000,
        "account-zone-production",
        "test-only:p@ssword",
    )


def test_residential_manager_fails_closed_when_env_is_missing(mod, tmp_path):
    fallback = tmp_path / "proxys.txt"
    fallback.write_text("pool.example:10000:user:test-only-password\n")

    with pytest.raises(mod.ProxyPoolConfigurationError, match="fallback is disabled"):
        mod._residential_manager(
            proxy_pool_json=None,
            proxy_file=str(fallback),
            allow_file_fallback=False,
        )


def test_residential_manager_uses_file_only_with_explicit_opt_in(mod, tmp_path):
    fallback = tmp_path / "proxys.txt"
    fallback.write_text("pool.example:10000:user:test-only-password\n")

    mgr, source = mod._residential_manager(
        proxy_pool_json=None,
        proxy_file=str(fallback),
        allow_file_fallback=True,
    )

    assert source == "explicit file fallback"
    assert mgr.total_count == 1


def test_malformed_env_never_silently_falls_back_to_file(mod, tmp_path):
    fallback = tmp_path / "proxys.txt"
    fallback.write_text("pool.example:10000:user:test-only-password\n")

    with pytest.raises(mod.ProxyPoolConfigurationError, match="valid JSON"):
        mod._residential_manager(
            proxy_pool_json="malformed-test-secret",
            proxy_file=str(fallback),
            allow_file_fallback=True,
        )


def test_proxy_filter_compose_is_env_only_by_default():
    compose = _COMPOSE_PATH.read_text()
    # The dedicated sofascore_proxy_filter service (#951) was moved into its own
    # compose project; a breadcrumb comment now marks its old spot between the
    # shared proxy_filter and caddy — bound the slice on that breadcrumb.
    service = compose.split("  proxy_filter:\n", 1)[1].split(
        "\n  # sofascore_proxy_filter ВЫНЕСЕН", 1
    )[0]

    assert "PROXY_POOL_JSON: ${PROXY_POOL_JSON:-}" in service
    assert 'PROXY_FILTER_ALLOW_FILE_FALLBACK: "false"' in service
    assert "proxys.txt:/opt/airflow/proxys.txt" not in service
    # The lease concurrency limit is operator-tunable; the serial guarantees
    # that matter are per source (SofaScore production/canary), not global.
    assert "${PROXY_FILTER_MAX_ACTIVE_LEASES:-4}" in service


def test_sofascore_has_a_dedicated_production_metered_proxy_service():
    # The gateway lives in its OWN compose project (#951, инцидент 2026-07-17):
    # a foreign `docker compose up` on the shared project must not recreate it.
    gateway = _SOFASCORE_GATEWAY_COMPOSE_PATH.read_text()
    service = gateway.split("  sofascore_proxy_filter:\n", 1)[1].split(
        "\nnetworks:\n", 1
    )[0]

    # Dedicated pool secret + file fallback until the purchased pool lands.
    assert "PROXY_POOL_JSON: ${SOFASCORE_PROXY_POOL_JSON:-}" in service
    assert 'PROXY_FILTER_ALLOW_FILE_FALLBACK: "true"' in service
    assert "./proxys.txt:/opt/airflow/proxys.txt:ro" in service
    assert "http://sofascore_proxy_filter:8900" in service
    # hard-cap 0 => production signer (a >0 cap is the never-authorized canary).
    assert '\n      - --sofascore-canary-hard-cap-bytes\n      - "0"' in service
    # One active SofaScore lease at a time.
    assert '\n      - --max-active-leases\n      - "1"' in service
    # Ledger/WAL on the persistent log root, isolated from the shared gateway.
    assert (
        "/logs/sofascore_proxy_filter/sofascore_allocation_claims.jsonl"
        in service
    )
    # Isolation contract: joins the shared dp-backend network as EXTERNAL (own
    # project) and is ABSENT from the shared compose.yaml, so foreign deploys
    # can't sweep it.
    assert "external: true" in gateway
    assert "name: dp-backend" in gateway
    assert "\n  sofascore_proxy_filter:\n" not in _COMPOSE_PATH.read_text()
    assert (
        "SOFASCORE_PROXY_BUDGET_ARTIFACT:"
        "-/opt/airflow/configs/sofascore/proxy_budget_canary.json"
    ) in service


def test_fbref_has_an_isolated_metered_proxy_service():
    compose = _COMPOSE_PATH.read_text()
    service = compose.split("  fbref_proxy_filter:\n", 1)[1].split(
        "\n  proxy_filter:\n", 1
    )[0]

    assert "PROXY_POOL_JSON: \"\"" in service
    assert "PROXY_FILTER_ALLOW_FILE_FALLBACK: \"true\"" in service
    assert (
        "${FBREF_PROXY_POOL_FILE:-./proxys.txt}:"
        "/opt/airflow/proxys.txt:ro"
    ) in service
    assert "PROXY_FILTER_CONTROL_TOKEN: ${FBREF_PROXY_CONTROL_TOKEN:-}" in service
    assert "SOFASCORE_PROXY_CONTROL_TOKEN" not in service
    assert "http://fbref_proxy_filter:8900" in service
    assert "${FBREF_PROXY_DAGRUN_BUDGET_BYTES:-104857600}" in service
    assert "${FBREF_PROXY_URL_BUDGET_BYTES:-104857600}" in service
    assert '\n      - "1"' in service
    assert "/logs/fbref/proxy_filter/unused_sofascore_claims.jsonl" in service
    assert "/logs/proxy_filter/sofascore_allocation_claims.jsonl" not in service


def test_fbref_control_secret_is_explicit_in_airflow_and_example_env():
    compose = _COMPOSE_PATH.read_text()
    common = compose.split("x-airflow-common: &airflow-common", 1)[1].split(
        "services:", 1
    )[0]
    assert "FBREF_PROXY_CONTROL_TOKEN: ${FBREF_PROXY_CONTROL_TOKEN:-}" in common
    assert "FBREF_PROXY_CONTROL_TOKEN: ${SOFASCORE_PROXY_CONTROL_TOKEN:-}" not in (
        compose
    )

    example = _ENV_EXAMPLE_PATH.read_text()
    assert "\nFBREF_PROXY_CONTROL_TOKEN=\n" in example
    assert (
        "FBREF_PROXY_POOL_FILE=/root/fbref-949-runtime/proxys.txt" in example
    )


def test_fbref_acceptance_compose_is_a_separate_project_scoped_stack():
    acceptance = _FBREF_ACCEPTANCE_COMPOSE_PATH.read_text()
    proxy = acceptance.split("  fbref_acceptance_proxy_filter:\n", 1)[1].split(
        "\n  fbref_acceptance_runner:\n", 1
    )[0]
    runner = acceptance.split("  fbref_acceptance_runner:\n", 1)[1].split(
        "\nnetworks:\n", 1
    )[0]

    assert "-p fbref-acceptance-949" in acceptance
    assert "container_name:" not in acceptance
    assert "ports:" not in acceptance
    assert "build:" not in acceptance
    assert acceptance.count(
        "image: ${FBREF_ACCEPTANCE_AIRFLOW_IMAGE:?"
    ) == 2
    assert acceptance.count(
        "/opt/airflow/scripts/fbref_acceptance_entrypoint.sh"
    ) == 2
    assert acceptance.count("user: ${AIRFLOW_UID:-50000}:0") == 2
    assert "FBREF_EXPECTED_GIT_SHA: ${FBREF_ACCEPTANCE_GIT_SHA:?" in proxy
    assert (
        "FBREF_EXPECTED_IMAGE_DIGEST: ${FBREF_ACCEPTANCE_AIRFLOW_IMAGE:?"
        in proxy
    )
    assert "FBREF_IMAGE_DIGEST: ${FBREF_ACCEPTANCE_AIRFLOW_IMAGE:?" in runner
    assert (
        "FBREF_ACCEPTANCE_OUTPUT_ROOT: /opt/airflow/logs/fbref_acceptance"
        in runner
    )
    assert acceptance.count(
        ":/opt/airflow/logs/fbref_acceptance"
    ) == 2
    assert (
        "PROXY_FILTER_CONTROL_TOKEN: ${FBREF_PROXY_CONTROL_TOKEN:?" in proxy
    )
    assert "SOFASCORE_PROXY_CONTROL_TOKEN" not in acceptance
    assert "${FBREF_PROXY_POOL_FILE:?" in proxy
    assert ":/run/secrets/fbref-proxys.txt:ro" in proxy
    assert "./proxys.txt" not in proxy
    assert "http://fbref_acceptance_proxy_filter:8900" in proxy
    assert "\n      - acceptance_proxy\n" in proxy
    assert "production_backend" not in proxy
    assert "production_storage" not in proxy
    assert "FBREF_PROXY_CONTROL_URL: http://fbref_acceptance_proxy_filter:8899" in (
        runner
    )
    assert "\n      - production_backend\n" in runner
    assert "\n      - production_storage\n" in runner
    assert "/var/lib/postgresql/data:rw,noexec,nosuid,size=1g" in acceptance
    assert "name: dp-backend" in acceptance
    assert "name: dp-storage" in acceptance


def test_fbref_acceptance_image_is_built_from_one_exact_git_archive():
    dockerfile = _ACCEPTANCE_DOCKERFILE_PATH.read_text()
    builder = _ACCEPTANCE_BUILD_SCRIPT_PATH.read_text()

    assert "COPY source.tar /tmp/fbref-acceptance-source.tar" in dockerfile
    assert "sha256sum -c -" in dockerfile
    assert "rm -rf /opt/airflow/dags /opt/airflow/scrapers" in dockerfile
    assert "verify_fbref_acceptance_image.py" in dockerfile
    assert "filter_proxy.py --help" in dockerfile
    assert "org.opencontainers.image.revision" in dockerfile
    assert "COPY dags" not in dockerfile
    assert "git -C \"$repo_root\" archive --format=tar \"$git_sha\"" in builder
    assert "dags scrapers scripts configs" in builder
    assert "${git_sha}:docker/images/airflow/Dockerfile.fbref-acceptance" in (
        builder
    )
    assert "runtime_base_id=" in builder
    assert "local/fbref-acceptance-base:" in builder
    assert "docker image tag \"$runtime_base_id\" \"$base_build_ref\"" in builder
    assert "docker image rm \"$base_build_ref\"" in builder
    assert "docker image inspect" in builder
    assert "FBREF_ACCEPTANCE_AIRFLOW_IMAGE=%s" in builder


def test_fbref_scheduler_image_requires_the_pinned_fontconfig():
    dockerfile = _SCHEDULER_DOCKERFILE_PATH.read_text()
    assert (
        "test -r /opt/fbref-camoufox/fontconfig/windows/fonts.conf"
        in dockerfile
    )


def test_fbref_lease_is_scoped_metered_and_host_restricted(mod):
    assert mod.FBREF_DAG_IDS == frozenset(
        {
            "dag_ingest_fbref",
            "dag_bootstrap_fbref",
            "dag_backfill_fbref",
            "dag_accept_fbref_bronze",
        }
    )
    assert mod._source_for_dag("dag_ingest_fbref") == "fbref"
    assert mod._source_for_dag("dag_bootstrap_fbref") == "fbref"
    assert mod._source_for_dag("dag_backfill_fbref") == "fbref"
    assert mod._source_for_dag("dag_accept_fbref_bronze") == "fbref"
    assert mod._source_for_dag("dag_replay_fbref_bronze") == ""
    lease = mod.Lease(
        lease_id="fbref-lease",
        token="secret",
        upstream=("proxy.example", 10000, "user", "password"),
        created_at=0.0,
        expires_at=9999999999.0,
        max_bytes=1000,
        dag_id="dag_ingest_fbref",
        run_id="run",
        source="fbref",
    )

    assert lease.report()["meter"] == "proxy_filter_provider_path_v2"
    assert mod._lease_host_allowed(lease, "fbref.com") is True
    assert mod._lease_host_allowed(lease, "www.fbref.com") is True
    assert mod._lease_host_allowed(lease, "challenges.cloudflare.com") is True
    assert mod._lease_host_allowed(lease, "api.ipify.org") is True
    assert mod._lease_host_allowed(lease, "ipinfo.io") is False
    assert mod._lease_host_allowed(lease, "example.com") is False
    assert mod._lease_url_budget_bytes(lease) == mod.DAGRUN_BUDGET_BYTES


def test_production_airflow_enables_safe_fbref_stage_janitor():
    compose = _COMPOSE_PATH.read_text()
    common = compose.split("x-airflow-common:", 1)[1].split("\nservices:", 1)[0]

    assert "FBREF_STAGE_JANITOR_MODE: ${FBREF_STAGE_JANITOR_MODE:-apply}" in common


# --- _dump --------------------------------------------------------------------


def test_dump_writes_expected_report_shape(mod, tmp_path):
    # Arrange — populate the module-level byte counters
    mod.up_bytes = defaultdict(int, {"sofifa.com": 1000})
    mod.down_bytes = defaultdict(int, {"sofifa.com": 1_048_576})  # 1 MiB down
    mod.conn_count = defaultdict(int, {"sofifa.com": 2})
    mod.blocked_count = defaultdict(int, {"doubleclick.net": 5})
    out = tmp_path / "report.json"
    # Act
    mod._dump(str(out), quiet=True)
    # Assert
    report = json.loads(out.read_text())
    assert {
        "total_mb",
        "daily",
        "leases",
        "dagruns",
        "allowed_hosts",
        "blocked_hosts",
    }.issubset(report)
    assert report["allowed_hosts"][0]["host"] == "sofifa.com"
    assert report["allowed_hosts"][0]["down_mb"] == pytest.approx(1.0, abs=0.01)
    assert report["blocked_hosts"] == [{"host": "doubleclick.net", "attempts": 5}]


def test_daily_budget_is_restored_from_atomic_report(mod, tmp_path):
    out = tmp_path / "report.json"
    today = mod._utc_day()
    out.write_text(
        json.dumps({"daily": {"day": today, "up_bytes": 123, "down_bytes": 456}})
    )
    mod._daily_day = ""
    mod._daily_up_bytes = mod._daily_down_bytes = mod._daily_reserved_bytes = 0

    mod._restore_daily_counter(str(out))

    assert mod._daily_day == today
    assert mod._daily_up_bytes == 123
    assert mod._daily_down_bytes == 456
    assert mod._daily_total_bytes() == 579


def test_budgeted_dump_exposes_exact_provider_bytes_for_canary(mod, tmp_path):
    mod.up_bytes = defaultdict(int, {"www.sofascore.com": 19})
    mod.down_bytes = defaultdict(int, {"www.sofascore.com": 23})
    mod.provider_budget_guard = object()
    mod.provider_budget_endpoint = "event"
    out = tmp_path / "provider.json"
    mod._dump(str(out), quiet=True)
    report = json.loads(out.read_text())
    assert report["total_provider_bytes"] == 42
    assert report["endpoint_provider_bytes"] == {"event": 42}
    assert report["endpoint_request_provider_bytes"] == {"event": [42]}


def test_pump_charges_provider_guard_before_forwarding(mod):
    class Reader:
        def __init__(self):
            self.chunks = [b"provider-bytes", b""]

        async def read(self, size):
            return self.chunks.pop(0)

    class Writer:
        def __init__(self):
            self.writes = []
            self.closed = False

        def write(self, chunk):
            self.writes.append(chunk)

        async def drain(self):
            return None

        def close(self):
            self.closed = True

    class Guard:
        def __init__(self):
            self.charges = []

        def consume(self, amount):
            self.charges.append(amount)

    writer = Writer()
    guard = Guard()
    counter = defaultdict(int)
    asyncio.run(mod._pump(Reader(), writer, "www.sofascore.com", counter, guard))
    assert guard.charges == [len(b"provider-bytes")]
    assert counter["www.sofascore.com"] == len(b"provider-bytes")
    assert writer.writes == [b"provider-bytes"]
    assert writer.closed is True


def test_pump_does_not_double_charge_a_preclaimed_provider_read(mod):
    class Reader:
        def __init__(self):
            self.chunks = [b"preclaimed", b""]

        async def read(self, size):
            return self.chunks.pop(0)

    class Writer:
        def __init__(self):
            self.writes = []

        def write(self, chunk):
            self.writes.append(chunk)

        async def drain(self):
            return None

        def close(self):
            return None

    class Guard:
        def __init__(self):
            self.claimed = []

        async def read_metered(self, reader, max_bytes):
            chunk = await reader.read(max_bytes)
            self.claimed.append(len(chunk))
            return chunk

        def consume(self, amount):
            raise AssertionError("preclaimed bytes must not be charged twice")

    writer = Writer()
    guard = Guard()
    counter = defaultdict(int)
    asyncio.run(mod._pump(Reader(), writer, "www.sofascore.com", counter, guard))
    assert guard.claimed == [len(b"preclaimed"), 0]
    assert counter["www.sofascore.com"] == len(b"preclaimed")
    assert writer.writes == [b"preclaimed"]


def test_whoscored_only_main_requires_production_class_before_runtime_io(
    mod,
    monkeypatch,
):
    args = SimpleNamespace(source_mode="whoscored-only")
    monkeypatch.setattr(mod.argparse.ArgumentParser, "parse_args", lambda self: args)
    validation_called = False

    def validate_runtime_contract(**_kwargs):
        nonlocal validation_called
        validation_called = True
        raise AssertionError("mutable runtime validation ran before class rejection")

    monkeypatch.setattr(
        mod._WHOSCORED_RUNTIME_CONTRACT,
        "validate_runtime_contract",
        validate_runtime_contract,
    )
    monkeypatch.setattr(
        mod._WHOSCORED_RUNTIME_CONTRACT,
        "require_production_runtime_class",
        lambda **_kwargs: (_ for _ in ()).throw(RuntimeError("actual=generic-v1")),
    )

    with pytest.raises(SystemExit, match="actual=generic-v1"):
        asyncio.run(mod.main())

    assert validation_called is False


def _initialize_real_metered_guard(mod, monkeypatch, tmp_path):
    """Run only filter_proxy's budget initialization and return its real guard."""
    from scripts.proxy_filter.budget import SharedBudgetLedger
    from tests.unit.scripts.test_sofascore_proxy_budget import _artifact

    artifact = _artifact(tmp_path / "canary.json")
    policy = mod.load_verified_policy(artifact, workload_class=MATCH_WORKLOAD_CLASS)
    ledger_path = tmp_path / "ledger.json"
    ledger = SharedBudgetLedger(ledger_path, policy)
    token, limit = ledger.reserve("logical-run", "event")
    args = SimpleNamespace(
        source_mode="shared-no-whoscored",
        listen="127.0.0.1:0",
        proxy_file=str(tmp_path / "unused-proxies.txt"),
        blocklist=None,
        out=str(tmp_path / "meter.json"),
        pidfile=str(tmp_path / "filter.pid"),
        budget_artifact=str(artifact),
        budget_ledger=str(ledger_path),
        budget_run_id="logical-run",
        budget_reservation_token=token,
        budget_endpoint="event",
        budget_workload_class=MATCH_WORKLOAD_CLASS,
    )
    monkeypatch.setattr(mod.argparse.ArgumentParser, "parse_args", lambda self: args)
    monkeypatch.setattr(
        mod,
        "_residential_manager",
        lambda **kwargs: (SimpleNamespace(total_count=1), "test pool"),
    )

    class Server:
        async def __aenter__(self):
            return self

        async def __aexit__(self, *args):
            return False

    async def start_server(*args, **kwargs):
        return Server()

    class StopEvent:
        def set(self):
            return None

        async def wait(self):
            return None

    monkeypatch.setattr(mod.asyncio, "start_server", start_server)
    monkeypatch.setattr(mod.asyncio, "Event", StopEvent)
    monkeypatch.setattr(
        mod.asyncio,
        "get_running_loop",
        lambda: SimpleNamespace(add_signal_handler=lambda *args: None),
    )

    def discard_background(coro):
        coro.close()
        return None

    monkeypatch.setattr(mod.asyncio, "ensure_future", discard_background)
    monkeypatch.setattr(
        mod._WHOSCORED_RUNTIME_CONTRACT,
        "validate_runtime_contract",
        lambda **_kwargs: {"code_tree_sha256": "a" * 64},
    )
    monkeypatch.setenv("PROXY_FILTER_CONTROL_TOKEN", mod.CONTROL_TOKEN)
    asyncio.run(mod.main())
    assert mod.provider_budget_guard is not None
    return mod.provider_budget_guard, ledger, token, limit


def test_real_metered_read_refunds_short_socket_reads_without_double_charge(
    mod,
    monkeypatch,
    tmp_path,
):
    guard, ledger, token, _ = _initialize_real_metered_guard(mod, monkeypatch, tmp_path)

    class ShortReader:
        def __init__(self):
            self.chunks = [b"short-read", b""]

        async def read(self, size):
            chunk = self.chunks.pop(0)
            assert len(chunk) <= size
            return chunk

    reader = ShortReader()
    first = asyncio.run(guard.read_metered(reader, 65536))
    assert first == b"short-read"
    assert ledger.snapshot("logical-run")["spent_provider_bytes"] == len(first)

    # EOF is also a short read: its entire preclaim must be refunded.
    assert asyncio.run(guard.read_metered(reader, 65536)) == b""
    assert ledger.snapshot("logical-run")["spent_provider_bytes"] == len(first)

    # finish validates the provider report against already-claimed bytes; it
    # must not add the same traffic a second time.
    assert ledger.finish(
        "logical-run", token, reported_provider_bytes=len(first)
    ) == len(first)
    assert ledger.snapshot("logical-run")["spent_provider_bytes"] == len(first)


def test_pump_forwards_only_the_atomic_final_provider_chunk(
    mod,
    monkeypatch,
    tmp_path,
):
    guard, ledger, _, limit = _initialize_real_metered_guard(mod, monkeypatch, tmp_path)

    class Reader:
        def __init__(self):
            self.payload = b"x" * (limit + 23)

        async def read(self, size):
            chunk, self.payload = self.payload[:size], self.payload[size:]
            return chunk

    class Writer:
        def __init__(self):
            self.writes = []
            self.closed = False

        def write(self, chunk):
            self.writes.append(chunk)

        async def drain(self):
            return None

        def close(self):
            self.closed = True

    writer = Writer()
    counter = defaultdict(int)
    asyncio.run(
        mod._pump(
            Reader(),
            writer,
            "www.sofascore.com",
            counter,
            guard,
        )
    )

    # The second read is refused before bytes move. _pump must not call
    # consume after a precharged read and cannot forward the 23-byte tail.
    assert sum(map(len, writer.writes)) == limit
    assert counter["www.sofascore.com"] == limit
    assert ledger.snapshot("logical-run")["spent_provider_bytes"] == limit
    assert writer.closed is True


# --- _pick_upstream / _acquire_upstream (idle-refresh rotation) ---------------


class _FakeProxy:
    def __init__(self, url):
        self.url = url


class _FakeManager:
    """Stand-in for ProxyManager: hands out a different proxy on each get_proxy()."""

    def __init__(self, urls):
        self._urls = list(urls)
        self.calls = 0

    def get_proxy(self):
        url = self._urls[self.calls % len(self._urls)]
        self.calls += 1
        return _FakeProxy(url)

    @property
    def total_count(self):
        return len(self._urls)


def test_pick_upstream_parses_creds_from_url(mod):
    # Arrange
    mgr = _FakeManager(["http://user:pass@pool.proxys.io:10000"])
    # Act
    host, port, user, pw = mod._pick_upstream(mgr)
    # Assert
    assert (host, port, user, pw) == ("pool.proxys.io", 10000, "user", "pass")


def test_acquire_upstream_draws_fresh_exit_when_idle(mod):
    # Arrange — pool of two exits, no tunnel open
    mgr = _FakeManager(
        ["http://u:p@pool.proxys.io:10000", "http://u:p@pool.proxys.io:10001"]
    )
    mod._current_up, mod._active = None, 0
    # Act / Assert — idle → draw the first exit
    assert mod._acquire_upstream(mgr)[1] == 10000
    assert mgr.calls == 1


def test_acquire_upstream_reuses_exit_while_a_tunnel_is_open(mod):
    # Arrange — the page's tunnel is open on the first exit (_active == 1)
    mgr = _FakeManager(
        ["http://u:p@pool.proxys.io:10000", "http://u:p@pool.proxys.io:10001"]
    )
    mod._current_up, mod._active = None, 0
    first = mod._acquire_upstream(mgr)  # mgr.calls -> 1
    mod._active = 1  # that tunnel stays open
    # Act — a sibling CONNECT in the SAME CF session asks for an upstream
    second = mod._acquire_upstream(mgr)
    # Assert — same exit IP (page + Turnstile on one IP = CF-safe), no new draw
    assert second == first
    assert mgr.calls == 1


def test_acquire_upstream_refreshes_for_next_session_once_idle(mod):
    # Arrange — session 1 ran and every tunnel closed (back to idle)
    mgr = _FakeManager(
        ["http://u:p@pool.proxys.io:10000", "http://u:p@pool.proxys.io:10001"]
    )
    mod._current_up, mod._active = None, 0
    mod._acquire_upstream(mgr)  # session 1 -> 10000, mgr.calls -> 1
    mod._active = 0  # all tunnels closed
    # Act — the next FlareSolverr session opens its first tunnel
    nxt = mod._acquire_upstream(mgr)
    # Assert — a fresh exit is drawn for the new session (#652 idle-refresh)
    assert nxt[1] == 10001
    assert mgr.calls == 2


# --- explicit sticky leases (legacy, credential-less callers) -----------------


def test_create_lease_pins_one_upstream_and_has_hard_limits(mod):
    mgr = _FakeManager(
        ["http://u:p@pool.proxys.io:10000", "http://u:p@pool.proxys.io:10001"]
    )
    mod.LEASES.clear()
    mod.LEASE_TOKENS.clear()
    mod._daily_day = ""
    mod._daily_up_bytes = mod._daily_down_bytes = 0

    lease = mod._create_lease(mgr, max_bytes=4096, ttl_seconds=30)

    assert lease.upstream[1] == 10000
    assert lease.max_bytes == 4096
    assert lease.expires_at > lease.created_at
    assert mod.LEASES[lease.lease_id] is lease
    assert mod.LEASE_TOKENS[lease.token] == lease.lease_id
    # Re-reading a lease never asks the pool for a different exit.
    assert lease.upstream[1] == 10000
    assert mgr.calls == 1


def test_proxy_basic_auth_resolves_only_the_matching_lease(mod):
    mgr = _FakeManager(["http://u:p@pool.proxys.io:10000"])
    mod.LEASES.clear()
    mod.LEASE_TOKENS.clear()
    mod._daily_day = ""
    mod._daily_up_bytes = mod._daily_down_bytes = 0
    lease = mod._create_lease(mgr, max_bytes=4096, ttl_seconds=30)
    encoded = base64.b64encode(f"lease:{lease.token}".encode()).decode()

    assert mod._lease_from_proxy_authorization(f"Basic {encoded}") is lease
    assert mod._lease_from_proxy_authorization("Basic bm9wZTpub3Bl") is None
    assert mod._lease_from_proxy_authorization(None) is None


def test_lease_accounting_is_exact_and_budget_is_fail_closed(mod):
    mgr = _FakeManager(["http://u:p@pool.proxys.io:10000"])
    mod.LEASES.clear()
    mod.LEASE_TOKENS.clear()
    mod.up_bytes = defaultdict(int)
    mod.down_bytes = defaultdict(int)
    mod._daily_day = ""
    mod._daily_up_bytes = mod._daily_down_bytes = 0
    lease = mod._create_lease(mgr, max_bytes=1000, ttl_seconds=30)

    mod._account_lease_bytes(lease, "www.whoscored.com", "up", 125)
    mod._account_lease_bytes(lease, "www.whoscored.com", "down", 875)

    assert lease.report()["up_bytes"] == 125
    assert lease.report()["down_bytes"] == 875
    assert lease.report()["total_bytes"] == 1000
    assert lease.report()["hosts"]["www.whoscored.com"] == {
        "up_bytes": 125,
        "down_bytes": 875,
    }
    assert lease.budget_exceeded is True
    assert mod._lease_remaining(lease) == 0


def test_closed_or_expired_lease_cannot_open_another_tunnel(mod):
    mgr = _FakeManager(["http://u:p@pool.proxys.io:10000"])
    mod.LEASES.clear()
    mod.LEASE_TOKENS.clear()
    mod._daily_day = ""
    mod._daily_up_bytes = mod._daily_down_bytes = 0
    lease = mod._create_lease(mgr, max_bytes=4096, ttl_seconds=30)

    lease.closed = True
    assert lease.usable is False
    assert mod._lease_remaining(lease) == 0
    lease.closed = False
    lease.expires_at = time.time() - 1
    assert lease.usable is False


def test_only_one_paid_lease_can_be_active(mod):
    mgr = _FakeManager(
        ["http://u:p@pool.proxys.io:10000", "http://u:p@pool.proxys.io:10001"]
    )
    # The shipped default is now a configurable pool of parallel leases (with
    # per-source serialization); pinning it back to one proves the global
    # concurrency ceiling is still enforced and still fails closed.
    mod.MAX_ACTIVE_LEASES = 1
    first = mod._create_lease(mgr, max_bytes=1000, ttl_seconds=30)

    with pytest.raises(RuntimeError, match="concurrency"):
        mod._create_lease(mgr, max_bytes=1000, ttl_seconds=30)

    first.closed = True
    second = mod._create_lease(mgr, max_bytes=1000, ttl_seconds=30)
    assert second.upstream[1] == 10001


def test_control_plane_paid_lease_requires_airflow_identity(mod):
    mgr = _FakeManager(["http://u:p@pool.proxys.io:10000"])

    with pytest.raises(ValueError, match="dag_id, run_id, task_id"):
        mod._create_lease(
            mgr,
            max_bytes=1000,
            ttl_seconds=30,
            metadata={"canonical_url": "https://www.whoscored.com/x"},
            require_context=True,
        )


def test_canonical_paid_url_keeps_and_sorts_full_query(mod):
    assert (
        mod._canonical_url(
            "HTTPS://WWW.WHOSCORED.COM/Matches/1/Live?z=2&a=&a=1#ignored"
        )
        == "https://www.whoscored.com/Matches/1/Live?a=&a=1&z=2"
    )


def test_dagrun_and_canonical_url_budgets_are_shared_across_leases(mod):
    mgr = _FakeManager(["http://u:p@pool.proxys.io:10000"])
    mod.DAGRUN_BUDGET_BYTES = 1000
    mod.URL_BUDGET_BYTES = 600
    metadata = {
        "dag_id": "dag",
        "run_id": "run",
        "task_id": "task-a",
        "canonical_url": "https://www.whoscored.com/Matches/1/Live?x=1",
    }
    first = mod._create_lease(mgr, max_bytes=1000, ttl_seconds=30, metadata=metadata)
    assert first.max_bytes == 600
    mod._account_lease_bytes(first, "www.whoscored.com", "down", 600)
    first.closed = True

    with pytest.raises(RuntimeError, match="budget exhausted"):
        mod._create_lease(mgr, max_bytes=1000, ttl_seconds=30, metadata=metadata)

    second = mod._create_lease(
        mgr,
        max_bytes=1000,
        ttl_seconds=30,
        metadata={
            **metadata,
            "task_id": "task-b",
            "canonical_url": "https://www.whoscored.com/Matches/2/Live",
        },
    )
    assert second.max_bytes == 400


@pytest.mark.parametrize(
    "dag_id",
    [
        "dag_ingest_transfermarkt",
        "dag_discover_transfermarkt_registry",
    ],
)
def test_transfermarkt_dagruns_require_an_explicit_separate_cap(mod, dag_id):
    mgr = _FakeManager(["http://u:p@pool.proxys.io:10000"])
    mod.URL_BUDGET_BYTES = 24 * 1024 * 1024
    metadata = {
        "dag_id": dag_id,
        "run_id": "run",
        "task_id": "task",
        "canonical_url": "https://www.transfermarkt.com/x",
    }

    with pytest.raises(RuntimeError, match="source-scoped authorization"):
        mod._create_lease(
            mgr,
            max_bytes=15_728_640,
            ttl_seconds=3600,
            metadata=metadata,
        )

    mod.TRANSFERMARKT_DAGRUN_BUDGET_BYTES = 15_728_640
    lease = mod._create_lease(
        mgr,
        max_bytes=15_728_640,
        ttl_seconds=3600,
        metadata=metadata,
    )

    assert mod.DAGRUN_BUDGET_BYTES == 8_000_000
    assert mod._dagrun_budget_bytes("dag_ingest_whoscored") == 0
    assert mod._dagrun_budget_bytes(dag_id) == 15_728_640
    assert lease.max_bytes == 15_728_640
    assert lease.report()["dagrun_budget_bytes"] == 15_728_640


def test_paid_lease_rejects_ttl_above_configured_hour(mod):
    mgr = _FakeManager(["http://u:p@pool.proxys.io:10000"])

    lease = mod._create_lease(mgr, max_bytes=1000, ttl_seconds=3600)
    assert lease.expires_at > time.time() + 3500
    lease.closed = True

    with pytest.raises(ValueError, match="ttl_seconds must be in 1..3600"):
        mod._create_lease(mgr, max_bytes=1000, ttl_seconds=3601)


def test_durable_byte_ledger_restores_shared_run_and_url_usage(mod):
    mgr = _FakeManager(["http://u:p@pool.proxys.io:10000"])
    metadata = {
        "dag_id": "dag",
        "run_id": "run",
        "task_id": "task",
        "canonical_url": "https://www.whoscored.com/Matches/1/Live",
    }
    lease = mod._create_lease(mgr, max_bytes=1000, ttl_seconds=30, metadata=metadata)
    mod._account_lease_bytes(lease, "www.whoscored.com", "up", 125)
    mod._account_lease_bytes(lease, "www.whoscored.com", "down", 375)

    mod._run_up_bytes.clear()
    mod._run_down_bytes.clear()
    mod._url_up_bytes.clear()
    mod._url_down_bytes.clear()
    restored = mod._restore_budget_ledger(mod.LEDGER_PATH, restore_daily=False)

    assert restored == 2
    assert mod._run_total_bytes("dag/run") == 500
    assert (
        mod._url_total_bytes("dag/run", "https://www.whoscored.com/Matches/1/Live")
        == 500
    )


def test_corrupt_paid_byte_ledger_fails_closed_on_restore(mod):
    Path(mod.LEDGER_PATH).write_text("{broken\n")

    with pytest.raises(RuntimeError, match="line 1"):
        mod._restore_budget_ledger(mod.LEDGER_PATH, restore_daily=False)


def test_oversized_paid_byte_ledger_event_fails_closed_on_restore(mod):
    mod.MAX_LEDGER_EVENT_BYTES = 32
    Path(mod.LEDGER_PATH).write_bytes(b'{"value":"' + b"x" * 80)

    with pytest.raises(RuntimeError, match="line 1"):
        mod._restore_budget_ledger(mod.LEDGER_PATH, restore_daily=False)


# --- authenticated production leases -----------------------------------------


def _sofascore_context(**values):
    budget = int(values.pop("budget", 4096))
    artifact_id = str(values.pop("artifact_id", "a" * 64))
    context = {
        "source": "sofascore",
        "dag_id": "dag_ingest_sofascore",
        "run_id": "scheduled__2026-07-11::season",
        "task_id": "capture_match_batch_00000",
        "canonical_url": "https://www.sofascore.com/",
        "scope": "match",
        "capture_scope": "competition-season",
        "entity": "17/76986",
    }
    context.update(values)
    phase = context["run_id"].rsplit("::", 1)[-1]
    allocation_scope = (
        "season" if phase == "season" else "player" if phase == "players" else "match"
    )
    workload_class = (
        "season_test_shape"
        if phase == "season"
        else PLAYER_WORKLOAD_CLASS
        if phase == "players"
        else MATCH_WORKLOAD_CLASS
    )
    identity = (
        f"{context['dag_id']}\0{context['run_id']}\0{context['task_id']}\0{budget}"
    )
    allocation = WorkloadAllocation(
        allocation_id="alloc-" + hashlib.sha256(identity.encode()).hexdigest()[:32],
        task_id=context["task_id"],
        scope=allocation_scope,
        workload_class=workload_class,
        batch_index=0,
        units=("1",),
        budget_bytes=budget,
    )
    plan = _signed_plan(
        artifact_id=artifact_id,
        dag_id=context["dag_id"],
        run_id=context["run_id"],
        player_universe_ids=(("1",) if phase == "players" else ()),
        allocations=(allocation,),
        control_token="c" * 32,
    )
    context.update(
        scope=allocation_scope,
        workload_plan=plan.to_dict(),
        allocation_id=allocation.allocation_id,
        allocation=allocation.to_dict(),
        attempt_id="1",
    )
    return context


def _sofascore_canary_context(**values):
    context = {
        "source": "sofascore_canary",
        "dag_id": "dag_canary_sofascore_proxy",
        "run_id": "manual__cold-canary-01",
        "task_id": "capture_fixed_cohort",
        "canonical_url": "https://www.sofascore.com/",
        "scope": "25_matches_50_players",
        "entity": "cold",
    }
    context.update(values)
    return context


def test_sofascore_lease_is_disabled_without_verified_canary_budget(mod):
    mgr = _FakeManager(["http://provider-user:provider-pass@pool.invalid:10000"])
    mod.SOFASCORE_DAGRUN_BUDGET_BYTES = 0

    with pytest.raises(RuntimeError, match="verified canary required"):
        mod._create_lease(
            mgr,
            max_bytes=4096,
            ttl_seconds=30,
            metadata=_sofascore_context(),
            require_context=True,
        )

    # Fail before selecting or opening any paid upstream.
    assert mgr.calls == 0


def test_production_rejects_missing_or_tampered_signed_plan_before_upstream(mod):
    mgr = _FakeManager(["http://u:p@pool.invalid:10000"])
    mod.SOFASCORE_DAGRUN_BUDGET_BYTES = 4096
    missing = _sofascore_context()
    missing.pop("workload_plan")
    with pytest.raises(mod.WorkloadPlanError):
        mod._create_lease(
            mgr,
            max_bytes=4096,
            ttl_seconds=30,
            metadata=missing,
            require_context=True,
        )
    tampered = _sofascore_context()
    tampered["workload_plan"]["signature"] = "0" * 64
    with pytest.raises(mod.WorkloadPlanError):
        mod._create_lease(
            mgr,
            max_bytes=4096,
            ttl_seconds=30,
            metadata=tampered,
            require_context=True,
        )
    assert mgr.calls == 0


def test_signed_allocation_is_concurrent_safe_and_retry_uses_remaining(mod):
    mgr = _FakeManager(["http://u:p@pool.invalid:10000"])
    mod.SOFASCORE_DAGRUN_BUDGET_BYTES = 1000
    context = _sofascore_context(budget=1000)
    first = mod._create_lease(
        mgr,
        max_bytes=1000,
        ttl_seconds=30,
        metadata=context,
        require_context=True,
    )
    with pytest.raises(RuntimeError, match="concurrency"):
        mod._create_lease(
            mgr,
            max_bytes=1000,
            ttl_seconds=30,
            metadata={**context, "attempt_id": "concurrent"},
            require_context=True,
        )
    boundary = mod._begin_endpoint_request(first, "event")
    mod._account_lease_bytes(first, "www.sofascore.com", "down", 125)
    mod._finish_endpoint_request(first, boundary)
    report = asyncio.run(
        mod._close_lease(
            first,
            completed=False,
            endpoint_request_provider_bytes={"event": [125]},
        )
    )
    assert report["plan_digest"] == context["workload_plan"]["plan_digest"]
    assert report["allocation_id"] == context["allocation_id"]
    assert report["endpoint_request_provider_bytes"] == {"event": [125]}
    retry = mod._create_lease(
        mgr,
        max_bytes=1000,
        ttl_seconds=30,
        metadata={**context, "attempt_id": "retry-2", "try_number": 2},
        require_context=True,
    )
    assert retry.max_bytes == 875
    assert mgr.calls == 2


def test_ttl_reaps_abandoned_claim_and_retry_needs_no_sidecar_restart(mod, monkeypatch):
    clock = [1_000.0]
    monkeypatch.setattr(mod, "_wall_time", lambda: clock[0])
    mgr = _FakeManager(["http://u:p@pool.invalid:10000"])
    mod.SOFASCORE_DAGRUN_BUDGET_BYTES = 1000
    context = _sofascore_context(budget=1000)
    first = mod._create_lease(
        mgr,
        max_bytes=1000,
        ttl_seconds=30,
        metadata=context,
        require_context=True,
    )

    clock[0] = 1_031.0
    retry = mod._create_lease(
        mgr,
        max_bytes=1000,
        ttl_seconds=30,
        metadata={**context, "attempt_id": "retry-after-ttl"},
        require_context=True,
    )

    assert first.expired is True
    assert first.closed is True
    assert first.allocation_finished is True
    assert first.close_recorded is True
    assert retry.max_bytes == 1000
    assert retry.allocation_claim.spent_provider_bytes == 0
    assert mgr.calls == 2
    wal = [
        json.loads(line)
        for line in Path(mod.SOFASCORE_ALLOCATION_WAL_PATH).read_text().splitlines()
    ]
    expired = [
        event
        for event in wal
        if event["event_type"] == "allocation_finished" and event.get("expired") is True
    ]
    assert len(expired) == 1
    assert expired[0]["lease_id"] == first.lease_id
    assert expired[0]["completed"] is False


def test_ttl_preserves_open_endpoint_bytes_and_retries_only_remainder(mod, monkeypatch):
    clock = [2_000.0]
    monkeypatch.setattr(mod, "_wall_time", lambda: clock[0])
    mgr = _FakeManager(["http://u:p@pool.invalid:10000"])
    mod.SOFASCORE_DAGRUN_BUDGET_BYTES = 1000
    context = _sofascore_context(budget=1000)
    first = mod._create_lease(
        mgr,
        max_bytes=1000,
        ttl_seconds=30,
        metadata=context,
        require_context=True,
    )
    mod._begin_endpoint_request(first, "lineups")
    mod._account_lease_bytes(first, "www.sofascore.com", "down", 125)

    clock[0] = 2_031.0
    retry = mod._create_lease(
        mgr,
        max_bytes=1000,
        ttl_seconds=30,
        metadata={**context, "attempt_id": "retry-open-endpoint-after-ttl"},
        require_context=True,
    )

    assert first.current_request_id == ""
    assert first.endpoint_request_provider_bytes == {"lineups": [125]}
    assert retry.max_bytes == 875
    assert retry.allocation_claim.spent_provider_bytes == 125
    assert mod._run_total_bytes(first.dagrun_key) == 125
    plan = mod.SignedDagRunPlan.from_dict(
        context["workload_plan"], control_token=mod.CONTROL_TOKEN
    )
    allocation = mod._allocation_ledger().snapshot(plan)["allocations"][
        context["allocation_id"]
    ]
    assert allocation["spent_provider_bytes"] == 125
    assert (
        allocation["active_claim"]["attempt_id_hash"]
        == hashlib.sha256(b"retry-open-endpoint-after-ttl").hexdigest()
    )
    assert allocation["lease_stats"][-1]["endpoint_request_provider_bytes"] == {
        "lineups": [125]
    }
    assert allocation["lease_stats"][-1]["completed"] is False
    parent = json.loads(Path(mod.SOFASCORE_PARENT_ENVELOPE_PATH).read_text())
    parent_run = next(iter(parent["runs"].values()))
    assert parent_run["spent_provider_bytes"] == 125


def test_ttl_with_active_provider_state_revokes_and_never_releases_claim(
    mod, monkeypatch
):
    clock = [3_000.0]
    monkeypatch.setattr(mod, "_wall_time", lambda: clock[0])
    mgr = _FakeManager(["http://u:p@pool.invalid:10000"])
    mod.SOFASCORE_DAGRUN_BUDGET_BYTES = 1000
    context = _sofascore_context(budget=1000)
    first = mod._create_lease(
        mgr,
        max_bytes=1000,
        ttl_seconds=30,
        metadata=context,
        require_context=True,
    )
    first.active_tunnels = 1
    first.reserved_bytes = 1
    clock[0] = 3_031.0

    with pytest.raises(RuntimeError, match="concurrency"):
        mod._create_lease(
            mgr,
            max_bytes=1000,
            ttl_seconds=30,
            metadata={**context, "attempt_id": "retry-before-drain"},
            require_context=True,
        )
    assert first.allocation_finished is False
    assert first.accounting_uncertain is True

    # A later in-process drain cannot prove that provider read-ahead contained
    # no unmetered bytes. The durable claim therefore remains unavailable for
    # retry instead of minting fresh allowance after the uncertainty latch.
    first.active_tunnels = 0
    first.reserved_bytes = 0
    with pytest.raises(RuntimeError, match="active attempt"):
        mod._create_lease(
            mgr,
            max_bytes=1000,
            ttl_seconds=30,
            metadata={**context, "attempt_id": "retry-after-drain"},
            require_context=True,
        )
    assert first.allocation_finished is False


def test_restart_recovers_endpoint_provenance_without_minting_bytes(mod):
    mgr = _FakeManager(["http://u:p@pool.invalid:10000"])
    mod.SOFASCORE_DAGRUN_BUDGET_BYTES = 1000
    context = _sofascore_context(budget=1000)
    lease = mod._create_lease(
        mgr,
        max_bytes=1000,
        ttl_seconds=30,
        metadata=context,
        require_context=True,
    )
    mod._begin_endpoint_request(lease, "lineups")
    mod._account_lease_bytes(lease, "www.sofascore.com", "down", 100)

    # Provider sockets vanish on process restart.  The private WAL retains the
    # claim token and active endpoint, while the allocation ledger retains bytes.
    mod.LEASES.clear()
    mod.LEASE_TOKENS.clear()
    mod.SOFASCORE_ALLOCATION_LEDGER = None
    mod._SOFASCORE_ALLOCATION_LEDGER_KEY = None
    assert mod._recover_allocation_wal() == 1

    plan = mod.SignedDagRunPlan.from_dict(
        context["workload_plan"], control_token=mod.CONTROL_TOKEN
    )
    snapshot = mod._allocation_ledger().snapshot(plan)
    allocation = snapshot["allocations"][context["allocation_id"]]
    assert allocation["active_claim"] is None
    assert allocation["spent_provider_bytes"] == 100
    assert allocation["lease_stats"][-1]["endpoint_request_provider_bytes"] == {
        "lineups": [100]
    }
    retry = mod._create_lease(
        mgr,
        max_bytes=1000,
        ttl_seconds=30,
        metadata={**context, "attempt_id": "retry-after-restart"},
        require_context=True,
    )
    assert retry.max_bytes == 900


def test_parent_envelope_sums_three_phases_and_stops_before_crossing(mod):
    mgr = _FakeManager(["http://u:p@pool.invalid:10000"])
    mod.SOFASCORE_DAGRUN_BUDGET_BYTES = 1000
    base = "scheduled__parent-envelope"
    season_context = _sofascore_context(run_id=f"{base}::season", budget=300)
    season = mod._create_lease(
        mgr,
        max_bytes=300,
        ttl_seconds=30,
        metadata=season_context,
        require_context=True,
    )
    season_boundary = mod._begin_endpoint_request(season, "schedule")
    mod._account_lease_bytes(season, "www.sofascore.com", "down", 300)
    mod._finish_endpoint_request(season, season_boundary)
    asyncio.run(
        mod._close_lease(
            season,
            completed=False,
            endpoint_request_provider_bytes={"schedule": [300]},
        )
    )

    target_context = _sofascore_context(run_id=f"{base}::targets", budget=400)
    target = mod._create_lease(
        mgr,
        max_bytes=400,
        ttl_seconds=30,
        metadata=target_context,
        require_context=True,
    )
    assert target.parent_run_cap_bytes == 700
    assert target.parent_run_spent_provider_bytes == 300
    target_boundary = mod._begin_endpoint_request(target, "event")
    mod._account_lease_bytes(target, "www.sofascore.com", "down", 400)
    mod._finish_endpoint_request(target, target_boundary)
    asyncio.run(
        mod._close_lease(
            target,
            completed=False,
            endpoint_request_provider_bytes={"event": [400]},
        )
    )

    player_context = _sofascore_context(run_id=f"{base}::players", budget=300)
    player = mod._create_lease(
        mgr,
        max_bytes=300,
        ttl_seconds=30,
        metadata=player_context,
        require_context=True,
    )
    assert player.parent_run_cap_bytes == 1000
    assert player.parent_run_spent_provider_bytes == 700
    player_boundary = mod._begin_endpoint_request(player, "player_profile")
    mod._account_lease_bytes(player, "www.sofascore.com", "down", 299)
    reserved = mod._reserve_lease_bytes(player, 10)
    assert reserved == 1
    mod._release_lease_reservation(player, reserved)
    with pytest.raises(mod.ParentEnvelopeBudgetExceeded):
        mod._account_lease_bytes(player, "www.sofascore.com", "down", 2)
    mod._finish_endpoint_request(player, player_boundary)
    asyncio.run(
        mod._close_lease(
            player,
            completed=False,
            endpoint_request_provider_bytes={"player_profile": [299]},
        )
    )

    retry = mod._create_lease(
        mgr,
        max_bytes=300,
        ttl_seconds=30,
        metadata={**player_context, "attempt_id": "retry"},
        require_context=True,
    )
    assert retry.max_bytes == 1
    assert retry.parent_run_cap_bytes == 1000
    assert retry.parent_run_spent_provider_bytes == 999
    asyncio.run(mod._close_lease(retry))

    changed_player = _sofascore_context(run_id=f"{base}::players", budget=301)
    with pytest.raises(mod.ParentEnvelopeError, match="immutable players plan"):
        mod._create_lease(
            mgr,
            max_bytes=301,
            ttl_seconds=30,
            metadata=changed_player,
            require_context=True,
        )
    assert mgr.calls == 4


def test_target_first_noop_season_is_allowed_but_late_season_cannot_expand(mod):
    mgr = _FakeManager(["http://u:p@pool.invalid:10000"])
    mod.SOFASCORE_DAGRUN_BUDGET_BYTES = 500
    base = "manual__target-first"
    target_context = _sofascore_context(run_id=f"{base}::targets", budget=500)
    target = mod._create_lease(
        mgr,
        max_bytes=500,
        ttl_seconds=30,
        metadata=target_context,
        require_context=True,
    )
    assert target.parent_run_cap_bytes == 500
    asyncio.run(mod._close_lease(target))

    with pytest.raises(mod.ParentEnvelopeError, match="target-first"):
        mod._create_lease(
            mgr,
            max_bytes=100,
            ttl_seconds=30,
            metadata=_sofascore_context(run_id=f"{base}::season", budget=100),
            require_context=True,
        )
    assert mgr.calls == 1


def test_later_player_phase_cannot_be_followed_by_a_new_match_phase(mod):
    mgr = _FakeManager(["http://u:p@pool.invalid:10000"])
    mod.SOFASCORE_DAGRUN_BUDGET_BYTES = 200
    base = "manual__player-first"
    player = mod._create_lease(
        mgr,
        max_bytes=100,
        ttl_seconds=30,
        metadata=_sofascore_context(
            run_id=f"{base}::players",
            budget=100,
        ),
        require_context=True,
    )
    asyncio.run(mod._close_lease(player))

    with pytest.raises(mod.ParentEnvelopeError, match="cannot expand"):
        mod._create_lease(
            mgr,
            max_bytes=100,
            ttl_seconds=30,
            metadata=_sofascore_context(
                run_id=f"{base}::targets",
                budget=100,
            ),
            require_context=True,
        )
    assert mgr.calls == 1


@pytest.mark.parametrize(
    "phase,bad_scope",
    [("season", "match"), ("targets", "player"), ("players", "match")],
)
def test_parent_envelope_rejects_mislabeled_phase_allocations(mod, phase, bad_scope):
    mgr = _FakeManager(["http://u:p@pool.invalid:10000"])
    mod.SOFASCORE_DAGRUN_BUDGET_BYTES = 100
    context = _sofascore_context(
        run_id=f"manual__bad-phase-{phase}::{phase}", budget=100
    )
    original = context["allocation"]
    allocation = WorkloadAllocation(
        allocation_id=original["allocation_id"],
        task_id=original["task_id"],
        scope=bad_scope,
        workload_class=original["class"],
        batch_index=original["batch_index"],
        units=tuple(original["units"]),
        budget_bytes=original["budget"],
    )
    plan = _signed_plan(
        artifact_id="a" * 64,
        dag_id=context["dag_id"],
        run_id=context["run_id"],
        player_universe_ids=(("1",) if bad_scope == "player" else ()),
        allocations=(allocation,),
        control_token="c" * 32,
    )
    context.update(
        scope=bad_scope,
        workload_plan=plan.to_dict(),
        allocation=allocation.to_dict(),
    )
    with pytest.raises(mod.ParentEnvelopeError, match="phase plan"):
        mod._create_lease(
            mgr,
            max_bytes=100,
            ttl_seconds=30,
            metadata=context,
            require_context=True,
        )
    assert mgr.calls == 0


def test_sofascore_source_cannot_bypass_budget_with_another_dag_id(mod):
    mgr = _FakeManager(["http://u:p@pool.invalid:10000"])
    mod.SOFASCORE_DAGRUN_BUDGET_BYTES = 4096

    with pytest.raises(ValueError, match="source does not match dag_id"):
        mod._create_lease(
            mgr,
            max_bytes=4096,
            ttl_seconds=30,
            metadata=_sofascore_context(dag_id="dag_ingest_whoscored"),
            require_context=True,
        )

    assert mgr.calls == 0


def test_explicit_canary_bootstraps_artifact_but_never_authorizes_production(
    mod,
    tmp_path,
):
    from tests.unit.scripts.test_sofascore_proxy_budget import _artifact

    mgr = _FakeManager(
        [
            "http://u:p@pool.invalid:10000",
            "http://u:p@pool.invalid:10001",
        ]
    )
    mod.SOFASCORE_DAGRUN_BUDGET_BYTES = 0
    mod.SOFASCORE_BUDGET_ARTIFACT_ID = ""
    mod.SOFASCORE_CANARY_HARD_CAP_BYTES = 4096
    mod.SOFASCORE_CANARY_POLICY_ID = mod._canary_policy_id(4096)

    canary = mod._create_lease(
        mgr,
        max_bytes=4096,
        ttl_seconds=30,
        metadata=_sofascore_canary_context(),
        require_context=True,
    )
    assert canary.source == "sofascore_canary"
    assert canary.report()["budget_artifact_id"] == mod.SOFASCORE_CANARY_POLICY_ID
    assert len(mod.SOFASCORE_CANARY_POLICY_ID) == 64
    assert mod.SOFASCORE_CANARY_POLICY_ID in Path(mod.LEDGER_PATH).read_text()
    canary.closed = True

    with pytest.raises(RuntimeError, match="verified canary required"):
        mod._create_lease(
            mgr,
            max_bytes=4096,
            ttl_seconds=30,
            metadata=_sofascore_context(),
            require_context=True,
        )

    # Twenty complete cold observations produce the independent reviewed
    # artifact which, and only which, unlocks the production DAG.
    artifact_path = _artifact(tmp_path / "canary.json", runs=20)
    policy = mod.load_verified_workload_policy(artifact_path)
    match_policy = policy.classes[MATCH_WORKLOAD_CLASS]
    assert match_policy.sample_count == 20
    mod.SOFASCORE_DAGRUN_BUDGET_BYTES = match_policy.hard_task_bytes
    mod.SOFASCORE_BUDGET_ARTIFACT_ID = policy.artifact_id
    production = mod._create_lease(
        mgr,
        max_bytes=match_policy.hard_task_bytes,
        ttl_seconds=30,
        metadata=_sofascore_context(
            budget=match_policy.hard_task_bytes,
            artifact_id=policy.artifact_id,
        ),
        require_context=True,
    )
    assert production.source == "sofascore"
    assert production.report()["budget_artifact_id"] == policy.artifact_id
    assert policy.artifact_id != mod.SOFASCORE_CANARY_POLICY_ID


def test_sofascore_lease_pins_upstream_and_uses_basic_token_auth(mod, caplog):
    mgr = _FakeManager(
        [
            "http://provider-user:provider-pass@pool.invalid:10000",
            "http://provider-user:provider-pass@pool.invalid:10001",
        ]
    )
    mod.SOFASCORE_DAGRUN_BUDGET_BYTES = 4096
    lease = mod._create_lease(
        mgr,
        max_bytes=4096,
        ttl_seconds=30,
        metadata=_sofascore_context(),
        require_context=True,
    )
    encoded = base64.b64encode(f"lease:{lease.token}".encode()).decode()

    assert lease.upstream[1] == 10000
    assert mod._lease_from_proxy_authorization(f"Basic {encoded}") is lease
    assert mod._lease_from_proxy_authorization("Basic bm9wZTpub3Bl") is None
    assert mgr.calls == 1
    assert lease.report()["source"] == "sofascore"
    assert lease.report()["upstream_fingerprint"]
    assert lease.token not in repr(lease)
    assert "provider-user" not in repr(lease)
    assert "provider-pass" not in repr(lease)
    assert "provider-user" not in caplog.text
    assert "provider-pass" not in caplog.text
    assert lease.token not in caplog.text
    report_path = Path(mod.LEDGER_PATH).with_name("report.json")
    mod._dump(str(report_path), quiet=True)
    serialized_report = report_path.read_text()
    assert "provider-user" not in serialized_report
    assert "provider-pass" not in serialized_report
    assert lease.token not in serialized_report


def test_v1_lease_control_contract_returns_token_and_authenticated_stats(mod):
    mgr = _FakeManager(["http://u:p@pool.invalid:10000"])
    mod.SOFASCORE_DAGRUN_BUDGET_BYTES = 4096
    mod.SOFASCORE_BUDGET_ARTIFACT_ID = "a" * 64
    request = json.dumps(
        {
            **_sofascore_context(),
            "max_bytes": 4096,
            "ttl_seconds": 30,
        }
    ).encode()

    class Reader:
        async def readexactly(self, length):
            assert length == len(request)
            return request

    class Writer:
        def __init__(self):
            self.payload = bytearray()

        def write(self, value):
            self.payload.extend(value)

        async def drain(self):
            return None

        def close(self):
            return None

    created_writer = Writer()
    handled = asyncio.run(
        mod._handle_control(
            "POST",
            "/v1/leases",
            {
                "content-length": str(len(request)),
                "x-proxy-control-token": mod.CONTROL_TOKEN,
            },
            Reader(),
            created_writer,
            mgr,
        )
    )
    created_head, created_body = bytes(created_writer.payload).split(b"\r\n\r\n", 1)
    created = json.loads(created_body)
    assert handled is True
    assert b"201 Created" in created_head
    assert created["proxy_url"] == "http://proxy_filter:8900"
    assert created["token"]
    assert "upstream" not in created

    stats_writer = Writer()
    asyncio.run(
        mod._handle_control(
            "GET",
            f"/v1/leases/{created['id']}/stats",
            {
                "authorization": f"Bearer {created['token']}",
                "x-proxy-control-token": mod.CONTROL_TOKEN,
            },
            Reader(),
            stats_writer,
            mgr,
        )
    )
    _, stats_body = bytes(stats_writer.payload).split(b"\r\n\r\n", 1)
    stats = json.loads(stats_body)
    assert stats["source"] == "sofascore"
    assert stats["total_bytes"] == 0
    assert stats["dagrun_budget_bytes"] == 4096
    assert stats["budget_artifact_id"] == "a" * 64
    assert created["token"] not in stats_body.decode()


def test_whoscored_control_post_returns_structured_guard_rejection(mod):
    mgr = _FakeManager(["http://u:p@pool.invalid:10000"])
    mod.WHOSCORED_PROVIDER_INVOICE_HARD_CAP_AVAILABLE = False
    request = json.dumps(
        {
            **_whoscored_campaign_context(mod, cap=1_000),
            "max_bytes": 1_000,
            "ttl_seconds": 30,
        }
    ).encode()

    class Reader:
        async def readexactly(self, length):
            assert length == len(request)
            return request

    class Writer:
        def __init__(self):
            self.payload = bytearray()

        def write(self, value):
            self.payload.extend(value)

        async def drain(self):
            return None

        def close(self):
            return None

    writer = Writer()
    handled = asyncio.run(
        mod._handle_control(
            "POST",
            "/v1/leases",
            {
                "content-length": str(len(request)),
                "x-proxy-control-token": mod.CONTROL_TOKEN,
            },
            Reader(),
            writer,
            mgr,
        )
    )
    head, body = bytes(writer.payload).split(b"\r\n\r\n", 1)

    assert handled is True
    assert b"409 Conflict" in head
    assert json.loads(body)["code"] == "campaign_rejected"
    assert mod.LEASES == {}
    assert mgr.calls == 0
    assert not Path(mod.WHOSCORED_CAMPAIGN_LEDGER_PATH).exists()


@pytest.mark.parametrize(
    ("operation", "arguments", "result_field"),
    [
        ("snapshot", {}, "campaign"),
        (
            "complete_allocation",
            {
                "allocation_id": "allocation-one",
                "dag_id": "dag_backfill_whoscored",
                "run_id": "manual__campaign-one",
                "task_id": "run_whoscored_proxy_canary",
                "attempt_id": "attempt-one",
                "report_sha256": "a" * 64,
                "request_ledger_sha256": "b" * 64,
            },
            "allocation",
        ),
        (
            "assert_exact_accounting",
            {
                "task_report_provider_bytes": 0,
                "request_ledger_provider_bytes": 0,
                "proxy_ledger_provider_bytes": 0,
                "require_complete": False,
            },
            "provider_billed_bytes",
        ),
        (
            "seal_for_reconciliation",
            {
                "dag_id": "dag_backfill_whoscored",
                "run_id": "manual__campaign-one",
                "provider_billed_bytes": 0,
                "attempt_accounting_sha256": "c" * 64,
            },
            "campaign",
        ),
        ("sealed_snapshot", {}, "campaign"),
    ],
)
def test_whoscored_campaign_control_rpc_has_five_exact_operations(
    mod, monkeypatch, operation, arguments, result_field
):
    mod.SOURCE_MODE = "whoscored-only"
    mod.CONTROL_TOKEN = "control-only-" + "c" * 32
    mod.WHOSCORED_PROXY_APPROVAL_HMAC_SECRET = "approval-only-" + "a" * 32
    mod.WHOSCORED_PROXY_LEDGER_HMAC_SECRET = "ledger-only-" + "l" * 32
    full = _whoscored_campaign_context(mod)
    context = {
        name: full[name]
        for name in (
            "dag_id",
            "run_id",
            "transport_policy",
            "proxy_campaign_approval",
            "proxy_campaign_id",
            "proxy_approval_id",
            "proxy_approval_sha256",
        )
    }
    calls = []

    class Ledger:
        def snapshot(self, approval):
            calls.append(("snapshot", approval.campaign_id))
            return {"status": "active"}

        def complete_allocation(self, approval, allocation_id, **values):
            calls.append(("complete_allocation", allocation_id, values))
            return {"completed": True}

        def assert_exact_accounting(self, approval, **values):
            calls.append(("assert_exact_accounting", approval.campaign_id, values))
            return 0

        def seal_for_reconciliation(self, approval, **values):
            calls.append(("seal_for_reconciliation", approval.campaign_id, values))
            return {"status": "sealed"}

        def sealed_snapshot(self, approval):
            calls.append(("sealed_snapshot", approval.campaign_id))
            return {"status": "sealed"}

    monkeypatch.setattr(mod, "_whoscored_campaign_ledger", lambda: Ledger())
    request = mod.canonical_json_bytes(
        {
            "schema_version": 1,
            "operation": operation,
            "context": context,
            "arguments": arguments,
        }
    )

    class Reader:
        async def readexactly(self, length):
            assert length == len(request)
            return request

    class Writer:
        def __init__(self):
            self.payload = bytearray()

        def write(self, value):
            self.payload.extend(value)

        async def drain(self):
            return None

        def close(self):
            return None

    writer = Writer()
    handled = asyncio.run(
        mod._handle_control(
            "POST",
            "/v1/whoscored/campaign-control",
            {
                "content-length": str(len(request)),
                "x-proxy-control-token": mod.CONTROL_TOKEN,
            },
            Reader(),
            writer,
            object(),
        )
    )
    head, body = bytes(writer.payload).split(b"\r\n\r\n", 1)

    assert handled is True
    assert b"200 OK" in head
    document = json.loads(body)
    assert document["operation"] == operation
    assert set(document["result"]) == {result_field}
    assert body == mod.canonical_json_bytes(document)
    assert calls and calls[0][0] == operation


def test_campaign_control_rejects_transfermarkt_token_before_reading_body(mod):
    mod.SOURCE_MODE = "whoscored-only"

    class Reader:
        async def readexactly(self, length):
            raise AssertionError("unauthorized campaign body must not be read")

    class Writer:
        def __init__(self):
            self.payload = bytearray()

        def write(self, value):
            self.payload.extend(value)

        async def drain(self):
            return None

        def close(self):
            return None

    writer = Writer()
    asyncio.run(
        mod._handle_control(
            "POST",
            "/v1/whoscored/campaign-control",
            {
                "content-length": "1",
                "x-proxy-control-token": mod.TRANSFERMARKT_CONTROL_TOKEN,
            },
            Reader(),
            writer,
            object(),
        )
    )

    assert b"401 Unauthorized" in writer.payload


def test_campaign_control_framing_rejects_duplicate_headers(mod):
    with pytest.raises(ValueError, match="ambiguous"):
        mod._strict_control_header_map(
            [b"Content-Length: 1\r\n", b"Content-Length: 2\r\n"]
        )


def test_common_control_token_cannot_impersonate_a_transfermarkt_dag(mod):
    mgr = _FakeManager(["http://u:p@pool.invalid:10000"])
    mod.TRANSFERMARKT_DAGRUN_BUDGET_BYTES = 15_728_640
    request = json.dumps(
        {
            "dag_id": "dag_ingest_transfermarkt",
            "run_id": "manual__one",
            "task_id": "run_transfermarkt",
            "canonical_url": "https://www.transfermarkt.com/x",
            "max_bytes": 1_000,
            "ttl_seconds": 30,
        }
    ).encode()

    class Reader:
        async def readexactly(self, length):
            assert length == len(request)
            return request

    class Writer:
        def __init__(self):
            self.payload = bytearray()

        def write(self, value):
            self.payload.extend(value)

        async def drain(self):
            return None

        def close(self):
            return None

    writer = Writer()
    asyncio.run(
        mod._handle_control(
            "POST",
            "/v1/leases",
            {
                "content-length": str(len(request)),
                "x-proxy-control-token": mod.CONTROL_TOKEN,
            },
            Reader(),
            writer,
            mgr,
        )
    )
    head, body = bytes(writer.payload).split(b"\r\n\r\n", 1)

    assert b"401 Unauthorized" in head
    assert json.loads(body)["error"] == "invalid control token"
    assert mod.LEASES == {}
    assert mgr.calls == 0


def test_fbref_auth_check_proves_meter_config_without_paid_lease(mod):
    mgr = _FakeManager(
        [
            "http://u:p@pool.invalid:10000",
            "http://u:p@pool.invalid:10001",
        ]
    )
    mod.DAILY_BUDGET_BYTES = 300_000_000
    mod.DAGRUN_BUDGET_BYTES = 104_857_600
    mod.URL_BUDGET_BYTES = 104_857_600
    mod.MAX_LEASE_BYTES = 104_857_600
    mod.MAX_LEASE_TTL_SECONDS = 7200
    mod.MAX_ACTIVE_LEASES = 1
    mod.LEASE_PROXY_URL = "http://fbref_proxy_filter:8900"

    class Reader:
        async def readexactly(self, _length):
            raise AssertionError("auth check must not read a request body")

    class Writer:
        def __init__(self):
            self.payload = bytearray()

        def write(self, value):
            self.payload.extend(value)

        async def drain(self):
            return None

        def close(self):
            return None

    writer = Writer()
    handled = asyncio.run(
        mod._handle_control(
            "GET",
            "/v1/auth-check",
            {"x-proxy-control-token": mod.CONTROL_TOKEN},
            Reader(),
            writer,
            mgr,
        )
    )
    head, body = bytes(writer.payload).split(b"\r\n\r\n", 1)
    report = json.loads(body)

    assert handled is True
    assert b"200 OK" in head
    assert report["meter"] == "proxy_filter_provider_path_v2"
    assert report["fbref_source_ready"] is True
    assert report["configured_pool_count"] == 2
    assert report["max_active_leases"] == 1
    assert mod.LEASES == {}
    assert mgr.calls == 0


def test_lease_creation_rejects_missing_control_token_without_state(mod):
    mgr = _FakeManager(["http://u:p@pool.invalid:10000"])

    class Reader:
        async def readexactly(self, length):
            raise AssertionError("unauthorized body must not be read")

    class Writer:
        def __init__(self):
            self.payload = bytearray()

        def write(self, value):
            self.payload.extend(value)

        async def drain(self):
            return None

        def close(self):
            return None

    writer = Writer()
    asyncio.run(
        mod._handle_control(
            "POST", "/v1/leases", {"content-length": "1"}, Reader(), writer, mgr
        )
    )

    assert b"401 Unauthorized" in writer.payload
    assert mod.LEASES == {}
    assert mgr.calls == 0


def test_sofascore_lease_host_scope_is_fail_closed(mod):
    mgr = _FakeManager(["http://u:p@pool.invalid:10000"])
    mod.SOFASCORE_DAGRUN_BUDGET_BYTES = 4096
    production = mod._create_lease(
        mgr,
        max_bytes=4096,
        ttl_seconds=30,
        metadata=_sofascore_context(),
        require_context=True,
    )

    assert mod._lease_host_allowed(production, "www.sofascore.com") is True
    assert mod._lease_host_allowed(production, "api.sofascore.com") is True
    assert mod._lease_host_allowed(production, "challenges.cloudflare.com") is True
    assert mod._lease_host_allowed(production, "evil.example") is False
    # Production leases MUST reach the geoip exit-probe host (#951): Camoufox's
    # geoip=True resolves the residential exit IP via api.ipify.org at browser
    # startup; blocking it aborted every production capture with InvalidProxy
    # before any data flowed. The canary lease already reaches it, so the
    # measured budget already carries the probe cost.
    assert mod._lease_host_allowed(production, "api.ipify.org") is True
    # Scope stays fail-closed: only the single exit-probe host is opened — the
    # other IP-echo fallbacks and arbitrary hosts remain blocked.
    assert mod._lease_host_allowed(production, "ipinfo.io") is False
    assert mod._lease_host_allowed(production, "checkip.amazonaws.com") is False


def test_authenticated_proxy_listener_rejects_missing_lease_before_dial(mod):
    mgr = _FakeManager(["http://u:p@pool.invalid:10000"])

    class Reader:
        def __init__(self):
            self.lines = [
                b"CONNECT www.sofascore.com:443 HTTP/1.1\r\n",
                b"Host: www.sofascore.com:443\r\n",
                b"\r\n",
            ]

        async def readline(self):
            return self.lines.pop(0)

    class Writer:
        def __init__(self):
            self.payload = bytearray()

        def write(self, value):
            self.payload.extend(value)

        async def drain(self):
            return None

        def close(self):
            return None

    writer = Writer()
    asyncio.run(mod.handle(Reader(), writer, mgr, require_lease=True))

    assert b"407 Proxy Authentication Required" in writer.payload
    assert mgr.calls == 0


@pytest.mark.parametrize(
    "extra_headers",
    [
        [b"X-Oversized: " + b"x" * (8 * 1024) + b"\r\n"],
        [b"X-Many: value\r\n"] * 65,
        [b"X-Aggregate: " + b"x" * 8000 + b"\r\n"] * 5,
    ],
    ids=("line", "count", "aggregate"),
)
def test_oversized_unauthenticated_head_is_rejected_before_provider_dial(
    mod, monkeypatch, extra_headers
):
    mgr = _FakeManager(["http://u:p@pool.invalid:10000"])
    reader = _ClientConnectReader(
        [b"CONNECT www.whoscored.com:443 HTTP/1.1\r\n", *extra_headers, b"\r\n"]
    )
    writer = _ClientWriter()

    async def forbidden_dial(*args, **kwargs):
        raise AssertionError("invalid unauthenticated head must never dial provider")

    monkeypatch.setattr(mod, "_open_upstream_connection", forbidden_dial)
    asyncio.run(mod.handle(reader, writer, mgr, require_lease=False))

    assert b"431 Request Header Fields Too Large" in writer.payload
    assert writer.closed is True
    assert mgr.calls == 0


def test_incomplete_unauthenticated_head_is_rejected_before_provider_dial(
    mod, monkeypatch
):
    mgr = _FakeManager(["http://u:p@pool.invalid:10000"])
    reader = _ClientConnectReader(
        [
            b"CONNECT www.whoscored.com:443 HTTP/1.1\r\n",
            b"Host: www.whoscored.com:443\r\n",
            b"",
        ]
    )
    writer = _ClientWriter()

    async def forbidden_dial(*args, **kwargs):
        raise AssertionError("incomplete unauthenticated head must never dial provider")

    monkeypatch.setattr(mod, "_open_upstream_connection", forbidden_dial)
    asyncio.run(mod.handle(reader, writer, mgr, require_lease=False))

    assert b"400 Bad Request" in writer.payload
    assert writer.closed is True
    assert mgr.calls == 0


def test_slow_unauthenticated_head_times_out_before_provider_dial(mod, monkeypatch):
    mgr = _FakeManager(["http://u:p@pool.invalid:10000"])
    monkeypatch.setattr(mod, "CLIENT_HEAD_TIMEOUT_SECONDS", 0.01)

    class SlowReader:
        def __init__(self):
            self.first = True

        async def readline(self):
            if self.first:
                self.first = False
                return b"CONNECT www.whoscored.com:443 HTTP/1.1\r\n"
            await asyncio.Event().wait()

    writer = _ClientWriter()

    async def forbidden_dial(*args, **kwargs):
        raise AssertionError("timed-out unauthenticated head must never dial provider")

    monkeypatch.setattr(mod, "_open_upstream_connection", forbidden_dial)
    asyncio.run(mod.handle(SlowReader(), writer, mgr, require_lease=False))

    assert b"408 Request Timeout" in writer.payload
    assert writer.closed is True
    assert mgr.calls == 0


def test_preauth_connection_limit_rejects_without_waiting_or_dialing(mod):
    mgr = _FakeManager(["http://u:p@pool.invalid:10000"])
    writer = _ClientWriter()

    async def exercise():
        mod._CLIENT_HEAD_SLOTS = asyncio.BoundedSemaphore(1)
        await mod._CLIENT_HEAD_SLOTS.acquire()
        try:
            await mod.handle(
                _ClientConnectReader(
                    [b"CONNECT www.whoscored.com:443 HTTP/1.1\r\n", b"\r\n"]
                ),
                writer,
                mgr,
                require_lease=False,
            )
        finally:
            mod._CLIENT_HEAD_SLOTS.release()

    asyncio.run(exercise())

    assert b"503 Service Unavailable" in writer.payload
    assert writer.closed is True
    assert mgr.calls == 0


def test_non_sofascore_leases_can_run_concurrently_up_to_configured_limit(mod):
    mgr = _FakeManager(
        [
            "http://u:p@pool.invalid:10000",
            "http://u:p@pool.invalid:10001",
            "http://u:p@pool.invalid:10002",
        ]
    )
    mod.MAX_ACTIVE_LEASES = 2
    mod.TRANSFERMARKT_DAGRUN_BUDGET_BYTES = 15_728_640
    first = mod._create_lease(
        mgr,
        max_bytes=1000,
        ttl_seconds=30,
        metadata={
            "dag_id": "dag_ingest_transfermarkt",
            "run_id": "run-a",
            "task_id": "task-a",
            "canonical_url": "https://www.transfermarkt.com/a",
        },
        require_context=True,
    )
    second = mod._create_lease(
        mgr,
        max_bytes=1000,
        ttl_seconds=30,
        metadata={
            "dag_id": "dag_ingest_transfermarkt",
            "run_id": "run-b",
            "task_id": "task-b",
            "canonical_url": "https://www.transfermarkt.com/b",
        },
        require_context=True,
    )

    assert first.source == "transfermarkt"
    assert second.source == "transfermarkt"
    assert mgr.calls == 2
    with pytest.raises(RuntimeError, match="concurrency"):
        mod._create_lease(
            mgr,
            max_bytes=1000,
            ttl_seconds=30,
            metadata={
                "dag_id": "dag_ingest_transfermarkt",
                "run_id": "run-c",
                "task_id": "task-c",
                "canonical_url": "https://www.transfermarkt.com/c",
            },
            require_context=True,
        )


def test_sofascore_production_and_canary_are_each_serial(mod):
    mgr = _FakeManager(
        [
            "http://u:p@pool.invalid:10000",
            "http://u:p@pool.invalid:10001",
        ]
    )
    mod.MAX_ACTIVE_LEASES = 4
    mod.SOFASCORE_DAGRUN_BUDGET_BYTES = 4096
    first = mod._create_lease(
        mgr,
        max_bytes=4096,
        ttl_seconds=30,
        metadata=_sofascore_context(),
        require_context=True,
    )
    with pytest.raises(RuntimeError, match="SofaScore paid-proxy concurrency"):
        mod._create_lease(
            mgr,
            max_bytes=4096,
            ttl_seconds=30,
            metadata=_sofascore_context(run_id="another-run::season"),
            require_context=True,
        )
    first.closed = True

    mod.SOFASCORE_CANARY_HARD_CAP_BYTES = 4096
    mod.SOFASCORE_CANARY_POLICY_ID = mod._canary_policy_id(4096)
    canary = mod._create_lease(
        mgr,
        max_bytes=4096,
        ttl_seconds=30,
        metadata=_sofascore_canary_context(),
        require_context=True,
    )
    with pytest.raises(RuntimeError, match="isolated serial"):
        mod.TRANSFERMARKT_DAGRUN_BUDGET_BYTES = 15_728_640
        mod._create_lease(
            mgr,
            max_bytes=1000,
            ttl_seconds=30,
            metadata={
                "dag_id": "dag_ingest_transfermarkt",
                "run_id": "run",
                "task_id": "task",
                "canonical_url": "https://www.transfermarkt.com/",
            },
            require_context=True,
        )
    assert canary.source == "sofascore_canary"


def test_exit_probe_host_is_available_to_sofascore_leases_not_anonymous(mod):
    mgr = _FakeManager(
        [
            "http://u:p@pool.invalid:10000",
            "http://u:p@pool.invalid:10001",
        ]
    )
    mod.SOFASCORE_DAGRUN_BUDGET_BYTES = 4096
    production = mod._create_lease(
        mgr,
        max_bytes=4096,
        ttl_seconds=30,
        metadata=_sofascore_context(),
        require_context=True,
    )
    production.closed = True
    mod.SOFASCORE_CANARY_HARD_CAP_BYTES = 4096
    mod.SOFASCORE_CANARY_POLICY_ID = mod._canary_policy_id(4096)
    canary = mod._create_lease(
        mgr,
        max_bytes=4096,
        ttl_seconds=30,
        metadata=_sofascore_canary_context(),
        require_context=True,
    )

    # The geoip exit-probe host is reachable by BOTH sofascore lease kinds
    # (#951): canary measured it, and production needs it for Camoufox
    # geoip=True at browser startup. An anonymous (no-lease) caller stays blocked.
    assert mod._lease_host_allowed(canary, "api.ipify.org") is True
    assert mod._lease_host_allowed(production, "api.ipify.org") is True
    assert mod._lease_host_allowed(None, "api.ipify.org") is False
    assert mod._lease_host_allowed(production, "www.sofascore.com") is True


def test_sofascore_dagrun_budget_is_shared_without_legacy_url_truncation(mod):
    mgr = _FakeManager(["http://u:p@pool.invalid:10000"])
    mod.SOFASCORE_DAGRUN_BUDGET_BYTES = 1000
    mod.URL_BUDGET_BYTES = 100  # legacy per-page cap must not cut warmed capture
    context = _sofascore_context(budget=1000)
    first = mod._create_lease(
        mgr,
        max_bytes=1000,
        ttl_seconds=30,
        metadata=context,
        require_context=True,
    )
    assert first.max_bytes == 1000
    assert first.report()["url_budget_bytes"] == 1000
    first_request = mod._begin_endpoint_request(first, "event")
    mod._account_lease_bytes(first, "www.sofascore.com", "down", 600)
    mod._finish_endpoint_request(first, first_request)
    asyncio.run(
        mod._close_lease(
            first,
            endpoint_request_provider_bytes={"event": [600]},
            completed=False,
        )
    )

    retry_context = {**context, "attempt_id": "2", "try_number": 2}
    second = mod._create_lease(
        mgr,
        max_bytes=1000,
        ttl_seconds=30,
        metadata=retry_context,
        require_context=True,
    )
    assert second.max_bytes == 400
    second_request = mod._begin_endpoint_request(second, "event")
    mod._account_lease_bytes(second, "www.sofascore.com", "up", 400)
    mod._finish_endpoint_request(second, second_request)
    asyncio.run(
        mod._close_lease(
            second,
            endpoint_request_provider_bytes={"event": [400]},
            completed=False,
        )
    )

    with pytest.raises(RuntimeError, match="budget exhausted"):
        mod._create_lease(
            mgr,
            max_bytes=1,
            ttl_seconds=30,
            metadata={**context, "attempt_id": "3", "try_number": 3},
            require_context=True,
        )


def test_close_ack_waits_for_tunnels_reservations_and_durable_ledger(mod):
    mgr = _FakeManager(["http://u:p@pool.invalid:10000"])
    mod.SOFASCORE_DAGRUN_BUDGET_BYTES = 1000
    lease = mod._create_lease(
        mgr,
        max_bytes=1000,
        ttl_seconds=30,
        metadata=_sofascore_context(),
        require_context=True,
    )
    lease.active_tunnels = 1
    lease.reserved_bytes = 10

    class Tunnel:
        def close(self):
            lease.active_tunnels = 0

    lease.tunnel_writers.add(Tunnel())

    pending = asyncio.run(mod._close_lease(lease))

    assert pending["closed"] is True
    assert pending["close_complete"] is False
    assert pending["active_tunnels"] == 0
    assert pending["reserved_bytes"] == 10
    assert lease.close_recorded is False

    lease.active_tunnels = 0
    lease.reserved_bytes = 0
    complete = asyncio.run(mod._close_lease(lease))

    assert complete["close_complete"] is True
    assert complete["active_tunnels"] == 0
    assert complete["reserved_bytes"] == 0
    assert lease.close_recorded is True


def test_lease_pump_pre_reads_only_the_remaining_provider_window(mod):
    mgr = _FakeManager(["http://u:p@pool.invalid:10000"])
    mod.SOFASCORE_DAGRUN_BUDGET_BYTES = 12
    lease = mod._create_lease(
        mgr,
        max_bytes=12,
        ttl_seconds=30,
        metadata=_sofascore_context(),
        require_context=True,
    )
    mod._begin_endpoint_request(lease, "event")

    class Reader:
        def __init__(self):
            self.payload = b"x" * 20
            self.read_sizes = []

        async def read(self, size):
            self.read_sizes.append(size)
            chunk, self.payload = self.payload[:size], self.payload[size:]
            return chunk

    class Writer:
        def __init__(self):
            self.writes = []
            self.closed = False

        def write(self, chunk):
            self.writes.append(chunk)

        async def drain(self):
            return None

        def close(self):
            self.closed = True

    reader = Reader()
    writer = Writer()
    asyncio.run(
        mod._pump(
            reader,
            writer,
            "www.sofascore.com",
            defaultdict(int),
            lease=lease,
            direction="down",
        )
    )

    # A provider read may never reserve the whole bidirectional tail while the
    # client upload leg is active. The fair window is recomputed after every
    # exact chunk and converges on the final hard-cap byte.
    assert reader.read_sizes == [6, 3, 2, 1]
    assert len(reader.payload) == 8
    assert b"".join(writer.writes) == b"x" * 12
    assert lease.down_bytes == 12
    assert lease.total_bytes == lease.max_bytes
    assert lease.budget_exceeded is True
    assert writer.closed is True


def test_blocking_client_leg_cannot_hold_provider_read_reservation(mod):
    mgr = _FakeManager(["http://u:p@pool.invalid:10000"])
    lease = _make_fbref_lease(mod, mgr, max_bytes=64)
    provider_payload = b"provider-can-progress"

    class BlockingClientReader:
        def __init__(self, started, release):
            self.started = started
            self.release = release

        async def read(self, _size):
            self.started.set()
            await self.release.wait()
            return b""

    class ProviderReader:
        def __init__(self, eof):
            self.payload = provider_payload
            self.eof = eof

        async def read(self, size):
            if self.payload:
                chunk, self.payload = self.payload[:size], self.payload[size:]
                return chunk
            self.eof.set()
            return b""

    class Writer:
        def __init__(self):
            self.payload = bytearray()
            self.closed = False

        def write(self, chunk):
            self.payload.extend(chunk)

        async def drain(self):
            return None

        def close(self):
            self.closed = True

    async def scenario():
        client_started = asyncio.Event()
        release_client = asyncio.Event()
        provider_eof = asyncio.Event()
        client_writer = Writer()
        provider_writer = Writer()
        task = asyncio.create_task(
            mod._run_tunnel_pumps(
                BlockingClientReader(client_started, release_client),
                client_writer,
                ProviderReader(provider_eof),
                provider_writer,
                "www.fbref.com",
                lease=lease,
            )
        )
        await asyncio.wait_for(client_started.wait(), timeout=1)
        await asyncio.wait_for(provider_eof.wait(), timeout=1)
        # Let the down pump settle/release its EOF reservation while the local
        # client remains silent forever from the provider's perspective.
        await asyncio.sleep(0)
        await asyncio.sleep(0)
        assert task.done() is False
        assert bytes(client_writer.payload) == provider_payload
        assert lease.down_bytes == len(provider_payload)
        assert lease.reserved_bytes == 0
        assert lease.accounting_uncertain is False
        release_client.set()
        await asyncio.wait_for(task, timeout=1)
        return client_writer, provider_writer

    client_writer, provider_writer = asyncio.run(scenario())

    assert client_writer.closed is True
    assert provider_writer.closed is True
    assert lease.usable is True
    assert lease.active_provider_readers == 0


def test_concurrent_provider_readers_keep_aggregate_upload_headroom(mod):
    mgr = _FakeManager(["http://u:p@pool.invalid:10000"])
    lease = _make_fbref_lease(mod, mgr, max_bytes=12)

    class BlockingProviderReader:
        def __init__(self, index, started, release):
            self.index = index
            self.started = started
            self.release = release

        async def read(self, _size):
            await self.started.put(self.index)
            await self.release.wait()
            return b""

    class Writer:
        def __init__(self):
            self.closed = False

        def write(self, _chunk):
            raise AssertionError("EOF-only provider reader must not write")

        async def drain(self):
            return None

        def close(self):
            self.closed = True

    async def scenario():
        started = asyncio.Queue()
        releases = [asyncio.Event() for _ in range(3)]
        writers = [Writer() for _ in range(3)]
        tasks = [
            asyncio.create_task(
                mod._pump(
                    BlockingProviderReader(index, started, releases[index]),
                    writers[index],
                    "www.fbref.com",
                    defaultdict(int),
                    lease=lease,
                    direction="down",
                )
            )
            for index in range(3)
        ]
        observed = [
            await asyncio.wait_for(started.get(), timeout=1) for _ in range(3)
        ]
        # Every provider reader may progress, but their aggregate pre-read
        # escrow leaves the exact N+1 share for an upload on this lease.
        assert lease.active_provider_readers == 3
        assert lease.provider_reserved_bytes == 9
        assert lease.reserved_bytes == 9
        assert mod._lease_remaining(lease) == 3
        for index in observed:
            releases[index].set()
        await asyncio.wait_for(asyncio.gather(*tasks), timeout=1)
        return observed, writers

    observed, writers = asyncio.run(scenario())

    assert sorted(observed) == [0, 1, 2]
    assert all(writer.closed for writer in writers)
    assert lease.provider_reserved_bytes == 0
    assert lease.reserved_bytes == 0
    assert lease.accounting_uncertain is False
    assert lease.usable is True


def test_cross_lease_provider_reservations_leave_shared_upload_headroom(mod):
    mgr = _FakeManager(["http://u:p@pool.invalid:10000"])
    mod.DAILY_BUDGET_BYTES = 12
    mod.DAGRUN_BUDGET_BYTES = 12
    mod.URL_BUDGET_BYTES = 12
    mod.MAX_LEASE_BYTES = 12
    mod.MAX_ACTIVE_LEASES = 4
    mod.TRANSFERMARKT_DAGRUN_BUDGET_BYTES = 12
    leases = tuple(
        mod._create_lease(
            mgr,
            max_bytes=12,
            ttl_seconds=30,
            metadata={
                "dag_id": "dag_ingest_transfermarkt",
                "run_id": f"run-{suffix}",
                "task_id": f"task-{suffix}",
                "canonical_url": f"https://www.transfermarkt.com/{suffix}",
            },
            require_context=True,
        )
        for suffix in ("a", "b", "c", "d")
    )
    first = leases[0]

    class BlockingProviderReader:
        def __init__(self, started, release):
            self.started = started
            self.release = release

        async def read(self, _size):
            self.started.set()
            await self.release.wait()
            return b""

    class UploadReader:
        def __init__(self):
            self.chunks = [b"xy", b""]

        async def read(self, _size):
            return self.chunks.pop(0)

    class Writer:
        def __init__(self):
            self.payload = bytearray()
            self.closed = False

        def write(self, chunk):
            self.payload.extend(chunk)

        async def drain(self):
            return None

        def close(self):
            self.closed = True

    async def scenario():
        started = [asyncio.Event() for _ in leases]
        release = [asyncio.Event() for _ in leases]
        down_writers = [Writer() for _ in leases]
        down_tasks = [
            asyncio.create_task(
                mod._pump(
                    BlockingProviderReader(started[index], release[index]),
                    down_writers[index],
                    "www.transfermarkt.com",
                    defaultdict(int),
                    lease=lease,
                    direction="down",
                )
            )
            for index, lease in enumerate(leases)
        ]
        await asyncio.gather(
            *(asyncio.wait_for(event.wait(), timeout=1) for event in started)
        )
        assert mod._ACTIVE_PROVIDER_READERS == 4
        assert mod._daily_reserved_bytes == 10
        assert all(mod._lease_remaining(lease) == 2 for lease in leases)

        upload_writer = Writer()
        await asyncio.wait_for(
            mod._pump(
                UploadReader(),
                upload_writer,
                "www.transfermarkt.com",
                defaultdict(int),
                lease=first,
                direction="up",
            ),
            timeout=1,
        )
        assert bytes(upload_writer.payload) == b"xy"
        assert all(task.done() is False for task in down_tasks)

        for event in release:
            event.set()
        await asyncio.wait_for(asyncio.gather(*down_tasks), timeout=1)
        return upload_writer, down_writers

    upload_writer, down_writers = asyncio.run(scenario())

    assert upload_writer.closed is True
    assert all(writer.closed for writer in down_writers)
    assert first.up_bytes == 2
    assert all(lease.reserved_bytes == 0 for lease in leases)
    assert all(lease.provider_reserved_bytes == 0 for lease in leases)
    assert all(lease.accounting_uncertain is False for lease in leases)
    assert mod._ACTIVE_PROVIDER_READERS == 0


def test_staged_client_upload_waits_for_temporary_shared_reservation(mod):
    mgr = _FakeManager(["http://u:p@pool.invalid:10000"])
    lease = _make_fbref_lease(mod, mgr, max_bytes=12)
    external_reservation = mod._reserve_lease_bytes(lease, 8)
    assert external_reservation == 8

    class Reader:
        def __init__(self):
            self.chunks = [b"abcdef", b""]

        async def read(self, _size):
            return self.chunks.pop(0)

    class Writer:
        def __init__(self, first_write):
            self.payload = bytearray()
            self.first_write = first_write
            self.closed = False

        def write(self, chunk):
            self.payload.extend(chunk)
            self.first_write.set()

        async def drain(self):
            return None

        def close(self):
            self.closed = True

    async def scenario():
        first_write = asyncio.Event()
        writer = Writer(first_write)
        task = asyncio.create_task(
            mod._pump(
                Reader(),
                writer,
                "www.fbref.com",
                defaultdict(int),
                lease=lease,
                direction="up",
            )
        )
        await asyncio.wait_for(first_write.wait(), timeout=1)
        # Four bytes fit now; the ordered two-byte tail must wait for the
        # unrelated in-flight reservation instead of poisoning the lease.
        await asyncio.sleep(0)
        assert bytes(writer.payload) == b"abcd"
        assert task.done() is False
        assert lease.budget_exceeded is False
        assert lease.accounting_uncertain is False
        mod._release_lease_reservation(lease, external_reservation)
        await asyncio.wait_for(task, timeout=1)
        return writer

    writer = asyncio.run(scenario())

    assert bytes(writer.payload) == b"abcdef"
    assert writer.closed is True
    assert lease.up_bytes == 6
    assert lease.reserved_bytes == 0
    assert lease.usable is True


def test_provider_connect_head_is_bounded_before_read_and_counted_exactly(mod):
    mgr = _FakeManager(["http://u:p@pool.invalid:10000"])
    mod.SOFASCORE_DAGRUN_BUDGET_BYTES = 8
    lease = mod._create_lease(
        mgr,
        max_bytes=8,
        ttl_seconds=30,
        metadata=_sofascore_context(),
        require_context=True,
    )
    mod._begin_endpoint_request(lease, "event")

    class Reader:
        def __init__(self):
            self.payload = bytearray(b"HTTP/1.1 200 OK\r\n\r\n")
            self.reads = 0

        async def read(self, size):
            self.reads += 1
            chunk = bytes(self.payload[:size])
            del self.payload[:size]
            return chunk

    reader = Reader()
    with pytest.raises(RuntimeError, match="over-budget"):
        asyncio.run(
            mod._read_metered_provider_head(
                reader,
                lease,
                "www.sofascore.com",
            )
        )

    assert reader.reads == 8
    assert lease.down_bytes == 8
    assert lease.budget_exceeded is True


def test_durable_lease_ledger_restores_daily_and_dagrun_exact_bytes(mod):
    mgr = _FakeManager(["http://u:p@pool.invalid:10000"])
    mod.SOFASCORE_DAGRUN_BUDGET_BYTES = 4096
    lease = mod._create_lease(
        mgr,
        max_bytes=4096,
        ttl_seconds=30,
        metadata=_sofascore_context(),
        require_context=True,
    )
    mod._begin_endpoint_request(lease, "event")
    mod._account_lease_bytes(lease, "www.sofascore.com", "up", 125)
    mod._account_lease_bytes(lease, "www.sofascore.com", "down", 875)

    mod._run_up_bytes.clear()
    mod._run_down_bytes.clear()
    mod._url_up_bytes.clear()
    mod._url_down_bytes.clear()
    mod._daily_day = ""
    mod._daily_up_bytes = mod._daily_down_bytes = 0
    restored = mod._restore_budget_ledger(mod.LEDGER_PATH, restore_daily=True)

    assert restored == 2
    assert mod._run_total_bytes(lease.dagrun_key) == 1000
    assert mod._daily_total_bytes() == 1000
    assert Path(mod.LEDGER_PATH).stat().st_mode & 0o777 == 0o600
    ledger = Path(mod.LEDGER_PATH).read_text()
    assert lease.token not in ledger
    assert "u:p" not in ledger


def test_daily_budget_caps_lease_and_blocks_followup_before_upstream_pick(mod):
    mgr = _FakeManager(
        [
            "http://u:p@pool.invalid:10000",
            "http://u:p@pool.invalid:10001",
        ]
    )
    mod.DAILY_BUDGET_BYTES = 10
    mod.SOFASCORE_DAGRUN_BUDGET_BYTES = 100
    lease = mod._create_lease(
        mgr,
        max_bytes=100,
        ttl_seconds=30,
        metadata=_sofascore_context(),
        require_context=True,
    )
    mod._begin_endpoint_request(lease, "event")
    assert lease.max_bytes == 10
    mod._account_lease_bytes(lease, "www.sofascore.com", "down", 10)
    lease.closed = True

    with pytest.raises(RuntimeError, match="daily paid-proxy budget exhausted"):
        mod._create_lease(
            mgr,
            max_bytes=1,
            ttl_seconds=30,
            metadata=_sofascore_context(task_id="retry"),
            require_context=True,
        )
    assert mgr.calls == 1


def test_corrupt_durable_lease_ledger_fails_closed(mod):
    Path(mod.LEDGER_PATH).write_text("{broken\n")

    with pytest.raises(RuntimeError, match="line 1"):
        mod._restore_budget_ledger(mod.LEDGER_PATH, restore_daily=True)


def test_sensitive_query_values_are_redacted_before_report_and_ledger(mod):
    canonical = mod._canonical_url(
        "https://www.sofascore.com/api/v1/x?token=secret&a=1&api_key=also-secret"
    )

    assert canonical == (
        "https://www.sofascore.com/api/v1/x?"
        "a=1&api_key=%5BREDACTED%5D&token=%5BREDACTED%5D"
    )
    assert "secret" not in canonical


# --- shipped blocklist safety -------------------------------------------------


def test_shipped_blocklist_blocks_adtech_but_not_cf_or_sites(mod):
    # Arrange — load the real config the filter ships with
    mod.BLOCKLIST = mod._load_blocklist(str(_BLOCKLIST_PATH))
    assert mod.BLOCKLIST, "shipped blocklist must not be empty"
    # Assert — must keep CF + the scraped sites alive
    for keep in (
        "challenges.cloudflare.com",
        "sofifa.com",
        "cdn.sofifa.net",
        "www.whoscored.com",
        "cdn.whoscored.com",
        "fonts.gstatic.com",
    ):
        assert mod._is_blocked(keep) is False, f"{keep} must NOT be blocked"
    # Assert — must drop the heavy ad-tech seen in the observe run
    for drop in (
        "securepubads.g.doubleclick.net",
        "www.googletagmanager.com",
        "ib.adnxs.com",
        "connect.facebook.net",
        "cdn.intergient.com",
    ):
        assert mod._is_blocked(drop) is True, f"{drop} must be blocked"


# --- dead residential exit failover (#946) -----------------------------------
#
# An immediate empty EOF/reset is provably free of provider response read-ahead
# and may fail over.  A silent accepted connection is different: its timeout can
# hide bytes in transport read-ahead, so it must revoke the lease and retain the
# remaining escrow.  These tests exercise both sides of that boundary.


class _FakeUpstreamReader:
    """Minimal StreamReader: serves ``read(n)`` from a buffer, optionally
    blocking forever once the buffer drains (a dead but connected exit)."""

    def __init__(self, data=b"", *, block_when_empty=False):
        self.buf = bytearray(data)
        self.block_when_empty = block_when_empty

    async def read(self, size):
        if not self.buf:
            if self.block_when_empty:
                await asyncio.Event().wait()
            return b""
        chunk = bytes(self.buf[:size])
        del self.buf[:size]
        return chunk


class _FakeUpstreamWriter:
    def __init__(self):
        self.data = bytearray()
        self.closed = False

    def write(self, chunk):
        self.data.extend(chunk)

    async def drain(self):
        return None

    def close(self):
        self.closed = True


class _ClientConnectReader:
    """Client leg of one CONNECT: hands back the request head, then EOF for the
    (empty) client->upstream tunnel payload."""

    def __init__(self, header_lines, tunnel_payload=b""):
        self.lines = deque(header_lines)
        self.tunnel_payload = bytearray(tunnel_payload)

    async def readline(self):
        return self.lines.popleft() if self.lines else b""

    async def read(self, size):
        chunk = bytes(self.tunnel_payload[:size])
        del self.tunnel_payload[:size]
        return chunk


class _YieldingClientConnectReader(_ClientConnectReader):
    """Model a client read that yields while the provider pump starts."""

    async def read(self, size):
        await asyncio.sleep(0)
        return await super().read(size)


class _ClientWriter:
    def __init__(self):
        self.payload = bytearray()
        self.closed = False

    def write(self, chunk):
        self.payload.extend(chunk)

    async def drain(self):
        return None

    def close(self):
        self.closed = True


def _make_sofascore_lease(mod, mgr, *, budget=4096, endpoint="event"):
    mod.LEASES.clear()
    mod.LEASE_TOKENS.clear()
    mod.up_bytes = defaultdict(int)
    mod.down_bytes = defaultdict(int)
    mod._daily_day = ""
    mod._daily_up_bytes = mod._daily_down_bytes = 0
    mod.SOFASCORE_DAGRUN_BUDGET_BYTES = budget
    lease = mod._create_lease(
        mgr,
        max_bytes=budget,
        ttl_seconds=30,
        metadata=_sofascore_context(budget=budget),
        require_context=True,
    )
    mod._begin_endpoint_request(lease, endpoint)
    return lease


def _connect_header_lines(lease, host="www.sofascore.com"):
    encoded = base64.b64encode(f"lease:{lease.token}".encode()).decode()
    return [
        f"CONNECT {host}:443 HTTP/1.1\r\n".encode(),
        f"Host: {host}:443\r\n".encode(),
        f"Proxy-Authorization: Basic {encoded}\r\n".encode(),
        b"\r\n",
    ]


def _expected_connect_head(host="www.sofascore.com"):
    auth = base64.b64encode(b"u:p").decode()
    return (
        f"CONNECT {host}:443 HTTP/1.1\r\n".encode()
        + f"Host: {host}:443\r\n".encode()
        + f"Proxy-Authorization: Basic {auth}\r\n\r\n".encode()
    )


def _tls_client_hello(
    *,
    sni_names=("www.whoscored.com",),
    extra_extensions=(),
    split_at=None,
):
    extensions = []
    if sni_names is not None:
        entries = b"".join(
            b"\x00"
            + len(name.encode("ascii")).to_bytes(2, "big")
            + name.encode("ascii")
            for name in sni_names
        )
        server_names = len(entries).to_bytes(2, "big") + entries
        extensions.append(
            b"\x00\x00" + len(server_names).to_bytes(2, "big") + server_names
        )
    for extension_type, extension_data in extra_extensions:
        extensions.append(
            int(extension_type).to_bytes(2, "big")
            + len(extension_data).to_bytes(2, "big")
            + extension_data
        )
    encoded_extensions = b"".join(extensions)
    body = (
        b"\x03\x03"
        + b"\x11" * 32
        + b"\x00"
        + b"\x00\x02\x13\x01"
        + b"\x01\x00"
        + len(encoded_extensions).to_bytes(2, "big")
        + encoded_extensions
    )
    handshake = b"\x01" + len(body).to_bytes(3, "big") + body
    fragments = (
        (handshake,)
        if split_at is None
        else (handshake[:split_at], handshake[split_at:])
    )
    return b"".join(
        b"\x16\x03\x01" + len(fragment).to_bytes(2, "big") + fragment
        for fragment in fragments
    )


def _oversized_fragmented_client_hello():
    handshake = b"\x01" + (65_531).to_bytes(3, "big") + b"\x00" * 65_531
    fragments = (
        handshake[:16_384],
        handshake[16_384:32_768],
        handshake[32_768:49_152],
        handshake[49_152:],
    )
    return b"".join(
        b"\x16\x03\x01" + len(fragment).to_bytes(2, "big") + fragment
        for fragment in fragments
    )


def _shrink_failover_timeouts(mod, monkeypatch):
    monkeypatch.setattr(mod, "LEASE_PROVIDER_HEAD_TIMEOUT_SECONDS", 0.02, raising=False)
    monkeypatch.setattr(
        mod, "LEASE_UPSTREAM_CONNECT_TIMEOUT_SECONDS", 0.02, raising=False
    )


def _patch_upstream_opener(mod, monkeypatch, fake_open):
    # ``_open_upstream_connection`` is the #946 test seam; also patch the raw
    # asyncio symbol so the pre-#946 code (which lacks the seam) still exercises
    # the hang and fails by TimeoutError rather than skipping the dial.
    monkeypatch.setattr(mod, "_open_upstream_connection", fake_open, raising=False)
    monkeypatch.setattr(mod.asyncio, "open_connection", fake_open)


@pytest.mark.parametrize(
    "client_hello",
    [
        _tls_client_hello(sni_names=None),
        _tls_client_hello(sni_names=("www.whoscored.com", "cdn.whoscored.com")),
        _tls_client_hello()[:-1],
        _tls_client_hello(extra_extensions=((0xFE0D, b"\x00"),)),
        _tls_client_hello(sni_names=("127.0.0.1",)),
        _tls_client_hello(sni_names=("cdn.whoscored.com",)),
        _tls_client_hello(extra_extensions=((0, b""),)),
        _tls_client_hello(extra_extensions=((0xFFCE, b"\x00"),)),
        b"\x16\x03\x01\x40\x01",
        _oversized_fragmented_client_hello(),
        b"\x17\x03\x03\x00\x01\x00",
    ],
    ids=(
        "missing",
        "multiple",
        "malformed",
        "ech",
        "ip",
        "mismatch",
        "duplicate-sni-extension",
        "esni",
        "oversized",
        "cumulative-limit",
        "encrypted-record",
    ),
)
def test_whoscored_invalid_client_hello_is_rejected_before_provider_dial(
    mod, monkeypatch, client_hello
):
    mgr = _FakeManager(["http://u:p@pool.invalid:10000"])
    lease = mod._create_lease(
        mgr,
        max_bytes=1_000,
        ttl_seconds=30,
        metadata=_whoscored_campaign_context(mod, cap=1_000),
        require_context=True,
    )
    opens = []

    async def fake_open(host, port):
        opens.append((host, port))
        return (
            _FakeUpstreamReader(b"HTTP/1.1 200 Connection established\r\n\r\n"),
            _FakeUpstreamWriter(),
        )

    _patch_upstream_opener(mod, monkeypatch, fake_open)
    client_writer = _ClientWriter()
    asyncio.run(
        mod.handle(
            _ClientConnectReader(
                _connect_header_lines(lease, host="www.whoscored.com"),
                tunnel_payload=client_hello,
            ),
            client_writer,
            mgr,
            require_lease=True,
        )
    )

    assert b"200 Connection established" in bytes(client_writer.payload)
    assert client_writer.closed is True
    assert opens == []
    assert lease.provider_request_count == 0
    assert lease.total_bytes == 0
    assert lease.usable is True
    assert lease.active_tunnels == 0


def test_whoscored_pending_client_hello_is_strictly_one_per_lease(mod, monkeypatch):
    mgr = _FakeManager(["http://u:p@pool.invalid:10000"])
    lease = mod._create_lease(
        mgr,
        max_bytes=1_000,
        ttl_seconds=30,
        metadata=_whoscored_campaign_context(mod, cap=1_000),
        require_context=True,
    )
    opens = []

    async def fake_open(*args, **kwargs):
        opens.append((args, kwargs))
        return _FakeUpstreamReader(), _FakeUpstreamWriter()

    class BlockingReader(_ClientConnectReader):
        def __init__(self, header_lines, started, release):
            super().__init__(header_lines)
            self.started = started
            self.release = release

        async def read(self, size):
            self.started.set()
            await self.release.wait()
            return b""

    async def scenario():
        started = asyncio.Event()
        release = asyncio.Event()
        first_writer = _ClientWriter()
        first = asyncio.create_task(
            mod.handle(
                BlockingReader(
                    _connect_header_lines(lease, host="www.whoscored.com"),
                    started,
                    release,
                ),
                first_writer,
                mgr,
                require_lease=True,
            )
        )
        await asyncio.wait_for(started.wait(), timeout=1)
        assert lease.pending_client_hellos == 1

        second_writer = _ClientWriter()
        await mod.handle(
            _ClientConnectReader(
                _connect_header_lines(lease, host="www.whoscored.com"),
                tunnel_payload=_tls_client_hello(),
            ),
            second_writer,
            mgr,
            require_lease=True,
        )
        assert b"429 Too Many Requests" in bytes(second_writer.payload)
        assert b"200 Connection established" not in bytes(second_writer.payload)
        assert second_writer.closed is True

        release.set()
        await first
        return first_writer

    _patch_upstream_opener(mod, monkeypatch, fake_open)
    first_writer = asyncio.run(scenario())

    assert b"200 Connection established" in bytes(first_writer.payload)
    assert first_writer.closed is True
    assert opens == []
    assert lease.pending_client_hellos == 0
    assert lease.active_tunnels == 0
    assert lease.provider_request_count == 0
    assert lease.total_bytes == 0
    assert (
        mod._WHOSCORED_CLIENT_HELLO_SLOTS._value
        == mod.MAX_PENDING_WHOSCORED_CLIENT_HELLOS
    )


def test_whoscored_global_pending_client_hello_cap_fails_fast_at_n_plus_one(
    mod, monkeypatch
):
    mgr = _FakeManager(["http://u:p@pool.invalid:10000"])
    capacity = 2
    leases = []
    now = time.time()
    for index in range(capacity + 1):
        lease = mod.Lease(
            lease_id=f"lease-{index}",
            token=f"token-{index}",
            upstream=("pool.invalid", 10000, "u", "p"),
            created_at=now,
            expires_at=now + 30,
            max_bytes=1_000,
            source="whoscored",
        )
        mod.LEASES[lease.lease_id] = lease
        mod.LEASE_TOKENS[lease.token] = lease.lease_id
        leases.append(lease)
    opens = []

    async def fake_open(*args, **kwargs):
        opens.append((args, kwargs))
        return _FakeUpstreamReader(), _FakeUpstreamWriter()

    class BlockingReader(_ClientConnectReader):
        def __init__(self, header_lines, started, release):
            super().__init__(header_lines)
            self.started = started
            self.release = release

        async def read(self, size):
            self.started.set()
            await self.release.wait()
            return b""

    async def scenario():
        release = asyncio.Event()
        started = [asyncio.Event() for _ in range(capacity)]
        writers = [_ClientWriter() for _ in leases]
        pending = [
            asyncio.create_task(
                mod.handle(
                    BlockingReader(
                        _connect_header_lines(leases[index], host="www.whoscored.com"),
                        started[index],
                        release,
                    ),
                    writers[index],
                    mgr,
                    require_lease=True,
                )
            )
            for index in range(capacity)
        ]
        await asyncio.wait_for(
            asyncio.gather(*(event.wait() for event in started)), timeout=1
        )

        await asyncio.wait_for(
            mod.handle(
                _ClientConnectReader(
                    _connect_header_lines(leases[-1], host="www.whoscored.com"),
                    tunnel_payload=_tls_client_hello(),
                ),
                writers[-1],
                mgr,
                require_lease=True,
            ),
            timeout=1,
        )
        release.set()
        await asyncio.gather(*pending)
        return writers

    monkeypatch.setattr(
        mod,
        "_WHOSCORED_CLIENT_HELLO_SLOTS",
        asyncio.BoundedSemaphore(capacity),
    )
    _patch_upstream_opener(mod, monkeypatch, fake_open)
    writers = asyncio.run(scenario())

    for writer in writers[:-1]:
        assert b"200 Connection established" in bytes(writer.payload)
        assert writer.closed is True
    assert b"503 Service Unavailable" in bytes(writers[-1].payload)
    assert b"200 Connection established" not in bytes(writers[-1].payload)
    assert writers[-1].closed is True
    assert opens == []
    assert all(lease.pending_client_hellos == 0 for lease in leases)
    assert all(lease.active_tunnels == 0 for lease in leases)
    assert all(lease.provider_request_count == 0 for lease in leases)
    assert all(lease.total_bytes == 0 for lease in leases)
    assert mod._WHOSCORED_CLIENT_HELLO_SLOTS._value == capacity


def test_whoscored_client_hello_timeout_releases_slot_before_any_dial(mod, monkeypatch):
    mgr = _FakeManager(["http://u:p@pool.invalid:10000"])
    lease = mod._create_lease(
        mgr,
        max_bytes=1_000,
        ttl_seconds=30,
        metadata=_whoscored_campaign_context(mod, cap=1_000),
        require_context=True,
    )
    opens = []

    class SlowReader(_ClientConnectReader):
        async def read(self, size):
            await asyncio.Event().wait()

    async def fake_open(*args, **kwargs):
        opens.append((args, kwargs))
        return _FakeUpstreamReader(), _FakeUpstreamWriter()

    monkeypatch.setattr(mod, "WHOSCORED_CLIENT_HELLO_TIMEOUT_SECONDS", 0.01)
    _patch_upstream_opener(mod, monkeypatch, fake_open)
    writer = _ClientWriter()
    asyncio.run(
        mod.handle(
            SlowReader(_connect_header_lines(lease, host="www.whoscored.com")),
            writer,
            mgr,
            require_lease=True,
        )
    )

    assert b"200 Connection established" in bytes(writer.payload)
    assert writer.closed is True
    assert opens == []
    assert lease.pending_client_hellos == 0
    assert lease.active_tunnels == 0
    assert lease.provider_request_count == 0
    assert lease.total_bytes == 0
    assert (
        mod._WHOSCORED_CLIENT_HELLO_SLOTS._value
        == mod.MAX_PENDING_WHOSCORED_CLIENT_HELLOS
    )


def test_whoscored_client_hello_cancellation_closes_client_and_releases_slot(
    mod, monkeypatch
):
    mgr = _FakeManager(["http://u:p@pool.invalid:10000"])
    lease = mod._create_lease(
        mgr,
        max_bytes=1_000,
        ttl_seconds=30,
        metadata=_whoscored_campaign_context(mod, cap=1_000),
        require_context=True,
    )
    opens = []

    async def fake_open(*args, **kwargs):
        opens.append((args, kwargs))
        return _FakeUpstreamReader(), _FakeUpstreamWriter()

    class BlockingReader(_ClientConnectReader):
        def __init__(self, header_lines, started):
            super().__init__(header_lines)
            self.started = started

        async def read(self, size):
            self.started.set()
            await asyncio.Event().wait()

    async def scenario():
        started = asyncio.Event()
        writer = _ClientWriter()
        task = asyncio.create_task(
            mod.handle(
                BlockingReader(
                    _connect_header_lines(lease, host="www.whoscored.com"),
                    started,
                ),
                writer,
                mgr,
                require_lease=True,
            )
        )
        await asyncio.wait_for(started.wait(), timeout=1)
        task.cancel()
        with pytest.raises(asyncio.CancelledError):
            await task
        return writer

    _patch_upstream_opener(mod, monkeypatch, fake_open)
    writer = asyncio.run(scenario())

    assert b"200 Connection established" in bytes(writer.payload)
    assert writer.closed is True
    assert opens == []
    assert lease.pending_client_hellos == 0
    assert lease.active_tunnels == 0
    assert lease.provider_request_count == 0
    assert lease.total_bytes == 0
    assert (
        mod._WHOSCORED_CLIENT_HELLO_SLOTS._value
        == mod.MAX_PENDING_WHOSCORED_CLIENT_HELLOS
    )


def test_whoscored_non_443_connect_is_rejected_before_provider_dial(mod, monkeypatch):
    mgr = _FakeManager(["http://u:p@pool.invalid:10000"])
    lease = mod._create_lease(
        mgr,
        max_bytes=1_000,
        ttl_seconds=30,
        metadata=_whoscored_campaign_context(mod, cap=1_000),
        require_context=True,
    )
    encoded = base64.b64encode(f"lease:{lease.token}".encode()).decode()
    reader = _ClientConnectReader(
        [
            b"CONNECT www.whoscored.com:8443 HTTP/1.1\r\n",
            b"Host: www.whoscored.com:8443\r\n",
            f"Proxy-Authorization: Basic {encoded}\r\n".encode(),
            b"\r\n",
        ],
        tunnel_payload=_tls_client_hello(),
    )
    opens = []

    async def fake_open(*args, **kwargs):
        opens.append((args, kwargs))
        return _FakeUpstreamReader(), _FakeUpstreamWriter()

    _patch_upstream_opener(mod, monkeypatch, fake_open)
    client_writer = _ClientWriter()
    asyncio.run(mod.handle(reader, client_writer, mgr, require_lease=True))

    assert b"403 Forbidden" in bytes(client_writer.payload)
    assert b"200 Connection established" not in bytes(client_writer.payload)
    assert opens == []
    assert lease.provider_request_count == 0
    assert lease.total_bytes == 0


def test_whoscored_valid_fragmented_client_hello_is_metered_and_forwarded_once(
    mod, monkeypatch
):
    mgr = _FakeManager(["http://u:p@pool.invalid:10000"])
    lease = mod._create_lease(
        mgr,
        max_bytes=1_000,
        ttl_seconds=30,
        metadata=_whoscored_campaign_context(mod, cap=1_000),
        require_context=True,
    )
    # Split even the 4-byte handshake header across records.
    client_hello = _tls_client_hello(split_at=2)
    provider_head = b"HTTP/1.1 200 Connection established\r\n\r\n"
    provider_writer = _FakeUpstreamWriter()
    opens = []

    async def fake_open(host, port):
        opens.append((host, port))
        return _FakeUpstreamReader(provider_head), provider_writer

    _patch_upstream_opener(mod, monkeypatch, fake_open)
    client_writer = _ClientWriter()
    asyncio.run(
        mod.handle(
            _ClientConnectReader(
                _connect_header_lines(lease, host="www.whoscored.com"),
                tunnel_payload=client_hello,
            ),
            client_writer,
            mgr,
            require_lease=True,
        )
    )

    expected_upstream = _expected_connect_head("www.whoscored.com") + client_hello
    assert opens == [("pool.invalid", 10000)]
    assert bytes(provider_writer.data) == expected_upstream
    assert lease.provider_request_count == 1
    assert lease.up_bytes == len(expected_upstream)
    assert lease.down_bytes == len(provider_head)
    assert lease.usable is True
    assert lease.active_tunnels == 0
    assert b"200 Connection established" in bytes(client_writer.payload)


def test_whoscored_failover_forwards_buffered_client_hello_only_to_live_exit(
    mod, monkeypatch
):
    mgr = _FakeManager(
        ["http://u:p@pool.invalid:10000", "http://u:p@pool.invalid:10001"]
    )
    lease = mod._create_lease(
        mgr,
        max_bytes=2_000,
        ttl_seconds=30,
        metadata=_whoscored_campaign_context(mod, cap=2_000, requests=2),
        require_context=True,
    )
    client_hello = _tls_client_hello()
    provider_head = b"HTTP/1.1 200 Connection established\r\n\r\n"
    dead_writer = _FakeUpstreamWriter()
    live_writer = _FakeUpstreamWriter()
    opens = []

    async def fake_open(host, port):
        opens.append((host, port))
        if len(opens) == 1:
            return _FakeUpstreamReader(b""), dead_writer
        return _FakeUpstreamReader(provider_head), live_writer

    _patch_upstream_opener(mod, monkeypatch, fake_open)
    client_writer = _ClientWriter()
    asyncio.run(
        mod.handle(
            _ClientConnectReader(
                _connect_header_lines(lease, host="www.whoscored.com"),
                tunnel_payload=client_hello,
            ),
            client_writer,
            mgr,
            require_lease=True,
        )
    )

    connect_head = _expected_connect_head("www.whoscored.com")
    assert bytes(dead_writer.data) == connect_head
    assert bytes(live_writer.data) == connect_head + client_hello
    assert opens == [("pool.invalid", 10000), ("pool.invalid", 10001)]
    assert lease.upstream_repins == 1
    assert lease.provider_request_count == 2
    assert lease.up_bytes == 2 * len(connect_head) + len(client_hello)
    assert lease.down_bytes == len(provider_head)
    assert lease.usable is True


def test_whoscored_failover_charges_each_provider_dial(mod, monkeypatch):
    mgr = _FakeManager(
        ["http://u:p@pool.invalid:10000", "http://u:p@pool.invalid:10001"]
    )
    lease = mod._create_lease(
        mgr,
        max_bytes=4_096,
        ttl_seconds=30,
        metadata=_whoscored_campaign_context(mod, cap=4_096, requests=2),
        require_context=True,
    )
    live_head = b"HTTP/1.1 200 Connection established\r\n\r\n"
    opens = []

    async def fake_open(host, port):
        opens.append((host, port))
        if len(opens) == 1:
            return _FakeUpstreamReader(b""), _FakeUpstreamWriter()
        return _FakeUpstreamReader(live_head), _FakeUpstreamWriter()

    _patch_upstream_opener(mod, monkeypatch, fake_open)
    _, writer, _, _ = asyncio.run(
        mod._open_lease_upstream_tunnel(
            lease,
            mgr,
            target="www.whoscored.com:443",
            host="www.whoscored.com",
        )
    )
    writer.close()

    snapshot = mod._whoscored_campaign_ledger().snapshot(lease.proxy_campaign_approval)
    assert len(opens) == 2
    assert lease.provider_request_count == 2
    assert snapshot["active_claims"][lease.lease_id]["requests_used"] == 2


def test_whoscored_failover_request_cap_stops_before_second_provider_dial(
    mod, monkeypatch
):
    mgr = _FakeManager(
        ["http://u:p@pool.invalid:10000", "http://u:p@pool.invalid:10001"]
    )
    lease = mod._create_lease(
        mgr,
        max_bytes=4_096,
        ttl_seconds=30,
        metadata=_whoscored_campaign_context(mod, cap=4_096, requests=1),
        require_context=True,
    )
    opens = []

    async def fake_open(host, port):
        opens.append((host, port))
        return _FakeUpstreamReader(b""), _FakeUpstreamWriter()

    _patch_upstream_opener(mod, monkeypatch, fake_open)

    with pytest.raises(ProxyCampaignError):
        asyncio.run(
            mod._open_lease_upstream_tunnel(
                lease,
                mgr,
                target="www.whoscored.com:443",
                host="www.whoscored.com",
            )
        )

    snapshot = mod._whoscored_campaign_ledger().snapshot(lease.proxy_campaign_approval)
    assert len(opens) == 1
    assert snapshot["active_claims"][lease.lease_id]["requests_used"] == 1


def test_whoscored_provider_head_timeout_revokes_without_repin_and_survives_restart(
    mod, monkeypatch
):
    mgr = _FakeManager(
        ["http://u:p@pool.invalid:10000", "http://u:p@pool.invalid:10001"]
    )
    context = _whoscored_campaign_context(mod, cap=1_000, requests=2)
    lease = mod._create_lease(
        mgr,
        max_bytes=1_000,
        ttl_seconds=30,
        metadata=context,
        require_context=True,
    )
    _shrink_failover_timeouts(mod, monkeypatch)
    opens = []

    class PrefixThenSilentReader:
        def __init__(self):
            self.calls = 0

        async def read(self, size):
            self.calls += 1
            if self.calls == 1:
                return b"H"
            # Represents a provider transport which may already have unread
            # billed bytes below StreamReader's observable one-byte result.
            await asyncio.Event().wait()

    async def fake_open(host, port):
        opens.append((host, port))
        return PrefixThenSilentReader(), _FakeUpstreamWriter()

    _patch_upstream_opener(mod, monkeypatch, fake_open)

    with pytest.raises(mod.UpstreamHeadTimeout):
        asyncio.run(
            mod._open_lease_upstream_tunnel(
                lease,
                mgr,
                target="www.whoscored.com:443",
                host="www.whoscored.com",
            )
        )

    assert len(opens) == 1
    assert lease.upstream_repins == 0
    assert lease.accounting_uncertain is True
    assert lease.usable is False
    assert lease.reserved_bytes > 0
    state = json.loads(Path(mod.WHOSCORED_CAMPAIGN_LEDGER_PATH).read_text())
    campaign = state["campaigns"][lease.proxy_campaign_approval.campaign_id]
    active = campaign["active_claims"][lease.lease_id]
    assert campaign["status"] == "revoked"
    assert active["spent_provider_bytes"] == lease.total_bytes
    assert active["reserved_provider_bytes"] == 1_000 - lease.total_bytes

    report = asyncio.run(mod._close_lease(lease, completed=False))
    assert report["close_complete"] is False
    assert "escrow retained" in report["close_error"]

    mod.LEASES.clear()
    mod.LEASE_TOKENS.clear()
    mod.WHOSCORED_CAMPAIGN_LEDGER = None
    mod._WHOSCORED_CAMPAIGN_LEDGER_KEY = None
    retry_context = {**context, "try_number": 2}
    retry_context["proxy_attempt_id"] = deterministic_proxy_attempt_id(
        dag_id=str(retry_context["dag_id"]),
        run_id=str(retry_context["run_id"]),
        task_id=str(retry_context["task_id"]),
        map_index=int(retry_context["map_index"]),
        try_number=2,
    )
    with pytest.raises(ProxyCampaignError, match="revoked"):
        mod._create_lease(
            mgr,
            max_bytes=1,
            ttl_seconds=30,
            metadata=retry_context,
            require_context=True,
        )


def test_whoscored_provider_write_drain_failure_revokes_without_repin(mod, monkeypatch):
    mgr = _FakeManager(
        ["http://u:p@pool.invalid:10000", "http://u:p@pool.invalid:10001"]
    )
    lease = mod._create_lease(
        mgr,
        max_bytes=1_000,
        ttl_seconds=30,
        metadata=_whoscored_campaign_context(mod, cap=1_000, requests=2),
        require_context=True,
    )
    opens = []

    class HiddenResponseReader:
        def __init__(self):
            self.payload = b"provider-response-already-buffered"
            self.read_calls = 0

        async def read(self, size):
            self.read_calls += 1
            return self.payload[:size]

    class DrainFailingProviderWriter(_FakeUpstreamWriter):
        async def drain(self):
            raise OSError("provider write completion is ambiguous")

    hidden_reader = HiddenResponseReader()

    async def fake_open(host, port):
        opens.append((host, port))
        return hidden_reader, DrainFailingProviderWriter()

    _patch_upstream_opener(mod, monkeypatch, fake_open)
    with pytest.raises(OSError, match="ambiguous"):
        asyncio.run(
            mod._open_lease_upstream_tunnel(
                lease,
                mgr,
                target="www.whoscored.com:443",
                host="www.whoscored.com",
            )
        )

    assert len(opens) == 1
    assert hidden_reader.read_calls == 0
    assert lease.upstream_repins == 0
    assert lease.accounting_uncertain is True
    assert lease.usable is False
    assert lease.reserved_bytes == 0
    state = json.loads(Path(mod.WHOSCORED_CAMPAIGN_LEDGER_PATH).read_text())
    campaign = state["campaigns"][lease.proxy_campaign_approval.campaign_id]
    active = campaign["active_claims"][lease.lease_id]
    assert campaign["status"] == "revoked"
    assert active["spent_provider_bytes"] == lease.up_bytes
    assert active["reserved_provider_bytes"] == 1_000 - lease.up_bytes


def test_tunnel_handoff_cancellation_before_down_pump_retains_campaign_escrow(
    mod, monkeypatch
):
    mgr = _FakeManager(["http://u:p@pool.invalid:10000"])
    lease = mod._create_lease(
        mgr,
        max_bytes=1_000,
        ttl_seconds=30,
        metadata=_whoscored_campaign_context(mod, cap=1_000),
        require_context=True,
    )

    class HiddenProviderReader:
        def __init__(self):
            self.read_calls = 0

        async def read(self, size):
            self.read_calls += 1
            return b"response-already-buffered"[:size]

    async def cancel_before_start(*operations):
        # Model task cancellation at the gather handoff before either coroutine
        # owns the provider StreamReader. Closing avoids un-awaited warnings.
        for operation in operations:
            operation.close()
        raise asyncio.CancelledError

    provider_reader = HiddenProviderReader()
    provider_writer = _FakeUpstreamWriter()
    lease.tunnel_writers.add(provider_writer)
    monkeypatch.setattr(mod.asyncio, "gather", cancel_before_start)

    with pytest.raises(asyncio.CancelledError):
        asyncio.run(
            mod._run_tunnel_pumps(
                _ClientConnectReader([]),
                _ClientWriter(),
                provider_reader,
                provider_writer,
                "www.whoscored.com",
                lease=lease,
            )
        )

    assert provider_reader.read_calls == 0
    assert provider_writer.closed is True
    assert lease.accounting_uncertain is True
    assert lease.usable is False
    state = json.loads(Path(mod.WHOSCORED_CAMPAIGN_LEDGER_PATH).read_text())
    campaign = state["campaigns"][lease.proxy_campaign_approval.campaign_id]
    assert campaign["status"] == "revoked"
    assert campaign["active_claims"][lease.lease_id]["reserved_provider_bytes"] == 1_000


def test_whoscored_lease_rejects_pipelined_non_connect_before_provider_dial(
    mod, monkeypatch
):
    mgr = _FakeManager(["http://u:p@pool.invalid:10000"])
    lease = mod._create_lease(
        mgr,
        max_bytes=1_000,
        ttl_seconds=30,
        metadata=_whoscored_campaign_context(mod, cap=1_000),
        require_context=True,
    )
    encoded = base64.b64encode(f"lease:{lease.token}".encode()).decode()

    class PipelinedReader:
        def __init__(self):
            self.lines = deque(
                [
                    b"GET https://www.whoscored.com/Matches/1/Live HTTP/1.1\r\n",
                    b"Host: www.whoscored.com\r\n",
                    f"Proxy-Authorization: Basic {encoded}\r\n".encode(),
                    b"\r\n",
                ]
            )
            self.read_calls = 0

        async def readline(self):
            return self.lines.popleft() if self.lines else b""

        async def read(self, size):
            self.read_calls += 1
            return b"GET http://evil.example/ HTTP/1.1\r\nHost: evil.example\r\n\r\n"

    async def forbidden_dial(*args, **kwargs):
        raise AssertionError("non-CONNECT WhoScored traffic must not reach provider")

    calls_before = mgr.calls
    reader = PipelinedReader()
    writer = _ClientWriter()
    monkeypatch.setattr(mod, "_open_upstream_connection", forbidden_dial)
    asyncio.run(mod.handle(reader, writer, mgr, require_lease=True))

    assert b"405 Method Not Allowed" in bytes(writer.payload)
    assert reader.read_calls == 0
    assert mgr.calls == calls_before
    assert lease.provider_request_count == 0
    assert lease.total_bytes == 0
    assert lease.usable is True


@pytest.mark.parametrize(
    "provider_response",
    [
        b"HTTP/1.1 407 Proxy Authentication Required\r\n\r\nhidden-body",
        b"HTTP/1.1 500 upstream reason200\r\n\r\nhidden-body",
        b"HTTP/1.1 200OK\r\n\r\nhidden-body",
    ],
    ids=("non-200", "reason-containing-200", "malformed-200"),
)
def test_whoscored_non_200_or_ambiguous_status_retains_read_ahead_escrow(
    mod, monkeypatch, provider_response
):
    mgr = _FakeManager(["http://u:p@pool.invalid:10000"])
    lease = mod._create_lease(
        mgr,
        max_bytes=1_000,
        ttl_seconds=30,
        metadata=_whoscored_campaign_context(mod, cap=1_000),
        require_context=True,
    )
    upstream_reader = _FakeUpstreamReader(provider_response)
    upstream_writer = _FakeUpstreamWriter()

    async def fake_open(host, port):
        return upstream_reader, upstream_writer

    _patch_upstream_opener(mod, monkeypatch, fake_open)
    client_writer = _ClientWriter()
    asyncio.run(
        mod.handle(
            _ClientConnectReader(
                _connect_header_lines(lease, host="www.whoscored.com"),
                tunnel_payload=_tls_client_hello(),
            ),
            client_writer,
            mgr,
            require_lease=True,
        )
    )

    assert b"502 Bad Gateway" not in bytes(client_writer.payload)
    assert b"200 Connection established" in bytes(client_writer.payload)
    assert bytes(upstream_reader.buf) == b"hidden-body"
    assert upstream_writer.closed is True
    assert lease.accounting_uncertain is True
    assert lease.usable is False
    state = json.loads(Path(mod.WHOSCORED_CAMPAIGN_LEDGER_PATH).read_text())
    campaign = state["campaigns"][lease.proxy_campaign_approval.campaign_id]
    assert campaign["status"] == "revoked"
    assert (
        campaign["active_claims"][lease.lease_id]["reserved_provider_bytes"]
        == 1_000 - lease.total_bytes
    )


def test_whoscored_client_connect_reply_drain_failure_dials_no_provider(
    mod, monkeypatch
):
    mgr = _FakeManager(["http://u:p@pool.invalid:10000"])
    lease = mod._create_lease(
        mgr,
        max_bytes=1_000,
        ttl_seconds=30,
        metadata=_whoscored_campaign_context(mod, cap=1_000),
        require_context=True,
    )
    provider_head = b"HTTP/1.1 200 Connection established\r\n\r\n"
    upstream_reader = _FakeUpstreamReader(provider_head + b"hidden-tunnel-bytes")
    upstream_writer = _FakeUpstreamWriter()
    opens = []

    async def fake_open(host, port):
        opens.append((host, port))
        return upstream_reader, upstream_writer

    class FailingClientWriter(_ClientWriter):
        async def drain(self):
            raise OSError("client disconnected before tunnel handoff")

    _patch_upstream_opener(mod, monkeypatch, fake_open)
    client_writer = FailingClientWriter()
    asyncio.run(
        mod.handle(
            _ClientConnectReader(
                _connect_header_lines(lease, host="www.whoscored.com"),
                tunnel_payload=_tls_client_hello(),
            ),
            client_writer,
            mgr,
            require_lease=True,
        )
    )

    assert b"200 Connection established" in bytes(client_writer.payload)
    assert opens == []
    assert bytes(upstream_reader.buf) == provider_head + b"hidden-tunnel-bytes"
    assert upstream_writer.closed is False
    assert lease.provider_request_count == 0
    assert lease.total_bytes == 0
    assert lease.accounting_uncertain is False
    assert lease.usable is True


def test_lease_connect_failover_repins_immediate_eof_and_tunnels(mod, monkeypatch):
    mgr = _FakeManager(
        ["http://u:p@pool.invalid:10000", "http://u:p@pool.invalid:10001"]
    )
    lease = _make_sofascore_lease(mod, mgr)
    expires_before = lease.expires_at
    _shrink_failover_timeouts(mod, monkeypatch)

    dead_writer = _FakeUpstreamWriter()
    live_writer = _FakeUpstreamWriter()
    live_head = b"HTTP/1.1 200 Connection established\r\n\r\n"
    tunnel = b"hello-tunnel"
    opens = []

    async def fake_open(host, port):
        opens.append((host, port))
        if len(opens) == 1:
            return _FakeUpstreamReader(b""), dead_writer
        return _FakeUpstreamReader(live_head + tunnel), live_writer

    _patch_upstream_opener(mod, monkeypatch, fake_open)

    client_writer = _ClientWriter()
    asyncio.run(
        asyncio.wait_for(
            mod.handle(
                _YieldingClientConnectReader(_connect_header_lines(lease)),
                client_writer,
                mgr,
                require_lease=True,
            ),
            2.0,
        )
    )

    head = _expected_connect_head()
    assert b"200 Connection established" in bytes(client_writer.payload)
    assert bytes(client_writer.payload).endswith(tunnel)
    assert lease.upstream == ("pool.invalid", 10001, "u", "p")
    assert lease.upstream_repins == 1
    assert dead_writer.data == head
    assert live_writer.data == head
    assert dead_writer.closed is True
    # up == both CONNECT heads (one billed to the dead exit, one to the live
    # exit); down == the live exit's response head + tunnel payload only.
    assert lease.up_bytes == 2 * len(head)
    assert lease.down_bytes == len(live_head) + len(tunnel)
    assert lease.expires_at == expires_before
    assert lease.report()["upstream_repins"] == 1
    assert lease.active_tunnels == 0
    # M1: no failed attempt may leave its writer behind in tunnel_writers.
    assert lease.tunnel_writers == set()


def test_reservation_waiter_wakes_when_peer_accounting_becomes_uncertain(mod):
    mgr = _FakeManager(["http://u:p@pool.invalid:10000"])
    lease = _make_sofascore_lease(mod, mgr)

    async def exercise():
        reservation = mod._reserve_lease_bytes(lease, lease.max_bytes)
        assert reservation > 0
        waiter = asyncio.create_task(mod._wait_for_reservation_turnover(lease))
        await asyncio.sleep(0)
        assert waiter.done() is False
        mod._latch_lease_accounting_uncertainty(lease)
        assert await asyncio.wait_for(waiter, 0.1) is None
        assert mod._RESERVATION_TURNOVER_WAITERS == set()
        assert lease.reserved_bytes == reservation
        assert lease.accounting_uncertain is True

    asyncio.run(exercise())


def test_fbref_never_repins_after_paid_connect_upload(mod, monkeypatch):
    mgr = _FakeManager(["http://u:p@pool.invalid:10001"])
    now = mod._wall_time()
    lease = mod.Lease(
        lease_id="fbref-one-attempt",
        token="secret",
        upstream=("pool.invalid", 10000, "u", "p"),
        created_at=now,
        expires_at=now + 30,
        max_bytes=4096,
        dag_id="dag_ingest_fbref",
        run_id="run",
        source="fbref",
    )
    _shrink_failover_timeouts(mod, monkeypatch)
    dead_writer = _FakeUpstreamWriter()

    async def fake_open(host, port):
        return _FakeUpstreamReader(b"", block_when_empty=True), dead_writer

    _patch_upstream_opener(mod, monkeypatch, fake_open)

    with pytest.raises(mod.UpstreamHeadTimeout):
        asyncio.run(
            mod._open_lease_upstream_tunnel(
                lease,
                mgr,
                target="www.fbref.com:443",
                host="www.fbref.com",
            )
        )

    auth = base64.b64encode(b"u:p").decode()
    expected = (
        b"CONNECT www.fbref.com:443 HTTP/1.1\r\n"
        b"Host: www.fbref.com:443\r\n"
        + f"Proxy-Authorization: Basic {auth}\r\n\r\n".encode()
    )
    assert dead_writer.data == expected
    assert dead_writer.closed is True
    assert lease.up_bytes == len(expected)
    assert lease.down_bytes == 0
    assert lease.upstream_repins == 0
    assert lease.upstream == ("pool.invalid", 10000, "u", "p")
    assert mgr.calls == 0


def test_fbref_never_retries_zero_byte_tcp_dial_failure(mod, monkeypatch):
    mgr = _FakeManager(["http://u:p@pool.invalid:10001"])
    now = mod._wall_time()
    lease = mod.Lease(
        lease_id="fbref-one-dial",
        token="secret",
        upstream=("pool.invalid", 10000, "u", "p"),
        created_at=now,
        expires_at=now + 30,
        max_bytes=4096,
        dag_id="dag_ingest_fbref",
        run_id="run",
        source="fbref",
    )
    _shrink_failover_timeouts(mod, monkeypatch)
    opens = []

    async def fake_open(host, port):
        opens.append((host, port))
        raise OSError("TCP dial failed")

    _patch_upstream_opener(mod, monkeypatch, fake_open)

    with pytest.raises(OSError, match="TCP dial failed"):
        asyncio.run(
            mod._open_lease_upstream_tunnel(
                lease,
                mgr,
                target="www.fbref.com:443",
                host="www.fbref.com",
            )
        )

    assert opens == [("pool.invalid", 10000)]
    assert lease.total_bytes == 0
    assert lease.upstream_repins == 0
    assert lease.upstream == ("pool.invalid", 10000, "u", "p")
    assert mgr.calls == 0


def test_lease_connect_failover_is_refused_after_first_provider_payload_byte(
    mod, monkeypatch
):
    mgr = _FakeManager(
        ["http://u:p@pool.invalid:10000", "http://u:p@pool.invalid:10001"]
    )
    lease = _make_sofascore_lease(mod, mgr)
    # A single down byte already arrived on the pinned exit. A later silent read
    # must retain escrow and fail closed rather than silently re-pin.
    mod._account_lease_bytes(lease, "www.sofascore.com", "down", 1)
    _shrink_failover_timeouts(mod, monkeypatch)

    async def fake_open(host, port):
        return _FakeUpstreamReader(b"", block_when_empty=True), _FakeUpstreamWriter()

    _patch_upstream_opener(mod, monkeypatch, fake_open)

    client_writer = _ClientWriter()
    asyncio.run(
        asyncio.wait_for(
            mod.handle(
                _ClientConnectReader(_connect_header_lines(lease)),
                client_writer,
                mgr,
                require_lease=True,
            ),
            2.0,
        )
    )

    assert b"502 Bad Gateway" in bytes(client_writer.payload)
    assert lease.upstream_repins == 0
    assert lease.upstream == ("pool.invalid", 10000, "u", "p")
    assert lease.usable is False
    assert lease.accounting_uncertain is True
    assert lease.reserved_bytes > 0
    assert lease.active_tunnels == 0
    assert lease.tunnel_writers == set()
    assert mgr.calls == 1


def test_failover_redraws_past_the_exit_that_just_failed(mod, monkeypatch):
    # The pool draw is random, so a re-pin can hand back the exact exit that
    # just closed without a byte. The failover must re-draw (bounded) until the
    # replacement differs from the failed one.
    mgr = _FakeManager(
        [
            "http://u:p@pool.invalid:10000",  # initial pin (dies)
            "http://u:p@pool.invalid:10000",  # first re-draw: same dead exit
            "http://u:p@pool.invalid:10001",  # second re-draw: fresh exit
        ]
    )
    lease = _make_sofascore_lease(mod, mgr)
    _shrink_failover_timeouts(mod, monkeypatch)

    live_head = b"HTTP/1.1 200 Connection established\r\n\r\n"
    opens = []

    async def fake_open(host, port):
        opens.append((host, port))
        if len(opens) == 1:
            return _FakeUpstreamReader(b""), _FakeUpstreamWriter()
        return _FakeUpstreamReader(live_head), _FakeUpstreamWriter()

    _patch_upstream_opener(mod, monkeypatch, fake_open)

    client_writer = _ClientWriter()
    asyncio.run(
        asyncio.wait_for(
            mod.handle(
                _ClientConnectReader(_connect_header_lines(lease)),
                client_writer,
                mgr,
                require_lease=True,
            ),
            2.0,
        )
    )

    assert b"200 Connection established" in bytes(client_writer.payload)
    assert lease.upstream == ("pool.invalid", 10001, "u", "p")
    assert lease.upstream_repins == 1  # one failover, even with the extra draw
    assert mgr.calls == 3
    assert opens[-1] == ("pool.invalid", 10001)


def test_provider_head_timeout_accounts_prefix_and_retains_unknown_escrow(
    mod, monkeypatch
):
    mgr = _FakeManager(["http://u:p@pool.invalid:10000"])
    lease = _make_sofascore_lease(mod, mgr)

    class Reader:
        def __init__(self):
            self.remaining = bytearray(b"HELLO")

        async def read(self, size):
            if self.remaining:
                chunk = bytes(self.remaining[:1])
                del self.remaining[:1]
                return chunk
            await asyncio.Event().wait()

    with pytest.raises(mod.UpstreamHeadTimeout):
        asyncio.run(
            asyncio.wait_for(
                mod._read_metered_provider_head(
                    Reader(),
                    lease,
                    "www.sofascore.com",
                    timeout_seconds=0.02,
                ),
                2.0,
            )
        )

    assert lease.down_bytes == 5
    assert lease.reserved_bytes == 4_096
    assert lease.accounting_uncertain is True
    assert lease.usable is False


def test_upstream_eof_before_any_head_byte_triggers_failover(mod, monkeypatch):
    mgr = _FakeManager(
        ["http://u:p@pool.invalid:10000", "http://u:p@pool.invalid:10001"]
    )
    lease = _make_sofascore_lease(mod, mgr)
    _shrink_failover_timeouts(mod, monkeypatch)

    live_head = b"HTTP/1.1 200 Connection established\r\n\r\n"
    opens = []

    async def fake_open(host, port):
        opens.append((host, port))
        if len(opens) == 1:
            # Immediate EOF before any head byte: a closed/reset exit.
            return _FakeUpstreamReader(b""), _FakeUpstreamWriter()
        return _FakeUpstreamReader(live_head), _FakeUpstreamWriter()

    _patch_upstream_opener(mod, monkeypatch, fake_open)

    client_writer = _ClientWriter()
    asyncio.run(
        asyncio.wait_for(
            mod.handle(
                _ClientConnectReader(_connect_header_lines(lease)),
                client_writer,
                mgr,
                require_lease=True,
            ),
            2.0,
        )
    )

    assert b"200 Connection established" in bytes(client_writer.payload)
    assert lease.upstream_repins == 1
    assert lease.upstream == ("pool.invalid", 10001, "u", "p")


def test_lease_failover_does_not_mint_second_lease_or_bypass_serial_limit(
    mod, monkeypatch
):
    mgr = _FakeManager(
        ["http://u:p@pool.invalid:10000", "http://u:p@pool.invalid:10001"]
    )
    lease = _make_sofascore_lease(mod, mgr)
    _shrink_failover_timeouts(mod, monkeypatch)

    live_head = b"HTTP/1.1 200 Connection established\r\n\r\n"
    opens = []
    observed = {}

    async def fake_open(host, port):
        opens.append((host, port))
        if len(opens) == 1:
            observed["leases_during"] = len(mod.LEASES)
            try:
                mod._create_lease(
                    mgr,
                    max_bytes=4096,
                    ttl_seconds=30,
                    metadata=_sofascore_context(run_id="concurrent__x::season"),
                    require_context=True,
                )
                observed["second"] = "MINTED"
            except RuntimeError as exc:
                observed["second"] = str(exc)
            return _FakeUpstreamReader(b""), _FakeUpstreamWriter()
        return _FakeUpstreamReader(live_head), _FakeUpstreamWriter()

    _patch_upstream_opener(mod, monkeypatch, fake_open)

    client_writer = _ClientWriter()
    asyncio.run(
        asyncio.wait_for(
            mod.handle(
                _ClientConnectReader(_connect_header_lines(lease)),
                client_writer,
                mgr,
                require_lease=True,
            ),
            2.0,
        )
    )

    assert observed["leases_during"] == 1
    assert "concurrency" in observed["second"]
    assert len(mod.LEASES) == 1
    assert lease.upstream_repins == 1


# --- metered SofaScore registry discovery (#946) ------------------------------


def _discovery_context(run_id="discovery__20260714T000000Z"):
    return {
        "dag_id": "dag_discover_sofascore_registry",
        "run_id": run_id,
        "task_id": "discover_sofascore_registry",
        "canonical_url": "https://api.sofascore.com/",
        "source": "sofascore_discovery",
        "scope": "discovery",
    }


def test_discovery_lease_is_refused_until_a_cap_is_authorized(mod):
    mgr = _FakeManager(["http://u:p@pool.invalid:10000"])

    assert mod.SOFASCORE_DISCOVERY_DAGRUN_BUDGET_BYTES == 0
    assert mod._source_for_dag("dag_discover_sofascore_registry") == (
        "sofascore_discovery"
    )
    with pytest.raises(RuntimeError, match="discovery lease unavailable"):
        mod._create_lease(
            mgr,
            max_bytes=1_000_000,
            ttl_seconds=3600,
            metadata=_discovery_context(),
            require_context=True,
        )


def test_authorized_discovery_lease_is_capped_by_its_dagrun_budget(mod):
    mgr = _FakeManager(["http://u:p@pool.invalid:10000"])
    mod.SOFASCORE_DISCOVERY_DAGRUN_BUDGET_BYTES = 12 * 1024 * 1024

    lease = mod._create_lease(
        mgr,
        max_bytes=8 * 1024 * 1024,
        ttl_seconds=3600,
        metadata=_discovery_context(),
        require_context=True,
    )

    assert lease.source == "sofascore_discovery"
    assert lease.max_bytes == 8 * 1024 * 1024
    report = lease.report()
    assert report["dagrun_budget_bytes"] == 12 * 1024 * 1024
    # Discovery carries no signed plan, no allocation and no canary artifact.
    assert report["plan_digest"] == ""
    assert report["allocation_id"] == ""
    assert report["budget_artifact_id"] == ""
    # WhoScored remains zero until a signed campaign supplies exact caps.
    assert mod._dagrun_budget_bytes("dag_ingest_whoscored") == 0


def test_discovery_lease_is_not_truncated_by_the_2mb_per_url_ceiling(mod):
    # Every discovery request hits one canonical API origin, so the legacy
    # per-URL ceiling would strangle a scan into 2 MB no matter its DagRun cap.
    mgr = _FakeManager(["http://u:p@pool.invalid:10000"])
    mod.URL_BUDGET_BYTES = 2_000_000
    mod.SOFASCORE_DISCOVERY_DAGRUN_BUDGET_BYTES = 16 * 1024 * 1024

    lease = mod._create_lease(
        mgr,
        max_bytes=8 * 1024 * 1024,
        ttl_seconds=3600,
        metadata=_discovery_context(),
        require_context=True,
    )

    assert lease.max_bytes == 8 * 1024 * 1024 > 2_000_000
    assert mod._lease_url_budget_bytes(lease) == 16 * 1024 * 1024
    assert lease.report()["url_budget_bytes"] == 16 * 1024 * 1024
    # And the whole-scan ceiling still applies across consecutive leases.
    mod._account_lease_bytes(lease, "api.sofascore.com", "down", 8 * 1024 * 1024)
    lease.closed = True
    second = mod._create_lease(
        mgr,
        max_bytes=8 * 1024 * 1024,
        ttl_seconds=3600,
        metadata=_discovery_context(),
        require_context=True,
    )
    assert second.max_bytes == 8 * 1024 * 1024
    mod._account_lease_bytes(second, "api.sofascore.com", "down", 8 * 1024 * 1024)
    second.closed = True
    with pytest.raises(RuntimeError, match="budget exhausted"):
        mod._create_lease(
            mgr,
            max_bytes=1024,
            ttl_seconds=3600,
            metadata=_discovery_context(),
            require_context=True,
        )


def test_discovery_scan_is_serial(mod):
    mgr = _FakeManager(
        ["http://u:p@pool.invalid:10000", "http://u:p@pool.invalid:10001"]
    )
    mod.MAX_ACTIVE_LEASES = 4
    mod.SOFASCORE_DISCOVERY_DAGRUN_BUDGET_BYTES = 12 * 1024 * 1024

    first = mod._create_lease(
        mgr,
        max_bytes=1_000_000,
        ttl_seconds=3600,
        metadata=_discovery_context(),
        require_context=True,
    )
    with pytest.raises(RuntimeError, match="discovery paid-proxy concurrency"):
        mod._create_lease(
            mgr,
            max_bytes=1_000_000,
            ttl_seconds=3600,
            metadata=_discovery_context(run_id="discovery__other"),
            require_context=True,
        )

    first.closed = True
    rotated = mod._create_lease(
        mgr,
        max_bytes=1_000_000,
        ttl_seconds=3600,
        metadata=_discovery_context(),
        require_context=True,
    )
    # The next lease in the same scan is pinned to a fresh residential exit.
    assert rotated.upstream[1] == 10001


def test_discovery_source_cannot_be_claimed_by_another_dag(mod):
    mgr = _FakeManager(["http://u:p@pool.invalid:10000"])
    mod.SOFASCORE_DISCOVERY_DAGRUN_BUDGET_BYTES = 12 * 1024 * 1024

    with pytest.raises(ValueError, match="source does not match dag_id"):
        mod._create_lease(
            mgr,
            max_bytes=1_000_000,
            ttl_seconds=3600,
            metadata={
                **_discovery_context(),
                "dag_id": "dag_ingest_sofascore",
            },
            require_context=True,
        )


def test_discovery_budget_is_reported_by_health_and_defaults_to_disabled():
    compose = _COMPOSE_PATH.read_text()
    service = compose.split("  proxy_filter:\n", 1)[1].split("\n  caddy:\n", 1)[0]
    source = _SCRIPT_PATH.read_text()

    assert "--sofascore-discovery-dagrun-budget-bytes" in service
    assert "${PROXY_FILTER_SOFASCORE_DISCOVERY_DAGRUN_BUDGET_BYTES:-0}" in service
    assert '"sofascore_discovery_enabled"' in source
    assert '"sofascore_discovery_dagrun_budget_bytes"' in source
    env_example = (REPO_ROOT / ".env.example").read_text()
    assert "PROXY_FILTER_SOFASCORE_DISCOVERY_DAGRUN_BUDGET_BYTES=0" in env_example


def _initialize_protected_whoscored_state(mod, tmp_path):
    mod.SOURCE_MODE = "whoscored-only"
    mod.WHOSCORED_PROXY_APPROVAL_HMAC_SECRET = "approval-" + "a" * 32
    mod.WHOSCORED_PROXY_LEDGER_HMAC_SECRET = "ledger-" + "l" * 32
    mod.LEDGER_PATH = str(tmp_path / "paid_requests.jsonl")
    mod.WHOSCORED_CAMPAIGN_LEDGER_PATH = str(tmp_path / "whoscored_campaigns.json")
    mod.WHOSCORED_STATE_MARKER_PATH = str(
        tmp_path / ".whoscored_state_initialized.json"
    )
    mod.WHOSCORED_CAMPAIGN_LEDGER = None
    mod._WHOSCORED_CAMPAIGN_LEDGER_KEY = None
    mod.WHOSCORED_STATE_ID = ""
    mod._PAID_LEDGER_CHAIN_COUNT = 0
    mod._PAID_LEDGER_CHAIN_OFFSET = 0
    mod._PAID_LEDGER_CHAIN_TAIL = ""
    report = tmp_path / "bytes.json"
    mod._initialize_whoscored_state(str(report))
    return report


def test_protected_whoscored_state_initializes_once_and_survives_restart(mod, tmp_path):
    report = _initialize_protected_whoscored_state(mod, tmp_path)
    marker = json.loads(Path(mod.WHOSCORED_STATE_MARKER_PATH).read_text())
    assert marker["schema_version"] == mod.WHOSCORED_STATE_MARKER_SCHEMA_VERSION
    assert marker["order_id"] == mod.WHOSCORED_PROVIDER_ORDER_ID
    assert (
        marker["provider_policy_sha256"]
        == mod.WHOSCORED_PROVIDER_POLICY_SHA256
    )

    mod.WHOSCORED_CAMPAIGN_LEDGER = None
    mod._WHOSCORED_CAMPAIGN_LEDGER_KEY = None
    mod.WHOSCORED_STATE_ID = ""
    mod._PAID_LEDGER_CHAIN_TAIL = ""
    mod._verify_whoscored_state(str(report))

    protected = [
        report,
        Path(mod.LEDGER_PATH),
        Path(mod.WHOSCORED_CAMPAIGN_LEDGER_PATH),
        Path(mod._paid_ledger_checkpoint_path()),
        Path(mod.WHOSCORED_STATE_MARKER_PATH),
    ]
    assert all(path.exists() for path in protected)
    assert all(path.stat().st_mode & 0o777 == 0o600 for path in protected)
    with pytest.raises(RuntimeError, match="empty state"):
        mod._initialize_whoscored_state(str(report))


@pytest.mark.parametrize(
    ("field", "replacement"),
    (
        ("WHOSCORED_PROVIDER_ORDER_ID", "proxysio-order-another"),
        ("WHOSCORED_PROVIDER_POLICY_SHA256", "c" * 64),
    ),
)
def test_protected_whoscored_state_rejects_provider_binding_mismatch(
    mod, tmp_path, field, replacement
):
    report = _initialize_protected_whoscored_state(mod, tmp_path)
    setattr(mod, field, replacement)
    mod.WHOSCORED_STATE_ID = ""

    with pytest.raises(RuntimeError, match="provider binding differs"):
        mod._verify_whoscored_state(str(report))


def test_legacy_state_marker_is_explicit_and_never_accepts_scheduled_v3(
    mod, tmp_path
):
    report = _initialize_protected_whoscored_state(mod, tmp_path)
    marker_path = Path(mod.WHOSCORED_STATE_MARKER_PATH)
    current = json.loads(marker_path.read_text())
    legacy_body = {
        "schema_version": mod.WHOSCORED_STATE_SCHEMA_VERSION,
        "state_id": current["state_id"],
        "created_at": current["created_at"],
        "path_sha256": current["path_sha256"],
    }
    legacy = {**legacy_body, "signature": mod._state_hmac(legacy_body)}
    marker_path.write_bytes(mod.canonical_json_bytes(legacy) + b"\n")
    marker_path.chmod(0o600)
    mod.WHOSCORED_STATE_ID = ""

    with pytest.raises(RuntimeError, match="marker fields are invalid"):
        mod._verify_whoscored_state(str(report))

    mod._verify_whoscored_state(str(report), allow_legacy_marker=True)
    assert mod.WHOSCORED_LEGACY_STATE_MARKER_LOADED is True
    mod._assert_whoscored_approval_state_binding(SimpleNamespace(schema_version=2))
    with pytest.raises(
        ProxyCampaignValidationError, match="provider-bound state marker"
    ):
        mod._assert_whoscored_approval_state_binding(
            SimpleNamespace(schema_version=3)
        )


@pytest.mark.parametrize(
    ("order_id", "policy_sha256"),
    (
        ("proxysio-order-another", "b" * 64),
        ("proxysio-order-38950", "c" * 64),
    ),
)
def test_scheduled_authority_must_match_current_state_binding(
    mod, order_id, policy_sha256
):
    mod._assert_whoscored_approval_state_binding(
        SimpleNamespace(
            schema_version=mod.SCHEDULED_PROXY_CAMPAIGN_SCHEMA_VERSION,
            scheduled_authority=SimpleNamespace(
                order_id=mod.WHOSCORED_PROVIDER_ORDER_ID,
                provider_policy_sha256=mod.WHOSCORED_PROVIDER_POLICY_SHA256,
            ),
        )
    )
    approval = SimpleNamespace(
        schema_version=mod.SCHEDULED_PROXY_CAMPAIGN_SCHEMA_VERSION,
        scheduled_authority=SimpleNamespace(
            order_id=order_id,
            provider_policy_sha256=policy_sha256,
        ),
    )

    with pytest.raises(ProxyCampaignValidationError, match="state binding"):
        mod._assert_whoscored_approval_state_binding(approval)


def test_protected_whoscored_state_rejects_campaign_lock_symlink(mod, tmp_path):
    mod.SOURCE_MODE = "whoscored-only"
    mod.WHOSCORED_PROXY_APPROVAL_HMAC_SECRET = "approval-" + "a" * 32
    mod.WHOSCORED_PROXY_LEDGER_HMAC_SECRET = "ledger-" + "l" * 32
    mod.LEDGER_PATH = str(tmp_path / "paid_requests.jsonl")
    mod.WHOSCORED_CAMPAIGN_LEDGER_PATH = str(tmp_path / "whoscored_campaigns.json")
    mod.WHOSCORED_STATE_MARKER_PATH = str(
        tmp_path / ".whoscored_state_initialized.json"
    )
    mod.WHOSCORED_CAMPAIGN_LEDGER = None
    mod._WHOSCORED_CAMPAIGN_LEDGER_KEY = None
    target = tmp_path / "unrelated-private-state"
    target.write_text("must-stay-unchanged", encoding="utf-8")
    target.chmod(0o600)
    lock_path = Path(mod.WHOSCORED_CAMPAIGN_LEDGER_PATH + ".lock")
    lock_path.symlink_to(target)

    with pytest.raises(ProxyCampaignError, match="lock is unavailable"):
        mod._initialize_whoscored_state(str(tmp_path / "bytes.json"))

    assert target.read_text(encoding="utf-8") == "must-stay-unchanged"
    assert target.stat().st_mode & 0o777 == 0o600
    assert not Path(mod.WHOSCORED_STATE_MARKER_PATH).exists()


@pytest.mark.parametrize(
    "missing_name",
    ["report", "paid_ledger", "campaign_ledger", "checkpoint", "marker"],
)
def test_protected_whoscored_state_missing_file_fails_closed(
    mod, tmp_path, missing_name
):
    report = _initialize_protected_whoscored_state(mod, tmp_path)
    paths = {
        "report": report,
        "paid_ledger": Path(mod.LEDGER_PATH),
        "campaign_ledger": Path(mod.WHOSCORED_CAMPAIGN_LEDGER_PATH),
        "checkpoint": Path(mod._paid_ledger_checkpoint_path()),
        "marker": Path(mod.WHOSCORED_STATE_MARKER_PATH),
    }
    paths[missing_name].unlink()
    mod.WHOSCORED_CAMPAIGN_LEDGER = None
    mod._WHOSCORED_CAMPAIGN_LEDGER_KEY = None
    mod.WHOSCORED_STATE_ID = ""

    with pytest.raises((RuntimeError, ProxyCampaignError)):
        mod._verify_whoscored_state(str(report))


def test_paid_ledger_whole_line_truncation_is_detected_by_signed_checkpoint(
    mod, tmp_path
):
    report = _initialize_protected_whoscored_state(mod, tmp_path)
    mod._verify_whoscored_state(str(report))
    lease = mod.Lease(
        lease_id="lease-one",
        token="lease-token",
        upstream=("pool.invalid", 443, "u", "p"),
        created_at=time.time(),
        expires_at=time.time() + 60,
        max_bytes=1000,
        source="whoscored",
        dag_id="dag_backfill_whoscored",
        run_id="manual__campaign-one",
        task_id="run_whoscored_proxy_canary",
        canonical_url="https://www.whoscored.com/Matches/1/Live",
    )
    mod._append_budget_event("bytes", lease, direction="down", bytes=10)
    assert Path(mod.LEDGER_PATH).read_bytes().endswith(b"\n")

    Path(mod.LEDGER_PATH).write_bytes(b"")
    with pytest.raises(RuntimeError, match="truncated|checkpoint"):
        mod._verify_paid_ledger_chain(mod.LEDGER_PATH)


def test_missing_protected_state_stops_before_pool_or_listener(
    mod, monkeypatch, tmp_path
):
    args = SimpleNamespace(
        source_mode="whoscored-only",
        listen="127.0.0.1:0",
        lease_listen="127.0.0.1:0",
        out=str(tmp_path / "bytes.json"),
        ledger=str(tmp_path / "paid_requests.jsonl"),
        whoscored_campaign_ledger=str(tmp_path / "whoscored_campaigns.json"),
        whoscored_state_marker=str(tmp_path / ".initialized.json"),
        whoscored_provider_order_id="proxysio-order-38950",
        whoscored_provider_policy_sha256="b" * 64,
        daily_budget_bytes=300_000_000,
        max_lease_bytes=2_000_000,
        transfermarkt_dagrun_budget_bytes=0,
        sofascore_canary_hard_cap_bytes=0,
        sofascore_discovery_dagrun_budget_bytes=0,
        sofascore_budget_artifact=None,
        initialize_whoscored_state=False,
    )
    monkeypatch.setattr(mod.argparse.ArgumentParser, "parse_args", lambda self: args)
    monkeypatch.setattr(
        mod._WHOSCORED_RUNTIME_CONTRACT,
        "require_production_runtime_class",
        lambda **_kwargs: None,
    )
    monkeypatch.setattr(
        mod._WHOSCORED_RUNTIME_CONTRACT,
        "validate_runtime_contract",
        lambda **_kwargs: {"code_tree_sha256": "a" * 64},
    )
    pool_calls = []
    monkeypatch.setattr(
        mod,
        "_residential_manager",
        lambda **_kwargs: pool_calls.append("pool") or pytest.fail("pool opened"),
    )
    monkeypatch.setattr(
        mod.asyncio,
        "start_server",
        lambda *_args, **_kwargs: pytest.fail("listener opened"),
    )
    monkeypatch.setenv("PROXY_FILTER_CONTROL_TOKEN", "c" * 32)
    monkeypatch.setenv("WHOSCORED_PROXY_APPROVAL_HMAC_SECRET", "a" * 32)
    monkeypatch.setenv("WHOSCORED_PROXY_LEDGER_HMAC_SECRET", "l" * 32)

    with pytest.raises(SystemExit, match="protected state rejected"):
        asyncio.run(mod.main())
    assert pool_calls == []


@pytest.mark.parametrize(
    ("daily_bytes", "lease_bytes"),
    (
        (None, None),
        (300_000_001, 2_000_000),
        (300_000_000, 2_000_001),
    ),
)
def test_whoscored_only_requires_exact_decimal_byte_caps_before_state(
    mod, monkeypatch, daily_bytes, lease_bytes
):
    args = SimpleNamespace(
        source_mode="whoscored-only",
        allow_legacy_noauth=False,
        daily_budget_bytes=daily_bytes,
        max_lease_bytes=lease_bytes,
    )
    monkeypatch.setattr(mod.argparse.ArgumentParser, "parse_args", lambda self: args)
    monkeypatch.setattr(
        mod._WHOSCORED_RUNTIME_CONTRACT,
        "require_production_runtime_class",
        lambda **_kwargs: None,
    )
    monkeypatch.setattr(
        mod._WHOSCORED_RUNTIME_CONTRACT,
        "validate_runtime_contract",
        lambda **_kwargs: {"code_tree_sha256": "a" * 64},
    )
    monkeypatch.setattr(
        mod,
        "_verify_whoscored_state",
        lambda *_args, **_kwargs: pytest.fail("protected state must not be read"),
    )
    monkeypatch.setenv("PROXY_FILTER_CONTROL_TOKEN", "c" * 32)
    monkeypatch.setenv("WHOSCORED_PROXY_APPROVAL_HMAC_SECRET", "a" * 32)
    monkeypatch.setenv("WHOSCORED_PROXY_LEDGER_HMAC_SECRET", "l" * 32)

    with pytest.raises(SystemExit, match="exact byte caps"):
        asyncio.run(mod.main())


def test_whoscored_only_rejects_cross_source_budget_before_state(mod, monkeypatch):
    args = SimpleNamespace(
        source_mode="whoscored-only",
        allow_legacy_noauth=False,
        daily_budget_bytes=300_000_000,
        max_lease_bytes=2_000_000,
        transfermarkt_dagrun_budget_bytes=0,
        sofascore_canary_hard_cap_bytes=0,
        sofascore_discovery_dagrun_budget_bytes=0,
        sofascore_budget_artifact="/cross-source-policy.json",
    )
    monkeypatch.setattr(mod.argparse.ArgumentParser, "parse_args", lambda self: args)
    monkeypatch.setattr(
        mod._WHOSCORED_RUNTIME_CONTRACT,
        "require_production_runtime_class",
        lambda **_kwargs: None,
    )
    monkeypatch.setattr(
        mod._WHOSCORED_RUNTIME_CONTRACT,
        "validate_runtime_contract",
        lambda **_kwargs: {"code_tree_sha256": "a" * 64},
    )
    monkeypatch.setattr(
        mod,
        "_verify_whoscored_state",
        lambda *_args, **_kwargs: pytest.fail("protected state must not be read"),
    )
    monkeypatch.setenv("PROXY_FILTER_CONTROL_TOKEN", "c" * 32)
    monkeypatch.setenv("WHOSCORED_PROXY_APPROVAL_HMAC_SECRET", "a" * 32)
    monkeypatch.setenv("WHOSCORED_PROXY_LEDGER_HMAC_SECRET", "l" * 32)

    with pytest.raises(SystemExit, match="cross-source"):
        asyncio.run(mod.main())


def test_whoscored_only_rejects_legacy_noauth_before_runtime_or_state(
    mod, monkeypatch
):
    args = SimpleNamespace(
        source_mode="whoscored-only",
        allow_legacy_noauth=True,
    )
    monkeypatch.setattr(mod.argparse.ArgumentParser, "parse_args", lambda self: args)
    monkeypatch.setattr(
        mod._WHOSCORED_RUNTIME_CONTRACT,
        "require_production_runtime_class",
        lambda **_kwargs: pytest.fail("runtime validation must not run"),
    )
    monkeypatch.setattr(
        mod,
        "_residential_manager",
        lambda **_kwargs: pytest.fail("pool must not be loaded"),
    )

    with pytest.raises(SystemExit, match="allow-legacy-noauth is forbidden"):
        asyncio.run(mod.main())


def test_whoscored_endpoint_boundaries_are_durable_and_sum_to_close_receipt(
    mod, monkeypatch
):
    mgr = _FakeManager(["http://u:p@pool.proxys.io:10000"])
    lease = mod._create_lease(
        mgr,
        max_bytes=1_000,
        ttl_seconds=30,
        metadata=_whoscored_campaign_context(mod, cap=1_000),
        require_context=True,
    )
    events = []
    monkeypatch.setattr(
        mod,
        "_append_budget_event",
        lambda event_type, _lease, **values: events.append((event_type, values)),
    )
    mod.SOURCE_MODE = "whoscored-only"
    mod._whoscored_campaign_ledger().record_request(
        lease.proxy_campaign_approval,
        lease.proxy_campaign_claim,
    )
    request_id = mod._begin_endpoint_request(lease, "target:" + "a" * 64)
    mod._account_lease_bytes(lease, "www.whoscored.com", "down", 321)

    assert mod._finish_endpoint_request(lease, request_id) == 321
    report = asyncio.run(mod._close_lease(lease, completed=False))

    assert report["close_complete"] is True
    assert report["endpoint_request_provider_bytes"] == {
        "target:" + "a" * 64: [321]
    }
    assert [event_type for event_type, _values in events] == [
        "endpoint_started",
        "bytes",
        "endpoint_finished",
        "lease_closed",
    ]
    assert events[1][1]["endpoint"] == "target:" + "a" * 64
    assert events[2][1]["provider_bytes"] == 321
    assert events[-1][1]["endpoint_request_provider_bytes"] == {
        "target:" + "a" * 64: [321]
    }


def test_whoscored_only_background_bytes_revoke_unowned_lease(mod):
    mgr = _FakeManager(["http://u:p@pool.proxys.io:10000"])
    lease = mod._create_lease(
        mgr,
        max_bytes=1_000,
        ttl_seconds=30,
        metadata=_whoscored_campaign_context(mod, cap=1_000),
        require_context=True,
    )
    mod.SOURCE_MODE = "whoscored-only"

    with pytest.raises(RuntimeError, match="no active endpoint owner"):
        mod._account_lease_bytes(
            lease, "www.whoscored.com", "down", 1
        )

    assert lease.accounting_uncertain is True
    assert lease.global_budget_escrow_bytes == 1_000
    with pytest.raises(ProxyCampaignError):
        mod._whoscored_campaign_ledger().record_request(
            lease.proxy_campaign_approval,
            lease.proxy_campaign_claim,
        )


def test_whoscored_batch_claim_binds_exact_endpoints_and_atomic_switches(
    mod, tmp_path
):
    report_path = _initialize_protected_whoscored_state(mod, tmp_path)
    mod._verify_whoscored_state(str(report_path))
    mgr = _FakeManager(["http://u:p@pool.proxys.io:10000"])
    bootstrap = "bootstrap:" + "a" * 64
    first = "target:" + "b" * 64
    second = "target:" + "c" * 64
    labels = (bootstrap, *sorted((first, second)))
    metadata = _whoscored_campaign_context(
        mod, cap=1_000, requests=4, leases=2
    )
    metadata.update(
        target_manifest_sha256="d" * 64,
        logical_target_units=2,
        expected_endpoint_labels=list(labels),
    )
    lease = mod._create_lease(
        mgr,
        max_bytes=1_000,
        ttl_seconds=30,
        metadata=metadata,
        require_context=True,
    )
    lease_created = json.loads(Path(mod.LEDGER_PATH).read_text().splitlines()[-1])
    assert {
        field: lease_created[field]
        for field in (
            "target_manifest_sha256",
            "logical_target_units",
            "expected_endpoint_labels",
        )
    } == {
        "target_manifest_sha256": "d" * 64,
        "logical_target_units": 2,
        "expected_endpoint_labels": list(labels),
    }
    mod._whoscored_campaign_ledger().record_request(
        lease.proxy_campaign_approval,
        lease.proxy_campaign_claim,
    )

    request_id = mod._begin_endpoint_request(lease, bootstrap)
    bootstrap_request_id = request_id
    mod._account_lease_bytes(lease, "www.whoscored.com", "down", 100)
    request_id = mod._switch_endpoint_request(lease, request_id, first)
    first_request_id = request_id
    mod._account_lease_bytes(lease, "www.whoscored.com", "down", 200)
    request_id = mod._switch_endpoint_request(lease, request_id, second)
    mod._account_lease_bytes(lease, "www.whoscored.com", "down", 300)
    assert mod._finish_endpoint_request(lease, request_id) == 300
    report = asyncio.run(mod._close_lease(lease, completed=False))

    assert report["close_complete"] is True
    assert report["target_manifest_sha256"] == "d" * 64
    assert report["logical_target_units"] == 2
    assert report["expected_endpoint_labels"] == list(labels)
    assert report["endpoint_request_provider_bytes"] == {
        bootstrap: [100],
        first: [200],
        second: [300],
    }
    durable_events = [
        json.loads(line) for line in Path(mod.LEDGER_PATH).read_text().splitlines()
    ]
    switch_events = [
        {
            field: event[field]
            for field in (
                "request_id",
                "endpoint",
                "provider_bytes",
                "next_request_id",
                "next_endpoint",
                "lease_total_bytes",
            )
        }
        for event in durable_events
        if event["event_type"] == "endpoint_switched"
    ]
    assert switch_events == [
        {
            "request_id": bootstrap_request_id,
            "endpoint": bootstrap,
            "provider_bytes": 100,
            "next_request_id": first_request_id,
            "next_endpoint": first,
            "lease_total_bytes": 100,
        },
        {
            "request_id": first_request_id,
            "endpoint": first,
            "provider_bytes": 200,
            "next_request_id": request_id,
            "next_endpoint": second,
            "lease_total_bytes": 300,
        },
    ]
    snapshot = mod._whoscored_campaign_ledger().snapshot(
        lease.proxy_campaign_approval
    )
    assert snapshot["leases_used"] == 2
    attempt = snapshot["allocations"][lease.allocation_id]["attempts"][0]
    assert attempt["target_manifest_sha256"] == "d" * 64
    assert attempt["logical_target_units"] == 2
    assert attempt["expected_endpoint_labels"] == list(labels)


def test_whoscored_batch_units_survive_restart_and_bound_retries_and_dials(mod):
    metadata = _whoscored_campaign_context(
        mod, cap=1_000, requests=2, leases=2
    )
    approval = ProxyCampaignApproval.from_dict(
        metadata["proxy_campaign_approval"]
    )
    labels = (
        "bootstrap:" + "a" * 64,
        "target:" + "b" * 64,
        "target:" + "c" * 64,
    )
    now = datetime.now(timezone.utc)
    ledger = ProxyCampaignLedger(
        mod.WHOSCORED_CAMPAIGN_LEDGER_PATH,
        secret=mod.WHOSCORED_PROXY_LEDGER_HMAC_SECRET,
        approval_secret=mod.WHOSCORED_PROXY_APPROVAL_HMAC_SECRET,
    )
    claim = ledger.claim(
        approval,
        metadata["proxy_allocation_id"],
        dag_id=metadata["dag_id"],
        run_id=metadata["run_id"],
        task_id=metadata["task_id"],
        attempt_id=metadata["proxy_attempt_id"],
        lease_id="lease-before-restart",
        expires_at=now + timedelta(seconds=1),
        canonical_url=metadata["canonical_url"],
        target_manifest_sha256="d" * 64,
        logical_target_units=2,
        expected_endpoint_labels=labels,
        now=now,
    )
    assert ledger.record_request(approval, claim, now=now) == 1
    assert ledger.record_request(approval, claim, now=now) == 2
    with pytest.raises(ProxyCampaignBudgetExceeded, match="request limit"):
        ledger.record_request(approval, claim, now=now)

    restarted = ProxyCampaignLedger(
        mod.WHOSCORED_CAMPAIGN_LEDGER_PATH,
        secret=mod.WHOSCORED_PROXY_LEDGER_HMAC_SECRET,
        approval_secret=mod.WHOSCORED_PROXY_APPROVAL_HMAC_SECRET,
        require_existing=True,
    )
    for index, manifest in enumerate(("d" * 64, "e" * 64), start=1):
        with pytest.raises(ProxyCampaignBudgetExceeded, match="lease limit"):
            restarted.claim(
                approval,
                metadata["proxy_allocation_id"],
                dag_id=metadata["dag_id"],
                run_id=metadata["run_id"],
                task_id=metadata["task_id"],
                attempt_id=metadata["proxy_attempt_id"],
                lease_id=f"lease-retry-{index}",
                expires_at=now + timedelta(seconds=10 + index),
                canonical_url=metadata["canonical_url"],
                target_manifest_sha256=manifest,
                logical_target_units=2,
                expected_endpoint_labels=labels,
                now=now + timedelta(seconds=2),
            )

# --- FBref browser-phase lease cap extension ---------------------------------


def _fbref_context(**values):
    context = {
        "source": "fbref",
        "dag_id": "dag_ingest_fbref",
        "run_id": "manual__fbref-cap",
        "task_id": "run_live_waves",
        "canonical_url": "https://fbref.com/en/",
    }
    context.update(values)
    return context


def _make_fbref_lease(mod, mgr, *, max_bytes=1000):
    mod.DAILY_BUDGET_BYTES = 5000
    mod.DAGRUN_BUDGET_BYTES = 5000
    mod.URL_BUDGET_BYTES = 5000
    mod.MAX_LEASE_BYTES = 5000
    return mod._create_lease(
        mgr,
        max_bytes=max_bytes,
        ttl_seconds=30,
        metadata=_fbref_context(),
        require_context=True,
    )


def test_fbref_absolute_http_replaces_browser_lease_auth_with_provider_auth(
    mod, monkeypatch
):
    mgr = _FakeManager(["http://u:p@pool.invalid:10000"])
    lease = _make_fbref_lease(mod, mgr)
    client_auth = base64.b64encode(f"lease:{lease.token}".encode()).decode()
    provider_auth = base64.b64encode(b"u:p").decode()
    target = "http://www.fbref.com/en/"
    upstream_reader = _FakeUpstreamReader(
        b"HTTP/1.1 204 No Content\r\nContent-Length: 0\r\n\r\n"
    )
    upstream_writer = _FakeUpstreamWriter()

    async def fake_open(host, port):
        assert (host, port) == ("pool.invalid", 10000)
        return upstream_reader, upstream_writer

    monkeypatch.setattr(mod, "_open_upstream_connection", fake_open)
    client_writer = _ClientWriter()
    asyncio.run(
        mod.handle(
            _ClientConnectReader(
                [
                    f"GET {target} HTTP/1.1\r\n".encode(),
                    b"Host: www.fbref.com\r\n",
                    b"User-Agent: browser-test\r\n",
                    f"Proxy-Authorization: Basic {client_auth}\r\n".encode(),
                    b"\r\n",
                ]
            ),
            client_writer,
            mgr,
            require_lease=True,
        )
    )

    forwarded = bytes(upstream_writer.data)
    assert forwarded == (
        f"GET {target} HTTP/1.1\r\n".encode()
        + b"Host: www.fbref.com\r\n"
        + b"User-Agent: browser-test\r\n"
        + f"Proxy-Authorization: Basic {provider_auth}\r\n\r\n".encode()
    )
    assert forwarded.count(b"Proxy-Authorization:") == 1
    assert client_auth.encode() not in forwarded
    assert lease.token.encode() not in forwarded
    assert b"lease:" not in forwarded
    assert lease.active_tunnels == 0
    assert lease.tunnel_writers == set()


def test_fbref_drained_lease_extension_is_durable_before_cap_mutation(mod):
    mgr = _FakeManager(["http://u:p@pool.invalid:10000"])
    lease = _make_fbref_lease(mod, mgr)
    mod._account_lease_bytes(lease, "www.fbref.com", "down", 200)

    report = mod._extend_fbref_lease(lease, 3000)

    assert lease.max_bytes == 3000
    assert report["max_bytes"] == 3000
    events = [
        json.loads(line) for line in Path(mod.LEDGER_PATH).read_text().splitlines()
    ]
    extended = events[-1]
    assert extended["event_type"] == "lease_extended"
    assert extended["previous_max_bytes"] == 1000
    assert extended["max_bytes"] == 3000
    assert extended["lease_total_bytes"] == 200


@pytest.mark.parametrize(
    "state",
    [
        "active_tunnels",
        "reserved_bytes",
        "current_request_id",
        "current_endpoint",
        "closed",
        "expired",
    ],
)
def test_fbref_lease_extension_refuses_non_idle_or_closed_state(mod, state):
    mgr = _FakeManager(["http://u:p@pool.invalid:10000"])
    lease = _make_fbref_lease(mod, mgr)
    if state == "expired":
        lease.expires_at = 0
    elif state in {"current_request_id", "current_endpoint"}:
        setattr(lease, state, "request-1")
    else:
        setattr(lease, state, 1 if state != "closed" else True)

    with pytest.raises(RuntimeError):
        mod._extend_fbref_lease(lease, 2000)

    assert lease.max_bytes == 1000


def test_fbref_lease_extension_rejects_shared_budget_overcommit(mod):
    mgr = _FakeManager(["http://u:p@pool.invalid:10000"])
    lease = _make_fbref_lease(mod, mgr)
    mod._account_lease_bytes(lease, "www.fbref.com", "down", 200)
    mod._run_down_bytes[lease.dagrun_key] += 3500

    with pytest.raises(RuntimeError, match="remaining shared budget"):
        mod._extend_fbref_lease(lease, 1600)

    assert mod._fbref_lease_extension_ceiling(lease) == 1500
    assert lease.max_bytes == 1000


def test_fbref_lease_extension_ledger_failure_leaves_old_hard_cap(mod, monkeypatch):
    mgr = _FakeManager(["http://u:p@pool.invalid:10000"])
    lease = _make_fbref_lease(mod, mgr)

    def fail_ledger(*_args, **_kwargs):
        raise OSError("fsync failed")

    monkeypatch.setattr(mod, "_append_budget_event", fail_ledger)

    with pytest.raises(OSError, match="fsync failed"):
        mod._extend_fbref_lease(lease, 2000)

    assert lease.max_bytes == 1000


def test_fbref_extend_control_requires_both_control_and_lease_tokens(mod):
    mgr = _FakeManager(["http://u:p@pool.invalid:10000"])
    lease = _make_fbref_lease(mod, mgr)
    request = json.dumps({"max_bytes": 2000}).encode()

    class Reader:
        async def readexactly(self, length):
            assert length == len(request)
            return request

    class Writer:
        def __init__(self):
            self.payload = bytearray()

        def write(self, value):
            self.payload.extend(value)

        async def drain(self):
            return None

        def close(self):
            return None

    for headers in (
        {"authorization": f"Bearer {lease.token}"},
        {"x-proxy-control-token": mod.CONTROL_TOKEN, "authorization": "Bearer bad"},
    ):
        writer = Writer()
        asyncio.run(
            mod._handle_control(
                "POST",
                f"/v1/leases/{lease.lease_id}/extend",
                {"content-length": str(len(request)), **headers},
                Reader(),
                writer,
                mgr,
            )
        )
        assert b"401 Unauthorized" in writer.payload
        assert lease.max_bytes == 1000

    writer = Writer()
    asyncio.run(
        mod._handle_control(
            "POST",
            f"/v1/leases/{lease.lease_id}/extend",
            {
                "content-length": str(len(request)),
                "x-proxy-control-token": mod.CONTROL_TOKEN,
                "authorization": f"Bearer {lease.token}",
            },
            Reader(),
            writer,
            mgr,
        )
    )
    head, body = bytes(writer.payload).split(b"\r\n\r\n", 1)
    assert b"200 OK" in head
    assert json.loads(body)["max_bytes"] == 2000
    assert lease.max_bytes == 2000


def test_fbref_proxy_hard_stops_an_oversized_browser_phase_transfer(mod):
    mgr = _FakeManager(["http://u:p@pool.invalid:10000"])
    lease = _make_fbref_lease(mod, mgr, max_bytes=12)

    class Reader:
        def __init__(self):
            self.payload = b"x" * 20
            self.read_sizes = []

        async def read(self, size):
            self.read_sizes.append(size)
            chunk, self.payload = self.payload[:size], self.payload[size:]
            return chunk

    class Writer:
        def __init__(self):
            self.writes = []
            self.closed = False

        def write(self, chunk):
            self.writes.append(chunk)

        async def drain(self):
            return None

        def close(self):
            self.closed = True

    reader = Reader()
    writer = Writer()
    asyncio.run(
        mod._pump(
            reader,
            writer,
            "www.fbref.com",
            defaultdict(int),
            lease=lease,
            direction="down",
        )
    )

    assert reader.read_sizes == [6, 3, 2, 1]
    assert reader.payload == b"x" * 8
    assert b"".join(writer.writes) == b"x" * 12
    assert lease.total_bytes == lease.max_bytes == 12
    assert lease.budget_exceeded is True
    assert writer.closed is True
