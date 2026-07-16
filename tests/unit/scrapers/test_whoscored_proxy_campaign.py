from __future__ import annotations

import hashlib
import json
import stat
from datetime import datetime, timedelta, timezone

import pytest

import scrapers.whoscored.proxy_campaign as proxy_campaign_module
from scrapers.whoscored.proxy_campaign import (
    DEFAULT_WHOSCORED_PAID_CAP_BYTES,
    PROXY_CAMPAIGN_METER,
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
    WHOSCORED_PAID_DAG_IDS,
    WHOSCORED_PAID_APPLICATION_GATEWAY_AVAILABLE,
    WHOSCORED_PROVIDER_INVOICE_HARD_CAP_AVAILABLE,
    WHOSCORED_PROXY_ALLOWED_HOSTS,
    ProxyCampaignAccountingError,
    ProxyCampaignApproval,
    ProxyCampaignBudgetExceeded,
    ProxyCampaignConcurrencyLimited,
    ProxyCampaignExpired,
    ProxyCampaignLedger,
    ProxyCampaignRevoked,
    ProxyCampaignSignatureError,
    ProxyCampaignValidationError,
    ProxyWorkAllocation,
    approval_from_context,
    assert_paid_runtime_authority_available,
    assert_paid_runtime_available,
    canonical_json_bytes,
    deterministic_proxy_attempt_id,
    load_proxy_campaign_approval,
    load_proxy_campaign_context,
    load_proxy_campaign_context_from_env,
    path_matches_family,
    proxy_campaign_ledger_from_env,
    sign_proxy_campaign_approval,
    strict_json_loads,
    whoscored_canary_run_id,
)


SECRET = "test-campaign-secret-which-is-long-enough"
NOW = datetime(2026, 7, 15, 12, tzinfo=timezone.utc)
PATHS = ["/Matches", "/api"]


def _allocation(
    allocation_id: str,
    *,
    phase: str,
    budget: int,
    task_id: str | None = None,
    requests: int = 3,
    leases: int = 3,
) -> dict[str, object]:
    return {
        "allocation_id": allocation_id,
        "phase": phase,
        "workload_class": f"{phase}_class",
        "work_item_id": f"item-{allocation_id}",
        "task_id": task_id or f"task-{allocation_id}",
        "budget_bytes": budget,
        "request_limit": requests,
        "lease_limit": leases,
        "allowed_path_families": PATHS,
    }


def _unsigned(
    *,
    approval_id: str = "approval-1",
    campaign_id: str = "campaign-1",
    run_id: str | None = None,
    discovery: int = 40,
    capture: int = 60,
    daily: int = 100,
    allocations: list[dict[str, object]] | None = None,
    requests: int = 6,
    leases: int = 6,
    concurrency: int = 2,
    dags: list[str] | None = None,
    paths: list[str] | None = None,
    expires: datetime | None = None,
) -> dict[str, object]:
    allowed_dags = dags or ["dag_backfill_whoscored"]
    intended_run_id = run_id or (
        whoscored_canary_run_id(campaign_id)
        if allowed_dags == [WHOSCORED_CANARY_DAG_ID]
        else "run-1"
    )
    if allocations is None:
        allocations = [
            _allocation("allocation-capture", phase="capture", budget=capture),
            _allocation("allocation-discovery", phase="discovery", budget=discovery),
        ]
    return {
        "schema_version": 2,
        "source": "whoscored",
        "approval_id": approval_id,
        "campaign_id": campaign_id,
        "run_id": intended_run_id,
        "issued_at": (NOW - timedelta(hours=1)).isoformat(),
        "expires_at": (expires or NOW + timedelta(hours=23)).isoformat(),
        "transport_policy": "direct_then_paid",
        "runtime_sha256": "a" * 64,
        "classifier_sha256": "b" * 64,
        "caps": {
            "total_provider_bytes": discovery + capture,
            "discovery_provider_bytes": discovery,
            "capture_provider_bytes": capture,
            "daily_provider_bytes": daily,
        },
        "limits": {
            "requests": requests,
            "leases": leases,
            "concurrency": concurrency,
        },
        "allowed_dag_ids": allowed_dags,
        "allowed_hosts": sorted(WHOSCORED_PROXY_ALLOWED_HOSTS),
        "allowed_path_families": paths or PATHS,
        "allocations": allocations,
        "meter": PROXY_CAMPAIGN_METER,
        "signature_algorithm": "hmac-sha256",
    }


def _approval(**kwargs) -> ProxyCampaignApproval:
    signed = sign_proxy_campaign_approval(_unsigned(**kwargs), SECRET)
    result = ProxyCampaignApproval.from_dict(signed)
    result.verify(SECRET, now=NOW)
    return result


def _runtime_context(
    approval: ProxyCampaignApproval,
    allocation_id: str = "allocation-capture",
    *,
    map_index: int = -1,
    try_number: int = 1,
) -> dict[str, object]:
    allocation = approval.allocation(allocation_id)
    dag_id = approval.allowed_dag_ids[0]
    attempt_id = deterministic_proxy_attempt_id(
        dag_id=dag_id,
        run_id=approval.run_id,
        task_id=allocation.task_id,
        map_index=map_index,
        try_number=try_number,
    )
    return {
        "transport_policy": "direct_then_paid",
        "dag_id": dag_id,
        "run_id": approval.run_id,
        "task_id": allocation.task_id,
        "map_index": map_index,
        "try_number": try_number,
        "proxy_campaign_approval": approval.to_dict(),
        "proxy_campaign_id": approval.campaign_id,
        "proxy_approval_id": approval.approval_id,
        "proxy_approval_sha256": approval.approval_sha256,
        "proxy_allocation": allocation.to_dict(),
        "proxy_allocation_id": allocation.allocation_id,
        "proxy_work_item_id": allocation.work_item_id,
        "proxy_attempt_id": attempt_id,
    }


def _claim(
    ledger: ProxyCampaignLedger,
    approval: ProxyCampaignApproval,
    allocation_id: str,
    *,
    lease_id: str,
    attempt_id: str,
    now: datetime = NOW,
    run_id: str = "run-1",
    expires_at: datetime | None = None,
):
    allocation = approval.allocation(allocation_id)
    return ledger.claim(
        approval,
        allocation_id,
        dag_id="dag_backfill_whoscored",
        run_id=run_id,
        task_id=allocation.task_id,
        attempt_id=attempt_id,
        lease_id=lease_id,
        expires_at=expires_at or now + timedelta(minutes=5),
        canonical_url="https://www.whoscored.com/api/work-item",
        now=now,
    )


def test_canonical_hmac_round_trip_and_context_mirroring():
    approval = _approval()
    allocation = approval.allocation("allocation-capture")
    attempt_id = deterministic_proxy_attempt_id(
        dag_id="dag_backfill_whoscored",
        run_id=approval.run_id,
        task_id=allocation.task_id,
        map_index=-1,
        try_number=1,
    )
    assert canonical_json_bytes(approval.unsigned_dict()) == canonical_json_bytes(
        json.loads(canonical_json_bytes(approval.unsigned_dict()))
    )
    context = {
        "transport_policy": "direct_then_paid",
        "dag_id": "dag_backfill_whoscored",
        "run_id": approval.run_id,
        "task_id": allocation.task_id,
        "map_index": -1,
        "try_number": 1,
        "proxy_campaign_approval": approval.to_dict(),
        "proxy_campaign_id": approval.campaign_id,
        "proxy_approval_id": approval.approval_id,
        "proxy_approval_sha256": approval.approval_sha256,
        "proxy_allocation": allocation.to_dict(),
        "proxy_allocation_id": allocation.allocation_id,
        "proxy_work_item_id": allocation.work_item_id,
        "proxy_attempt_id": attempt_id,
    }
    parsed, parsed_allocation, attempt = approval_from_context(
        context, secret=SECRET, now=NOW
    )
    assert parsed == approval
    assert parsed_allocation == allocation
    assert attempt == attempt_id

    context["proxy_approval_sha256"] = "0" * 64
    with pytest.raises(ProxyCampaignValidationError, match="does not match"):
        approval_from_context(context, secret=SECRET, now=NOW)
    context["proxy_approval_sha256"] = approval.approval_sha256
    context["run_id"] = "another-run"
    with pytest.raises(ProxyCampaignValidationError, match="does not match"):
        approval_from_context(context, secret=SECRET, now=NOW)


def test_airflow_attempt_zero_is_never_valid_paid_authority():
    with pytest.raises(ProxyCampaignValidationError, match=">= 1"):
        deterministic_proxy_attempt_id(
            dag_id="dag_backfill_whoscored",
            run_id="run-1",
            task_id="task-allocation-capture",
            map_index=-1,
            try_number=0,
        )


def test_paid_runtime_authority_authenticates_context_before_release_gates():
    context = _runtime_context(_approval())

    with pytest.raises(
        ProxyCampaignValidationError,
        match="full paid crawl is disabled",
    ):
        assert_paid_runtime_available(context, secret=SECRET, now=NOW)

    forged = dict(context)
    forged["run_id"] = "attacker-selected-run"
    with pytest.raises(ProxyCampaignValidationError, match="does not match"):
        assert_paid_runtime_available(forged, secret=SECRET, now=NOW)


def test_paid_runtime_authority_requires_every_non_canary_release_gate(monkeypatch):
    context = _runtime_context(_approval())
    monkeypatch.setattr(
        proxy_campaign_module,
        "WHOSCORED_PROVIDER_INVOICE_HARD_CAP_AVAILABLE",
        True,
    )
    monkeypatch.setattr(
        proxy_campaign_module,
        "WHOSCORED_PAID_APPLICATION_GATEWAY_AVAILABLE",
        True,
    )

    with pytest.raises(
        ProxyCampaignValidationError,
        match="full paid crawl is disabled",
    ):
        assert_paid_runtime_available(context, secret=SECRET, now=NOW)

    monkeypatch.setattr(
        proxy_campaign_module,
        "WHOSCORED_FULL_PAID_CRAWL_AVAILABLE",
        True,
    )
    approval, allocation, attempt_id = assert_paid_runtime_available(
        context,
        secret=SECRET,
        now=NOW,
    )
    assert approval.approval_id == "approval-1"
    assert allocation.allocation_id == "allocation-capture"
    assert attempt_id == context["proxy_attempt_id"]


def test_runner_structural_gate_never_reads_authority_environment(monkeypatch):
    context = _runtime_context(_approval())
    monkeypatch.setattr(
        proxy_campaign_module,
        "WHOSCORED_PROVIDER_INVOICE_HARD_CAP_AVAILABLE",
        True,
    )
    monkeypatch.setattr(
        proxy_campaign_module,
        "WHOSCORED_PAID_APPLICATION_GATEWAY_AVAILABLE",
        True,
    )
    monkeypatch.setattr(
        proxy_campaign_module,
        "WHOSCORED_FULL_PAID_CRAWL_AVAILABLE",
        True,
    )

    class ForbiddenEnvironment(dict):
        def get(self, *_args, **_kwargs):
            raise AssertionError("runner attempted to read an authority secret")

    approval, allocation, _attempt = assert_paid_runtime_available(
        context,
        environ=ForbiddenEnvironment(),
        now=NOW,
    )
    assert approval.campaign_id == "campaign-1"
    assert allocation.allocation_id == "allocation-capture"

    with pytest.raises(ProxyCampaignSignatureError, match="at least 32 bytes"):
        assert_paid_runtime_authority_available(context, environ={}, now=NOW)


def test_paid_runtime_authority_requires_the_exact_canary_contract(monkeypatch):
    non_exact_canary = _approval(
        campaign_id="non-exact-canary",
        dags=[WHOSCORED_CANARY_DAG_ID],
    )
    context = _runtime_context(non_exact_canary)
    monkeypatch.setattr(
        proxy_campaign_module,
        "WHOSCORED_PROVIDER_INVOICE_HARD_CAP_AVAILABLE",
        True,
    )
    monkeypatch.setattr(
        proxy_campaign_module,
        "WHOSCORED_PAID_APPLICATION_GATEWAY_AVAILABLE",
        True,
    )
    monkeypatch.setattr(
        proxy_campaign_module,
        "WHOSCORED_FULL_PAID_CRAWL_AVAILABLE",
        True,
    )

    with pytest.raises(ProxyCampaignValidationError, match="exact 1 GB contract"):
        assert_paid_runtime_available(context, secret=SECRET, now=NOW)


def test_schema_v1_and_first_claim_for_another_run_fail_closed(tmp_path):
    legacy = _unsigned()
    legacy["schema_version"] = 1
    legacy.pop("run_id")
    with pytest.raises(
        ProxyCampaignValidationError,
        match="missing or unknown fields",
    ):
        ProxyCampaignApproval.from_dict(sign_proxy_campaign_approval(legacy, SECRET))

    approval = _approval(run_id="signed-run")
    ledger_path = tmp_path / "fresh-campaign.json"
    ledger = ProxyCampaignLedger(ledger_path, secret=SECRET)
    with pytest.raises(ProxyCampaignValidationError, match="signed campaign run_id"):
        _claim(
            ledger,
            approval,
            "allocation-capture",
            lease_id="lease-wrong-first-run",
            attempt_id="attempt-wrong-first-run",
            run_id="attacker-selected-run",
        )
    assert not ledger_path.exists()


def test_tamper_wrong_secret_and_expiry_fail_closed():
    signed = sign_proxy_campaign_approval(_unsigned(), SECRET)
    signed["runtime_sha256"] = "c" * 64
    with pytest.raises(ProxyCampaignSignatureError, match="canonical body"):
        ProxyCampaignApproval.from_dict(signed).verify(SECRET, now=NOW)

    approval = _approval()
    with pytest.raises(ProxyCampaignSignatureError, match="HMAC"):
        approval.verify("another-secret-which-is-also-long-enough", now=NOW)
    with pytest.raises(ProxyCampaignExpired, match="not active yet"):
        approval.verify(SECRET, now=NOW - timedelta(hours=2))
    with pytest.raises(ProxyCampaignExpired, match="expired"):
        approval.verify(SECRET, now=NOW + timedelta(days=1))

    overlong = _unsigned(expires=NOW + timedelta(hours=23, seconds=1))
    with pytest.raises(ProxyCampaignValidationError, match="24 hours"):
        sign_proxy_campaign_approval(overlong, SECRET)


def test_approval_loader_rejects_duplicate_root_and_nested_json_keys(tmp_path):
    approval = _approval()
    path = tmp_path / "approval.json"
    canonical = json.dumps(approval.to_dict(), sort_keys=True)
    path.write_text(
        '{"approval_id":"benign-review-value",' + canonical[1:],
        encoding="utf-8",
    )

    with pytest.raises(ProxyCampaignValidationError, match="duplicate JSON key"):
        load_proxy_campaign_approval(
            path,
            secret=SECRET,
            expected_approval_id=approval.approval_id,
            expected_approval_sha256=approval.approval_sha256,
            now=NOW,
        )
    with pytest.raises(ProxyCampaignValidationError, match="duplicate JSON key"):
        strict_json_loads('{"caps":{"total":1,"total":2}}')


def test_campaign_ledger_rejects_duplicate_keys_before_hmac_collapse(tmp_path):
    approval = _approval()
    path = tmp_path / "campaign.json"
    ledger = ProxyCampaignLedger(path, secret=SECRET)
    _claim(
        ledger,
        approval,
        "allocation-capture",
        lease_id="lease-duplicate-ledger",
        attempt_id="attempt-duplicate-ledger",
    )
    canonical = path.read_text(encoding="utf-8")
    path.write_text('{"schema_version":1,' + canonical.lstrip()[1:], encoding="utf-8")

    with pytest.raises(ProxyCampaignAccountingError, match="corrupt"):
        ledger.snapshot(approval, now=NOW)


def test_campaign_ledger_lock_rejects_symlink_without_touching_target(tmp_path):
    approval = _approval()
    path = tmp_path / "campaign.json"
    lock_path = path.with_suffix(path.suffix + ".lock")
    target = tmp_path / "unrelated-secret"
    target.write_text("must-stay-unchanged", encoding="utf-8")
    target.chmod(0o600)
    lock_path.symlink_to(target)

    with pytest.raises(ProxyCampaignAccountingError, match="lock is unavailable"):
        ProxyCampaignLedger(path, secret=SECRET).snapshot(approval, now=NOW)

    assert target.read_text(encoding="utf-8") == "must-stay-unchanged"
    assert stat.S_IMODE(target.stat().st_mode) == 0o600
    assert not path.exists()


def test_campaign_ledger_lock_requires_private_regular_owned_file(tmp_path):
    path = tmp_path / "campaign.json"
    lock_path = path.with_suffix(path.suffix + ".lock")
    lock_path.write_text("", encoding="utf-8")
    lock_path.chmod(0o640)

    with pytest.raises(ProxyCampaignAccountingError, match="lock file is unsafe"):
        ProxyCampaignLedger(path, secret=SECRET).initialize_empty()

    assert not path.exists()


def test_campaign_ledger_creates_private_lock_and_reuses_it(tmp_path):
    path = tmp_path / "campaign.json"
    ledger = ProxyCampaignLedger(path, secret=SECRET)
    ledger.initialize_empty()

    lock_path = path.with_suffix(path.suffix + ".lock")
    assert lock_path.is_file()
    assert stat.S_IMODE(lock_path.stat().st_mode) == 0o600
    assert ledger.verify_integrity()["campaigns"] == {}


def test_default_cap_is_zero_and_canary_supports_exact_decimal_gigabyte():
    assert DEFAULT_WHOSCORED_PAID_CAP_BYTES == 0
    assert WHOSCORED_PROVIDER_INVOICE_HARD_CAP_AVAILABLE is True
    assert WHOSCORED_PAID_APPLICATION_GATEWAY_AVAILABLE is True
    assert {
        "dag_ingest_whoscored",
        "dag_backfill_whoscored",
        "dag_canary_whoscored_proxy",
    } == WHOSCORED_PAID_DAG_IDS
    canary = _approval(
        campaign_id="exact-canary",
        discovery=WHOSCORED_CANARY_DISCOVERY_CAP_BYTES,
        capture=WHOSCORED_CANARY_CAPTURE_CAP_BYTES,
        daily=WHOSCORED_CANARY_CAP_BYTES,
        allocations=[
            {
                "allocation_id": WHOSCORED_CANARY_DISCOVERY_ALLOCATION_ID,
                "phase": "discovery",
                "workload_class": "catalog_discovery",
                "work_item_id": WHOSCORED_CANARY_DISCOVERY_WORK_ITEM_ID,
                "task_id": WHOSCORED_CANARY_TASK_ID,
                "budget_bytes": WHOSCORED_CANARY_DISCOVERY_CAP_BYTES,
                "request_limit": WHOSCORED_CANARY_DISCOVERY_REQUEST_LIMIT,
                "lease_limit": WHOSCORED_CANARY_DISCOVERY_LEASE_LIMIT,
                "allowed_path_families": list(WHOSCORED_CANARY_DISCOVERY_PATH_FAMILIES),
            },
            {
                "allocation_id": WHOSCORED_CANARY_CAPTURE_ALLOCATION_ID,
                "phase": "capture",
                "workload_class": "representative_cohort",
                "work_item_id": WHOSCORED_CANARY_CAPTURE_WORK_ITEM_ID,
                "task_id": WHOSCORED_CANARY_TASK_ID,
                "budget_bytes": WHOSCORED_CANARY_CAPTURE_CAP_BYTES,
                "request_limit": WHOSCORED_CANARY_CAPTURE_REQUEST_LIMIT,
                "lease_limit": WHOSCORED_CANARY_CAPTURE_LEASE_LIMIT,
                "allowed_path_families": list(WHOSCORED_CANARY_ALLOWED_PATH_FAMILIES),
            },
        ],
        requests=(
            WHOSCORED_CANARY_DISCOVERY_REQUEST_LIMIT
            + WHOSCORED_CANARY_CAPTURE_REQUEST_LIMIT
        ),
        leases=(
            WHOSCORED_CANARY_DISCOVERY_LEASE_LIMIT
            + WHOSCORED_CANARY_CAPTURE_LEASE_LIMIT
        ),
        concurrency=1,
        dags=[WHOSCORED_CANARY_DAG_ID],
        paths=list(WHOSCORED_CANARY_ALLOWED_PATH_FAMILIES),
    )
    assert canary.is_exact_canary
    assert canary.caps.total_provider_bytes == 1_000_000_000
    assert whoscored_canary_run_id(canary.campaign_id) == "manual__exact-canary"

    weakened = canary.unsigned_dict()
    weakened["caps"] = {
        **weakened["caps"],
        "daily_provider_bytes": WHOSCORED_CANARY_CAP_BYTES - 1,
    }
    weakened["caps"]["total_provider_bytes"] -= 1
    weakened["caps"]["capture_provider_bytes"] -= 1
    weakened["allocations"][1]["budget_bytes"] -= 1
    assert not ProxyCampaignApproval.from_dict(
        sign_proxy_campaign_approval(weakened, SECRET)
    ).is_exact_canary

    weakened_limits = canary.unsigned_dict()
    weakened_limits["limits"]["requests"] += 1
    weakened_limits["allocations"][1]["request_limit"] += 1
    assert not ProxyCampaignApproval.from_dict(
        sign_proxy_campaign_approval(weakened_limits, SECRET)
    ).is_exact_canary


def test_strict_host_and_path_families_reject_lookalikes():
    approval = _approval()
    allocation_id = "allocation-capture"
    assert approval.allows_url(
        "https://www.whoscored.com/api/matches", allocation_id=allocation_id
    )
    assert not approval.allows_url(
        "https://www.whoscored.com/apix/matches", allocation_id=allocation_id
    )
    assert not approval.allows_url(
        "https://evil.whoscored.com/api/matches", allocation_id=allocation_id
    )
    assert not approval.allows_url(
        "http://www.whoscored.com/api/matches", allocation_id=allocation_id
    )


def test_env_loader_is_direct_only_when_absent_and_fails_on_partial(tmp_path):
    assert load_proxy_campaign_context_from_env(environ={}) == {}
    with pytest.raises(ProxyCampaignValidationError, match="incomplete"):
        load_proxy_campaign_context_from_env(
            environ={"WHOSCORED_PROXY_APPROVAL_PATH": "missing.json"},
            secret=SECRET,
        )

    approval = _approval()
    path = tmp_path / "approval.json"
    path.write_text(json.dumps(approval.to_dict()))
    env = {
        "WHOSCORED_PROXY_APPROVAL_PATH": str(path),
        "WHOSCORED_PROXY_APPROVAL_ID": approval.approval_id,
        "WHOSCORED_PROXY_APPROVAL_SHA256": approval.approval_sha256,
        "WHOSCORED_PROXY_ALLOCATION_ID": "allocation-capture",
        "WHOSCORED_PROXY_ATTEMPT_ID": "attempt-env-1",
        "AIRFLOW_CTX_DAG_RUN_ID": approval.run_id,
    }
    payload = load_proxy_campaign_context_from_env(environ=env, secret=SECRET, now=NOW)
    assert payload["transport_policy"] == "direct_then_paid"
    assert payload["proxy_campaign_id"] == approval.campaign_id
    assert payload["proxy_allocation_id"] == "allocation-capture"


def test_task_work_item_resolver_selects_exactly_one_signed_allocation(tmp_path):
    approval = _approval()
    path = tmp_path / "approval.json"
    path.write_text(json.dumps(approval.to_dict()))
    attempt = deterministic_proxy_attempt_id(
        dag_id="dag_backfill_whoscored",
        run_id="scheduled__2026-07-15T00:00:00+00:00",
        task_id="task-allocation-capture",
        map_index=4,
        try_number=2,
    )
    payload = load_proxy_campaign_context(
        path,
        expected_approval_id=approval.approval_id,
        expected_approval_sha256=approval.approval_sha256,
        run_id=approval.run_id,
        task_id="task-allocation-capture",
        work_item_id="item-allocation-capture",
        attempt_id=attempt,
        secret=SECRET,
        now=NOW,
    )
    assert payload["proxy_allocation_id"] == "allocation-capture"
    assert payload["proxy_attempt_id"] == attempt

    with pytest.raises(ProxyCampaignValidationError, match="exactly one"):
        load_proxy_campaign_context(
            path,
            expected_approval_id=approval.approval_id,
            expected_approval_sha256=approval.approval_sha256,
            run_id=approval.run_id,
            task_id="task-allocation-capture",
            work_item_id="missing-item",
            attempt_id=attempt,
            secret=SECRET,
            now=NOW,
        )


def test_retry_and_restart_use_only_original_allocation_remainder(tmp_path):
    approval = _approval()
    path = tmp_path / "campaign.json"
    first_ledger = ProxyCampaignLedger(path, secret=SECRET)
    first = _claim(
        first_ledger,
        approval,
        "allocation-capture",
        lease_id="lease-1",
        attempt_id="attempt-1",
    )
    first_ledger.record_request(approval, first, now=NOW)
    first_ledger.consume(approval, first, 25, now=NOW)
    first_ledger.finish(
        approval,
        first,
        provider_billed_bytes=25,
        completed=False,
        now=NOW,
    )

    restarted = ProxyCampaignLedger(path, secret=SECRET)
    with pytest.raises(ProxyCampaignValidationError, match="signed campaign run_id"):
        _claim(
            restarted,
            approval,
            "allocation-capture",
            lease_id="lease-wrong-run",
            attempt_id="attempt-wrong-run",
            run_id="continuation-2",
        )
    retry = _claim(
        restarted,
        approval,
        "allocation-capture",
        lease_id="lease-2",
        attempt_id="attempt-2",
        run_id="run-1",
    )
    assert retry.remaining_provider_bytes == 35
    restarted.record_request(approval, retry, now=NOW)
    restarted.consume(approval, retry, 35, now=NOW)
    restarted.finish(
        approval,
        retry,
        provider_billed_bytes=35,
        completed=True,
        now=NOW,
    )
    snapshot = restarted.snapshot(approval, now=NOW)
    assert snapshot["spent_provider_bytes"] == 60
    attempts = snapshot["allocations"]["allocation-capture"]["attempts"]
    assert [item["provider_billed_bytes"] for item in attempts] == [25, 35]
    assert [item["provider_requests"] for item in attempts] == [1, 1]
    assert [item["completed"] for item in attempts] == [False, True]


def test_existing_campaign_rejects_approval_top_up_even_before_spend(tmp_path):
    original = _approval()
    topped_up = _approval(
        approval_id="approval-2",
        capture=80,
        daily=120,
        allocations=[
            _allocation("allocation-capture", phase="capture", budget=80),
            _allocation("allocation-discovery", phase="discovery", budget=40),
        ],
    )
    ledger = ProxyCampaignLedger(tmp_path / "campaign.json", secret=SECRET)
    ledger.snapshot(original, now=NOW)

    with pytest.raises(
        ProxyCampaignAccountingError,
        match="approval is immutable; issue a new campaign_id",
    ):
        ledger.snapshot(topped_up, now=NOW)


def test_reconciliation_seal_is_terminal_and_binds_exact_attempts(tmp_path):
    approval = _approval()
    ledger = ProxyCampaignLedger(tmp_path / "campaign.json", secret=SECRET)
    claim = _claim(
        ledger,
        approval,
        "allocation-capture",
        lease_id="lease-before-seal",
        attempt_id="attempt-before-seal",
    )
    ledger.record_request(approval, claim, now=NOW)
    ledger.consume(approval, claim, 17, now=NOW)
    attempt = ledger.finish(
        approval,
        claim,
        provider_billed_bytes=17,
        completed=False,
        now=NOW,
    )
    accounting = [
        {
            "allocation_id": "allocation-capture",
            "attempt_id_hash": attempt["attempt_id_hash"],
            "lease_id_hash": attempt["lease_id_hash"],
            "canonical_url_sha256": attempt["canonical_url_sha256"],
            "provider_billed_bytes": 17,
        }
    ]
    accounting_sha256 = hashlib.sha256(canonical_json_bytes(accounting)).hexdigest()

    sealed = ledger.seal_for_reconciliation(
        approval,
        dag_id="dag_backfill_whoscored",
        run_id="run-1",
        provider_billed_bytes=17,
        attempt_accounting_sha256=accounting_sha256,
    )

    assert sealed["status"] == "sealed"
    assert sealed["reconciliation_seal"]["schema_version"] == 2
    assert sealed["reconciliation_seal"]["attempt_journal_count"] == 1
    assert (
        sealed["reconciliation_seal"]["attempt_journal_tail_sha256"]
        == (sealed["attempt_journal"]["tail_sha256"])
    )
    assert (
        ledger.sealed_snapshot(approval)["reconciliation_seal"][
            "attempt_accounting_sha256"
        ]
        == accounting_sha256
    )
    with pytest.raises(ProxyCampaignAccountingError, match="terminal"):
        _claim(
            ledger,
            approval,
            "allocation-capture",
            lease_id="late-lease",
            attempt_id="late-attempt",
        )


def test_attempt_journal_replays_fsynced_lead_exactly_once_after_crash(
    tmp_path, monkeypatch
):
    approval = _approval()
    path = tmp_path / "campaign.json"
    ledger = ProxyCampaignLedger(path, secret=SECRET)
    claim = _claim(
        ledger,
        approval,
        "allocation-capture",
        lease_id="lease-crash-after-journal-fsync",
        attempt_id="attempt-crash-after-journal-fsync",
    )

    def crash_after_fsync(_record):
        raise RuntimeError("injected crash after journal fsync")

    monkeypatch.setattr(ledger, "_after_attempt_journal_fsync", crash_after_fsync)
    with pytest.raises(RuntimeError, match="injected crash"):
        ledger.finish(
            approval,
            claim,
            provider_billed_bytes=0,
            completed=False,
            now=NOW,
        )

    before = json.loads(path.read_text(encoding="utf-8"))["campaigns"][
        approval.campaign_id
    ]
    assert before["attempt_journal"]["count"] == 0
    assert "lease-crash-after-journal-fsync" in before["active_claims"]
    journal_path = ledger.attempt_journal_path(approval.campaign_id)
    lead_size = journal_path.stat().st_size
    assert lead_size > before["attempt_journal"]["offset"]

    restarted = ProxyCampaignLedger(path, secret=SECRET)
    recovered = restarted.snapshot(approval, now=NOW)
    assert recovered["attempt_journal"]["count"] == 1
    assert recovered["attempt_journal"]["offset"] == lead_size
    assert recovered["active_claims"] == {}
    assert len(recovered["allocations"]["allocation-capture"]["attempts"]) == 1

    repeated = restarted.snapshot(approval, now=NOW)
    assert repeated["attempt_journal"] == recovered["attempt_journal"]
    assert journal_path.stat().st_size == lead_size
    assert len(repeated["allocations"]["allocation-capture"]["attempts"]) == 1


def test_first_attempt_fsyncs_preexisting_journal_dentry_before_record(
    tmp_path, monkeypatch
):
    approval = _approval()
    ledger = ProxyCampaignLedger(tmp_path / "campaign.json", secret=SECRET)
    claim = _claim(
        ledger,
        approval,
        "allocation-capture",
        lease_id="lease-first-dentry",
        attempt_id="attempt-first-dentry",
    )
    journal_path = ledger.attempt_journal_path(approval.campaign_id)
    journal_path.touch(mode=0o600)
    journal_path.chmod(0o600)
    observed = []
    original_fsync = proxy_campaign_module.os.fsync

    def recording_fsync(descriptor):
        mode = proxy_campaign_module.os.fstat(descriptor).st_mode
        observed.append("directory" if stat.S_ISDIR(mode) else "file")
        original_fsync(descriptor)

    monkeypatch.setattr(proxy_campaign_module.os, "fsync", recording_fsync)

    ledger.finish(
        approval,
        claim,
        provider_billed_bytes=0,
        completed=False,
        now=NOW,
    )

    assert observed[:2] == ["directory", "file"]
    snapshot = ledger.snapshot(approval, now=NOW)
    assert snapshot["attempt_journal"]["count"] == 1


def _journal_with_three_attempts(tmp_path):
    approval = _approval(
        allocations=[
            _allocation(
                "allocation-capture",
                phase="capture",
                budget=60,
                requests=4,
                leases=4,
            ),
            _allocation(
                "allocation-discovery",
                phase="discovery",
                budget=40,
                requests=2,
                leases=2,
            ),
        ],
        requests=6,
        leases=6,
    )
    path = tmp_path / "campaign.json"
    ledger = ProxyCampaignLedger(path, secret=SECRET)
    for index in range(3):
        claim = _claim(
            ledger,
            approval,
            "allocation-capture",
            lease_id=f"lease-journal-{index}",
            attempt_id=f"attempt-journal-{index}",
        )
        ledger.finish(
            approval,
            claim,
            provider_billed_bytes=0,
            completed=False,
            now=NOW,
        )
    return approval, path, ledger, ledger.attempt_journal_path(approval.campaign_id)


@pytest.mark.parametrize("mutation", ["tamper", "truncate", "reorder", "duplicate"])
def test_attempt_journal_rejects_tamper_truncate_reorder_and_duplicate(
    tmp_path, mutation
):
    approval, _path, ledger, journal_path = _journal_with_three_attempts(tmp_path)
    records = journal_path.read_bytes().splitlines(keepends=True)
    if mutation == "tamper":
        records[0] = records[0].replace(
            b'"provider_requests":0', b'"provider_requests":1'
        )
        journal_path.write_bytes(b"".join(records))
    elif mutation == "truncate":
        journal_path.write_bytes(b"".join(records)[:-1])
    elif mutation == "reorder":
        journal_path.write_bytes(records[1] + records[0] + records[2])
    else:
        journal_path.write_bytes(b"".join(records) + records[-1])

    with pytest.raises(ProxyCampaignAccountingError, match="journal"):
        ledger.snapshot(approval, now=NOW)


def test_attempt_journal_rejects_authenticated_state_ahead_of_archive(tmp_path):
    approval, path, ledger, journal_path = _journal_with_three_attempts(tmp_path)
    envelope = json.loads(path.read_text(encoding="utf-8"))
    body = {
        "schema_version": envelope["schema_version"],
        "campaigns": envelope["campaigns"],
    }
    state = body["campaigns"][approval.campaign_id]["attempt_journal"]
    state["count"] += 1
    state["offset"] = journal_path.stat().st_size + 1
    ledger._write(body)

    with pytest.raises(ProxyCampaignAccountingError, match="shorter than signed"):
        ledger.snapshot(approval, now=NOW)


def test_partial_uncommitted_journal_suffix_is_discarded_from_signed_offset(
    tmp_path,
):
    approval, _path, ledger, journal_path = _journal_with_three_attempts(tmp_path)
    committed_size = journal_path.stat().st_size
    with journal_path.open("ab") as stream:
        stream.write(b'{"interrupted":')

    snapshot = ledger.snapshot(approval, now=NOW)

    assert journal_path.stat().st_size == committed_size
    assert snapshot["attempt_journal"]["count"] == 3
    assert len(snapshot["allocations"]["allocation-capture"]["attempts"]) == 3


def test_legacy_inline_sealed_evidence_remains_readable(tmp_path):
    approval = _approval()
    path = tmp_path / "campaign.json"
    ledger = ProxyCampaignLedger(path, secret=SECRET)
    claim = _claim(
        ledger,
        approval,
        "allocation-capture",
        lease_id="lease-legacy-sealed",
        attempt_id="attempt-legacy-sealed",
    )
    attempt = ledger.finish(
        approval,
        claim,
        provider_billed_bytes=0,
        completed=False,
        now=NOW,
    )
    accounting = [
        {
            "allocation_id": "allocation-capture",
            "attempt_id_hash": attempt["attempt_id_hash"],
            "lease_id_hash": attempt["lease_id_hash"],
            "canonical_url_sha256": attempt["canonical_url_sha256"],
            "provider_billed_bytes": 0,
        }
    ]
    digest = hashlib.sha256(canonical_json_bytes(accounting)).hexdigest()
    sealed = ledger.seal_for_reconciliation(
        approval,
        dag_id="dag_backfill_whoscored",
        run_id=approval.run_id,
        provider_billed_bytes=0,
        attempt_accounting_sha256=digest,
    )
    legacy = json.loads(json.dumps(sealed))
    legacy.pop("attempt_journal")
    legacy["reconciliation_seal"] = {
        key: value
        for key, value in legacy["reconciliation_seal"].items()
        if not key.startswith("attempt_journal_")
    }
    legacy["reconciliation_seal"]["schema_version"] = 1
    ledger._write({"schema_version": 1, "campaigns": {approval.campaign_id: legacy}})

    recovered = ProxyCampaignLedger(path, secret=SECRET).sealed_snapshot(approval)

    assert recovered["reconciliation_seal"]["schema_version"] == 1
    assert recovered["allocations"]["allocation-capture"]["attempts"] == [attempt]


@pytest.mark.parametrize("legacy", [False, True])
def test_sealed_evidence_is_immutable_for_snapshot_revoke_and_mutators(
    tmp_path, legacy
):
    approval = _approval()
    path = tmp_path / "campaign.json"
    ledger = ProxyCampaignLedger(path, secret=SECRET)
    claim = _claim(
        ledger,
        approval,
        "allocation-capture",
        lease_id="lease-immutable-seal",
        attempt_id="attempt-immutable-seal",
    )
    attempt = ledger.finish(
        approval,
        claim,
        provider_billed_bytes=0,
        completed=False,
        now=NOW,
    )
    accounting = [
        {
            "allocation_id": "allocation-capture",
            "attempt_id_hash": attempt["attempt_id_hash"],
            "lease_id_hash": attempt["lease_id_hash"],
            "canonical_url_sha256": attempt["canonical_url_sha256"],
            "provider_billed_bytes": 0,
        }
    ]
    accounting_sha256 = hashlib.sha256(canonical_json_bytes(accounting)).hexdigest()
    sealed = ledger.seal_for_reconciliation(
        approval,
        dag_id="dag_backfill_whoscored",
        run_id=approval.run_id,
        provider_billed_bytes=0,
        attempt_accounting_sha256=accounting_sha256,
    )
    if legacy:
        persisted = json.loads(json.dumps(sealed))
        persisted.pop("attempt_journal")
        persisted["reconciliation_seal"] = {
            key: value
            for key, value in persisted["reconciliation_seal"].items()
            if not key.startswith("attempt_journal_")
        }
        persisted["reconciliation_seal"]["schema_version"] = 1
        ledger._write(
            {"schema_version": 1, "campaigns": {approval.campaign_id: persisted}}
        )

    journal_path = ledger.attempt_journal_path(approval.campaign_id)
    before_main = hashlib.sha256(path.read_bytes()).hexdigest()
    before_journal = hashlib.sha256(journal_path.read_bytes()).hexdigest()
    with pytest.raises(ProxyCampaignAccountingError, match="immutable"):
        ledger.snapshot(approval, now=NOW)
    with pytest.raises(ProxyCampaignAccountingError, match="immutable"):
        ledger.revoke(approval.campaign_id, reason="must not destroy seal")
    with pytest.raises(ProxyCampaignAccountingError, match="immutable"):
        _claim(
            ledger,
            approval,
            "allocation-capture",
            lease_id="lease-after-immutable-seal",
            attempt_id="attempt-after-immutable-seal",
        )
    with pytest.raises(ProxyCampaignAccountingError, match="immutable"):
        ledger.complete_allocation(
            approval,
            "allocation-capture",
            dag_id="dag_backfill_whoscored",
            run_id=approval.run_id,
            task_id=approval.allocation("allocation-capture").task_id,
            attempt_id="attempt-immutable-seal",
            report_sha256="c" * 64,
            request_ledger_sha256="d" * 64,
            now=NOW,
        )
    if not legacy:
        assert (
            ledger.seal_for_reconciliation(
                approval,
                dag_id="dag_backfill_whoscored",
                run_id=approval.run_id,
                provider_billed_bytes=0,
                attempt_accounting_sha256=accounting_sha256,
            )["reconciliation_seal"]
            == sealed["reconciliation_seal"]
        )

    assert hashlib.sha256(path.read_bytes()).hexdigest() == before_main
    assert hashlib.sha256(journal_path.read_bytes()).hexdigest() == before_journal
    assert ledger.sealed_snapshot(approval)["reconciliation_seal"][
        "schema_version"
    ] == (1 if legacy else 2)


def test_7500_terminal_attempts_keep_main_state_bounded_and_writes_linear(
    tmp_path, monkeypatch
):
    attempt_count = 7_500
    approval = _approval(
        allocations=[
            _allocation(
                "allocation-capture",
                phase="capture",
                budget=60,
                requests=attempt_count,
                leases=attempt_count,
            ),
            _allocation(
                "allocation-discovery",
                phase="discovery",
                budget=40,
                requests=1,
                leases=1,
            ),
        ],
        requests=attempt_count + 1,
        leases=attempt_count + 1,
        concurrency=1,
    )
    path = tmp_path / "campaign.json"
    ledger = ProxyCampaignLedger(path, secret=SECRET)
    original_write = ledger._write
    main_bytes_written = 0

    def counted_write(body):
        nonlocal main_bytes_written
        main_bytes_written += len(canonical_json_bytes(ledger._seal(body))) + 1
        original_write(body)

    monkeypatch.setattr(proxy_campaign_module.os, "fsync", lambda _fd: None)
    monkeypatch.setattr(ledger, "_write", counted_write)
    first_half_bytes = 0
    journal_path = ledger.attempt_journal_path(approval.campaign_id)
    for index in range(attempt_count):
        claim = _claim(
            ledger,
            approval,
            "allocation-capture",
            lease_id=f"lease-scale-{index:04d}",
            attempt_id=f"attempt-scale-{index:04d}",
        )
        ledger.finish(
            approval,
            claim,
            provider_billed_bytes=0,
            completed=False,
            now=NOW,
        )
        if index + 1 == attempt_count // 2:
            first_half_bytes = main_bytes_written + journal_path.stat().st_size

    total_bytes = main_bytes_written + journal_path.stat().st_size
    second_half_bytes = total_bytes - first_half_bytes
    envelope = json.loads(path.read_text(encoding="utf-8"))
    persisted = envelope["campaigns"][approval.campaign_id]

    assert path.stat().st_size < 20_000
    assert persisted["allocations"]["allocation-capture"]["attempts"] == []
    assert persisted["attempt_journal"]["count"] == attempt_count
    assert persisted["attempt_journal"]["offset"] == journal_path.stat().st_size
    assert second_half_bytes < first_half_bytes * 1.20
    assert total_bytes < attempt_count * 30_000

    snapshot = ledger.snapshot(approval, now=NOW)
    assert len(snapshot["allocations"]["allocation-capture"]["attempts"]) == (
        attempt_count
    )


def test_reconciliation_attempt_digest_is_order_independent():
    attempts = [
        {
            "attempt_id_hash": "b" * 64,
            "lease_id_hash": "d" * 64,
            "canonical_url_sha256": "f" * 64,
            "provider_billed_bytes": 2,
        },
        {
            "attempt_id_hash": "a" * 64,
            "lease_id_hash": "c" * 64,
            "canonical_url_sha256": "e" * 64,
            "provider_billed_bytes": 1,
        },
    ]
    forward = {"allocations": {"allocation-capture": {"attempts": attempts}}}
    reverse = {
        "allocations": {"allocation-capture": {"attempts": list(reversed(attempts))}}
    }

    assert ProxyCampaignLedger._attempt_accounting_sha256(
        forward
    ) == ProxyCampaignLedger._attempt_accounting_sha256(reverse)


def test_retry_spend_and_success_are_sealed_from_out_of_order_attempts(tmp_path):
    approval = _approval()
    allocation = approval.allocation("allocation-capture")
    ledger = ProxyCampaignLedger(tmp_path / "campaign.json", secret=SECRET)
    attempts = []
    for try_number, billed, completed in ((1, 25, False), (2, 35, True)):
        attempt_id = deterministic_proxy_attempt_id(
            dag_id="dag_backfill_whoscored",
            run_id=approval.run_id,
            task_id=allocation.task_id,
            map_index=7,
            try_number=try_number,
        )
        claim = _claim(
            ledger,
            approval,
            "allocation-capture",
            lease_id=f"lease-try-{try_number}",
            attempt_id=attempt_id,
        )
        ledger.record_request(approval, claim, now=NOW)
        ledger.consume(approval, claim, billed, now=NOW)
        attempts.append(
            ledger.finish(
                approval,
                claim,
                provider_billed_bytes=billed,
                completed=completed,
                now=NOW,
            )
        )

    out_of_order_accounting = [
        {
            "allocation_id": allocation.allocation_id,
            "attempt_id_hash": attempt["attempt_id_hash"],
            "lease_id_hash": attempt["lease_id_hash"],
            "canonical_url_sha256": attempt["canonical_url_sha256"],
            "provider_billed_bytes": attempt["provider_billed_bytes"],
        }
        for attempt in reversed(attempts)
    ]
    accounting_sha256 = hashlib.sha256(
        canonical_json_bytes(
            sorted(
                out_of_order_accounting,
                key=lambda item: (
                    item["allocation_id"],
                    item["attempt_id_hash"],
                    item["lease_id_hash"],
                ),
            )
        )
    ).hexdigest()
    sealed = ledger.seal_for_reconciliation(
        approval,
        dag_id="dag_backfill_whoscored",
        run_id=approval.run_id,
        provider_billed_bytes=60,
        attempt_accounting_sha256=accounting_sha256,
    )

    assert sealed["status"] == "sealed"
    assert sealed["spent_provider_bytes"] == 60
    assert [
        attempt["provider_billed_bytes"]
        for attempt in sealed["allocations"][allocation.allocation_id]["attempts"]
    ] == [25, 35]
    assert sealed["reconciliation_seal"]["attempt_accounting_sha256"] == (
        accounting_sha256
    )


def test_durable_lease_escrow_is_exactly_settled_on_clean_close(tmp_path):
    approval = _approval()
    ledger = ProxyCampaignLedger(tmp_path / "campaign.json", secret=SECRET)
    claim = _claim(
        ledger,
        approval,
        "allocation-capture",
        lease_id="lease-escrow-clean",
        attempt_id="attempt-escrow-clean",
    )

    assert ledger.reserve_provider_bytes(approval, claim, 50, now=NOW) == 50
    assert ledger.remaining(approval, claim, now=NOW) == 50
    ledger.record_request(approval, claim, now=NOW)
    ledger.consume(approval, claim, 17, now=NOW)
    assert ledger.remaining(approval, claim, now=NOW) == 33
    assert ledger.release_provider_reservation(approval, claim, now=NOW) == 33
    ledger.finish(
        approval,
        claim,
        provider_billed_bytes=17,
        completed=False,
        now=NOW,
    )

    snapshot = ledger.snapshot(approval, now=NOW)
    assert snapshot["spent_provider_bytes"] == 17
    assert snapshot["active_claims"] == {}


def test_expiry_blocks_new_authority_but_settles_inflight_escrow(tmp_path):
    expires_at = NOW + timedelta(minutes=5)
    approval = _approval(expires=expires_at)
    ledger = ProxyCampaignLedger(tmp_path / "campaign.json", secret=SECRET)
    claim = _claim(
        ledger,
        approval,
        "allocation-capture",
        lease_id="lease-expiry-boundary",
        attempt_id="attempt-expiry-boundary",
        expires_at=expires_at,
    )

    assert ledger.reserve_provider_bytes(approval, claim, 50, now=NOW) == 50
    ledger.record_request(approval, claim, now=NOW)

    # The provider read began under live authority and was fully escrowed, but
    # its final bytes arrived at the exact signed boundary. They must remain
    # chargeable; only minting another request/reservation is forbidden.
    with pytest.raises(ProxyCampaignExpired):
        ledger.record_request(approval, claim, now=expires_at)
    with pytest.raises(ProxyCampaignExpired):
        ledger.reserve_provider_bytes(approval, claim, 1, now=expires_at)
    ledger.consume(approval, claim, 17, now=expires_at)
    assert ledger.release_provider_reservation(approval, claim, now=expires_at) == 33
    attempt = ledger.finish(
        approval,
        claim,
        provider_billed_bytes=17,
        completed=False,
        now=expires_at,
    )

    assert attempt["provider_billed_bytes"] == 17
    assert attempt["expired"] is True
    snapshot = ledger.snapshot(
        approval,
        now=expires_at - timedelta(microseconds=1),
    )
    assert snapshot["spent_provider_bytes"] == 17
    assert snapshot["active_claims"] == {}


def test_crash_orphaned_escrow_revokes_instead_of_reopening_budget(tmp_path):
    approval = _approval(concurrency=1)
    path = tmp_path / "campaign.json"
    ledger = ProxyCampaignLedger(path, secret=SECRET)
    claim = _claim(
        ledger,
        approval,
        "allocation-capture",
        lease_id="lease-escrow-crash",
        attempt_id="attempt-escrow-crash",
    )
    ledger.reserve_provider_bytes(approval, claim, 60, now=NOW)
    ledger.record_request(approval, claim, now=NOW)
    ledger.consume(approval, claim, 10, now=NOW)

    restarted = ProxyCampaignLedger(path, secret=SECRET)
    snapshot = restarted.snapshot(approval, now=NOW + timedelta(minutes=6))
    assert snapshot["status"] == "revoked"
    active = snapshot["active_claims"]["lease-escrow-crash"]
    assert active["spent_provider_bytes"] == 10
    assert active["reserved_provider_bytes"] == 50
    attempt = snapshot["allocations"]["allocation-capture"]["attempts"][-1]
    assert attempt["unsettled_provider_reservation_bytes"] == 50

    with pytest.raises(ProxyCampaignRevoked):
        restarted.assert_exact_accounting(
            approval,
            task_report_provider_bytes=10,
            request_ledger_provider_bytes=10,
            proxy_ledger_provider_bytes=10,
            now=NOW + timedelta(minutes=6),
        )

    with pytest.raises(ProxyCampaignRevoked):
        _claim(
            restarted,
            approval,
            "allocation-capture",
            lease_id="lease-after-crash",
            attempt_id="attempt-after-crash",
            now=NOW + timedelta(minutes=6),
        )


def test_provider_order_accounting_aggregates_all_campaigns_across_days(tmp_path):
    path = tmp_path / "campaign.json"
    ledger = ProxyCampaignLedger(path, secret=SECRET)
    first_approval = _approval()
    first = _claim(
        ledger,
        first_approval,
        "allocation-capture",
        lease_id="lease-order-first",
        attempt_id="attempt-order-first",
    )
    ledger.reserve_provider_bytes(first_approval, first, 50, now=NOW)
    ledger.record_request(first_approval, first, now=NOW)
    ledger.consume(first_approval, first, 20, now=NOW)

    second_approval = _approval(
        approval_id="approval-order-second",
        campaign_id="campaign-order-second",
        run_id="run-order-second",
    )
    second = _claim(
        ledger,
        second_approval,
        "allocation-capture",
        lease_id="lease-order-second",
        attempt_id="attempt-order-second",
        run_id="run-order-second",
    )
    with pytest.raises(ProxyCampaignBudgetExceeded, match="provider-order lifetime"):
        ledger.reserve_provider_bytes(
            second_approval,
            second,
            41,
            provider_order_cap_bytes=90,
            global_daily_cap_bytes=90,
            now=NOW,
        )
    ledger.reserve_provider_bytes(
        second_approval,
        second,
        40,
        provider_order_cap_bytes=90,
        global_daily_cap_bytes=90,
        now=NOW,
    )

    restarted = ProxyCampaignLedger(path, secret=SECRET)
    accounting = restarted.provider_order_accounting(now=NOW)
    assert accounting == {
        "spent_provider_bytes": 20,
        "reserved_provider_bytes": 70,
        "exposure_provider_bytes": 90,
        "current_day_spent_provider_bytes": 20,
        "current_day_reserved_provider_bytes": 70,
    }
    next_day = restarted.provider_order_accounting(now=NOW + timedelta(days=1))
    assert next_day["exposure_provider_bytes"] == 90
    assert next_day["current_day_spent_provider_bytes"] == 0
    assert next_day["current_day_reserved_provider_bytes"] == 0


def test_retry_persists_expired_orphan_revocation_before_raising(tmp_path):
    approval = _approval(concurrency=1)
    path = tmp_path / "campaign.json"
    ledger = ProxyCampaignLedger(path, secret=SECRET)
    claim = _claim(
        ledger,
        approval,
        "allocation-capture",
        lease_id="lease-orphan-before-retry",
        attempt_id="attempt-orphan-before-retry",
    )
    ledger.reserve_provider_bytes(approval, claim, 40, now=NOW)
    ledger.record_request(approval, claim, now=NOW)
    ledger.consume(approval, claim, 7, now=NOW)

    retry_time = NOW + timedelta(minutes=6)
    restarted = ProxyCampaignLedger(path, secret=SECRET)
    with pytest.raises(ProxyCampaignRevoked):
        _claim(
            restarted,
            approval,
            "allocation-capture",
            lease_id="lease-denied-after-orphan",
            attempt_id="attempt-denied-after-orphan",
            now=retry_time,
        )

    # Inspect the authenticated on-disk envelope directly: no snapshot call is
    # allowed to become the operation which happens to persist this evidence.
    envelope = json.loads(path.read_text())
    campaign = envelope["campaigns"][approval.campaign_id]
    assert campaign["status"] == "revoked"
    assert campaign["revocation_reason"] == (
        "unsettled provider-byte reservation after lease expiry"
    )
    active = campaign["active_claims"]["lease-orphan-before-retry"]
    assert active["spent_provider_bytes"] == 7
    assert active["reserved_provider_bytes"] == 33
    assert campaign["allocations"]["allocation-capture"]["attempts"] == []
    assert campaign["attempt_journal"]["count"] == 1
    records = [
        json.loads(line)
        for line in restarted.attempt_journal_path(approval.campaign_id)
        .read_text(encoding="utf-8")
        .splitlines()
    ]
    assert records[-1]["attempt"]["expired"] is True
    assert records[-1]["attempt"]["unsettled_provider_reservation_bytes"] == 33


def test_daily_cap_survives_restart_and_resets_only_on_utc_day(tmp_path):
    approval = _approval(daily=30)
    path = tmp_path / "campaign.json"
    ledger = ProxyCampaignLedger(path, secret=SECRET)
    first = _claim(
        ledger,
        approval,
        "allocation-capture",
        lease_id="lease-day-1",
        attempt_id="attempt-day-1",
    )
    ledger.record_request(approval, first, now=NOW)
    ledger.consume(approval, first, 30, now=NOW)
    ledger.finish(
        approval,
        first,
        provider_billed_bytes=30,
        completed=False,
        now=NOW,
    )
    with pytest.raises(ProxyCampaignBudgetExceeded, match="byte cap"):
        _claim(
            ProxyCampaignLedger(path, secret=SECRET),
            approval,
            "allocation-capture",
            lease_id="lease-day-1b",
            attempt_id="attempt-day-1b",
        )

    tomorrow = NOW + timedelta(hours=22)
    retry = _claim(
        ProxyCampaignLedger(path, secret=SECRET),
        approval,
        "allocation-capture",
        lease_id="lease-day-2",
        attempt_id="attempt-day-2",
        now=tomorrow,
    )
    assert retry.remaining_provider_bytes == 30


def test_concurrency_revoke_and_exact_four_ledger_reconciliation(tmp_path):
    approval = _approval(concurrency=1)
    ledger = ProxyCampaignLedger(tmp_path / "campaign.json", secret=SECRET)
    claim = _claim(
        ledger,
        approval,
        "allocation-capture",
        lease_id="lease-active",
        attempt_id="attempt-active",
    )
    with pytest.raises(ProxyCampaignConcurrencyLimited):
        _claim(
            ledger,
            approval,
            "allocation-discovery",
            lease_id="lease-overlap",
            attempt_id="attempt-overlap",
        )
    assert ledger.record_request(approval, claim, now=NOW) == 1
    ledger.consume(approval, claim, 7, now=NOW)
    ledger.finish(
        approval,
        claim,
        provider_billed_bytes=7,
        completed=False,
        now=NOW,
    )
    ledger.complete_allocation(
        approval,
        "allocation-capture",
        dag_id="dag_backfill_whoscored",
        run_id="run-1",
        task_id=approval.allocation("allocation-capture").task_id,
        attempt_id="attempt-active",
        report_sha256="c" * 64,
        request_ledger_sha256="d" * 64,
        now=NOW,
    )
    ledger.complete_allocation(
        approval,
        "allocation-discovery",
        dag_id="dag_backfill_whoscored",
        run_id="run-1",
        task_id=approval.allocation("allocation-discovery").task_id,
        attempt_id="attempt-active",
        report_sha256="e" * 64,
        request_ledger_sha256="f" * 64,
        now=NOW,
    )
    assert (
        ledger.complete_allocation(
            approval,
            "allocation-discovery",
            dag_id="dag_backfill_whoscored",
            run_id="run-1",
            task_id=approval.allocation("allocation-discovery").task_id,
            attempt_id="attempt-active",
            report_sha256="e" * 64,
            request_ledger_sha256="f" * 64,
            now=NOW,
        )["completed"]
        is True
    )
    with pytest.raises(ProxyCampaignAccountingError, match="another execution"):
        ledger.complete_allocation(
            approval,
            "allocation-discovery",
            dag_id="dag_backfill_whoscored",
            run_id="run-1",
            task_id=approval.allocation("allocation-discovery").task_id,
            attempt_id="attempt-active",
            report_sha256="0" * 64,
            request_ledger_sha256="f" * 64,
            now=NOW,
        )
    assert (
        ledger.assert_exact_accounting(
            approval,
            task_report_provider_bytes=7,
            request_ledger_provider_bytes=7,
            proxy_ledger_provider_bytes=7,
            require_complete=True,
            now=NOW,
        )
        == 7
    )
    with pytest.raises(ProxyCampaignAccountingError, match="differ"):
        ledger.assert_exact_accounting(
            approval,
            task_report_provider_bytes=7,
            request_ledger_provider_bytes=6,
            proxy_ledger_provider_bytes=7,
            require_complete=True,
            now=NOW,
        )

    ledger.revoke(approval.campaign_id, reason="operator stop")
    with pytest.raises(ProxyCampaignRevoked):
        ledger.snapshot(approval, now=NOW)


def test_ledger_tampering_is_detected(tmp_path):
    approval = _approval()
    path = tmp_path / "campaign.json"
    ledger = ProxyCampaignLedger(path, secret=SECRET)
    ledger.snapshot(approval, now=NOW)
    payload = json.loads(path.read_text())
    payload["campaigns"][approval.campaign_id]["spent_provider_bytes"] = 1
    path.write_text(json.dumps(payload))
    with pytest.raises(ProxyCampaignAccountingError, match="authentication"):
        ledger.snapshot(approval, now=NOW)


def test_proxy_work_allocation_from_dict_is_strict_and_immutable():
    allocation = ProxyWorkAllocation.from_dict(
        _allocation("allocation-1", phase="capture", budget=10)
    )
    assert ProxyWorkAllocation.from_dict(allocation.to_dict()) == allocation
    malformed = allocation.to_dict()
    malformed["extra"] = True
    with pytest.raises(ProxyCampaignValidationError, match="unknown extra"):
        ProxyWorkAllocation.from_dict(malformed)


def test_root_path_family_is_exact_and_url_authority_is_canonical():
    allocation = ProxyWorkAllocation.from_dict(
        {
            **_allocation("allocation-root", phase="capture", budget=10),
            "allowed_path_families": ["/", "/Matches"],
        }
    )
    hosts = ("www.whoscored.com",)

    assert path_matches_family("/", "/") is True
    assert path_matches_family("/Matches/123/Live", "/Matches") is True
    assert path_matches_family("/Matchesevil", "/Matches") is False
    assert path_matches_family("/anything", "/") is False
    assert allocation.allows_url("https://www.whoscored.com/", allowed_hosts=hosts)
    assert allocation.allows_url(
        "https://www.whoscored.com/Matches/123/Live", allowed_hosts=hosts
    )
    assert not allocation.allows_url(
        "https://www.whoscored.com/Players/123/Show", allowed_hosts=hosts
    )
    assert not allocation.allows_url(
        "https://www.whoscored.com:444/Matches/123/Live", allowed_hosts=hosts
    )
    assert not allocation.allows_url(
        "https://user@www.whoscored.com/Matches/123/Live", allowed_hosts=hosts
    )
    assert not allocation.allows_url(
        "https://www.whoscored.com/Matches/123/Live#fragment", allowed_hosts=hosts
    )


@pytest.mark.parametrize(
    "path",
    [
        "/Matches//Players/123",
        "/Matches/./Players/123",
        "/Matches/../Players/123",
        "/Matches/..\\Players\\123",
        "/Matches/%2e%2e/Players/123",
        "/Matches/%2E%2E%2FPlayers/123",
        "/Matches/%5c..%5cPlayers/123",
        "/Matches/%252e%252e/Players/123",
        "/Matches/%252fPlayers/123",
        "/Matches/%not-an-escape/Players/123",
    ],
)
def test_noncanonical_target_paths_never_match_signed_families(path):
    approval = _approval()
    allocation = approval.allocation("allocation-capture")
    url = f"https://www.whoscored.com{path}"

    assert path_matches_family(path, "/Matches") is False
    assert allocation.allows_url(url, allowed_hosts=("www.whoscored.com",)) is False
    assert approval.allows_url(url, allocation_id=allocation.allocation_id) is False


@pytest.mark.parametrize(
    "family",
    [
        "/Matches//nested",
        "/Matches/./nested",
        "/Matches/../Players",
        "/Matches\\Players",
        "/Matches/%2e%2e",
        "/Matches/%2FPlayers",
        "/Matches/%5cPlayers",
        "/Matches/%252e%252e",
        "/Matches/%broken",
    ],
)
def test_signed_path_families_must_use_unambiguous_canonical_spelling(family):
    raw = _allocation("allocation-path", phase="capture", budget=10)
    raw["allowed_path_families"] = [family]

    with pytest.raises(ProxyCampaignValidationError, match="canonical path family"):
        ProxyWorkAllocation.from_dict(raw)


def test_benign_percent_encoding_does_not_widen_or_disable_a_path_family():
    approval = _approval()
    url = "https://www.whoscored.com/Matches/123/Name%20With%20Spaces"

    assert path_matches_family("/Matches/123/Name%20With%20Spaces", "/Matches")
    assert approval.allows_url(url, allocation_id="allocation-capture")


def test_campaign_ledger_factory_requires_dedicated_whoscored_secret(tmp_path):
    approval = _approval()
    path = tmp_path / "campaign.json"
    ledger = proxy_campaign_ledger_from_env(
        path,
        environ={
            "WHOSCORED_PROXY_APPROVAL_HMAC_SECRET": SECRET,
            "WHOSCORED_PROXY_LEDGER_HMAC_SECRET": "l" * 32,
            "WHOSCORED_PROXY_CONTROL_TOKEN": "must-not-be-authority" * 2,
        },
    )
    assert ledger.snapshot(approval, now=NOW)["campaign_id"] == approval.campaign_id

    with pytest.raises(ProxyCampaignSignatureError, match="at least 32 bytes"):
        proxy_campaign_ledger_from_env(path, environ={})
