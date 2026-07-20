"""Signed transport authority contracts shared by the WhoScored DAGs."""

from __future__ import annotations

import hashlib
import json
from datetime import datetime, timedelta, timezone
from types import SimpleNamespace

import pytest

from dags.scripts import whoscored_proxy_runtime as runtime
from scrapers.whoscored.proxy_campaign import (
    PROXY_CAMPAIGN_METER,
    WHOSCORED_PROXY_ALLOWED_HOSTS,
    sign_proxy_campaign_approval,
)


SECRET = "unit-test-control-secret-which-is-long-enough"


@pytest.fixture(autouse=True)
def _mounted_approval_root(monkeypatch, tmp_path):
    monkeypatch.setenv("WHOSCORED_PROXY_APPROVAL_ROOT", str(tmp_path))


def _context(
    conf=None, *, task_id="ingest_active_scope", dag_id="dag_ingest_whoscored"
):
    return {
        "dag": SimpleNamespace(dag_id=dag_id),
        "dag_run": SimpleNamespace(
            dag_id=dag_id,
            run_id="manual__paid-1",
            conf=dict(conf or {}),
        ),
        "run_id": "manual__paid-1",
        "ti": SimpleNamespace(task_id=task_id, map_index=4, try_number=2),
        "params": {"direct_only": False, "require_zero_paid": False},
    }


def _signed_approval(*, scope_work_item: str, dag_id: str = "dag_ingest_whoscored"):
    now = datetime.now(timezone.utc)
    unsigned = {
        "schema_version": 2,
        "source": "whoscored",
        "approval_id": "approval-daily-1",
        "campaign_id": "campaign-daily-1",
        "run_id": "manual__paid-1",
        "issued_at": (now - timedelta(minutes=5)).isoformat(),
        "expires_at": (now + timedelta(hours=23)).isoformat(),
        "transport_policy": "direct_then_paid",
        "runtime_sha256": "a" * 64,
        "classifier_sha256": "b" * 64,
        "caps": {
            "total_provider_bytes": 100,
            "discovery_provider_bytes": 40,
            "capture_provider_bytes": 60,
            "daily_provider_bytes": 100,
        },
        "limits": {"requests": 4, "leases": 4, "concurrency": 1},
        "allowed_dag_ids": [dag_id],
        "allowed_hosts": sorted(WHOSCORED_PROXY_ALLOWED_HOSTS),
        "allowed_path_families": ["/Matches", "/api"],
        "allocations": [
            {
                "allocation_id": "capture-scope",
                "phase": "capture",
                "workload_class": "daily-scope",
                "work_item_id": scope_work_item,
                "task_id": "ingest_active_scope",
                "budget_bytes": 60,
                "request_limit": 2,
                "lease_limit": 2,
                "allowed_path_families": ["/Matches", "/api"],
            },
            {
                "allocation_id": "discovery-catalog",
                "phase": "discovery",
                "workload_class": "catalog",
                "work_item_id": "catalog-discovery",
                "task_id": "discover_whoscored_catalog",
                "budget_bytes": 40,
                "request_limit": 2,
                "lease_limit": 2,
                "allowed_path_families": ["/Matches", "/api"],
            },
        ],
        "meter": PROXY_CAMPAIGN_METER,
        "signature_algorithm": "hmac-sha256",
    }
    return sign_proxy_campaign_approval(unsigned, SECRET)


def _write_classifier_tree(root):
    payloads = {
        "configs/medallion/competitions.yaml": b"competitions-v1",
        "scrapers/whoscored/catalog.py": b"catalog-v1",
        "scrapers/whoscored/domain.py": b"domain-v1",
    }
    for relative, payload in payloads.items():
        path = root / relative
        path.parent.mkdir(parents=True, exist_ok=True)
        path.write_bytes(payload)
    return root / "configs" / "medallion" / "competitions.yaml"


@pytest.mark.unit
def test_classifier_digest_changes_when_competition_registry_changes(tmp_path):
    registry = _write_classifier_tree(tmp_path)
    original = runtime.classifier_code_sha256(runtime_root=tmp_path)

    registry.write_bytes(b"competitions-v2")

    assert runtime.classifier_code_sha256(runtime_root=tmp_path) != original


@pytest.mark.unit
def test_runtime_classifier_digest_uses_only_startup_attested_hashes(monkeypatch):
    expected_files = {
        relative: hashlib.sha256(relative.encode("utf-8")).hexdigest()
        for relative in runtime.CLASSIFIER_RUNTIME_FILES
    }
    calls = []

    def attested(relative, *, runtime_root):
        calls.append((relative, runtime_root))
        return expected_files[relative]

    monkeypatch.setattr(
        runtime._WHOSCORED_RUNTIME_CONTRACT,
        "attested_runtime_file_sha256",
        attested,
    )
    monkeypatch.setattr(
        runtime.Path,
        "open",
        lambda *_args, **_kwargs: pytest.fail("runtime classifier reopened a path"),
    )

    expected = hashlib.sha256(runtime.canonical_json_bytes(expected_files)).hexdigest()
    assert runtime.classifier_code_sha256() == expected
    assert calls == [
        (relative, runtime.Path(runtime._whoscored_root))
        for relative in runtime.CLASSIFIER_RUNTIME_FILES
    ]


@pytest.mark.unit
def test_policy_defaults_direct_and_legacy_booleans_cannot_enable_paid():
    assert runtime.resolve_transport_policy(_context()) == "direct_only"

    with pytest.raises(
        runtime.WhoScoredProxyRuntimeError,
        match="requires paid_approval_id",
    ):
        runtime.resolve_transport_policy(
            _context({"transport_policy": "direct_then_paid"})
        )

    with pytest.raises(
        runtime.WhoScoredProxyRuntimeError,
        match="pins require",
    ):
        runtime.resolve_transport_policy(
            _context(
                {
                    "transport_policy": "direct_only",
                    "paid_approval_id": "cannot-authorize",
                }
            )
        )


@pytest.mark.unit
def test_paid_approval_must_cover_complete_dagrun_window():
    short = SimpleNamespace(
        expires_at=(datetime.now(timezone.utc) + timedelta(hours=5)).isoformat()
    )

    with pytest.raises(
        runtime.WhoScoredProxyRuntimeError,
        match="complete DagRun timeout window",
    ):
        runtime._verify_approval_window(short, dag_id="dag_ingest_whoscored")


@pytest.mark.unit
def test_signed_paid_runtime_uses_deployment_path_and_exact_allocation(
    monkeypatch, tmp_path
):
    work_item = runtime.stable_scope_work_item("WS-252-2=2526")
    signed = _signed_approval(scope_work_item=work_item)
    approval_path = tmp_path / f"{signed['approval_id']}.json"
    approval_path.write_text(json.dumps(signed), encoding="utf-8")
    approval_path.chmod(0o600)
    monkeypatch.setenv("WHOSCORED_PROXY_CONTROL_TOKEN", SECRET)
    monkeypatch.setenv(runtime.PAID_GATEWAY_URL_ENV, "http://paid-gateway:8898")
    monkeypatch.setenv(runtime.PAID_GATEWAY_TOKEN_ENV, "g" * 32)
    monkeypatch.setenv("WHOSCORED_PAID_PROXY_URL", "http://raw-proxy:8900")
    monkeypatch.setenv("WHOSCORED_PROXY_CONTROL_URL", "http://raw-control:8899")
    monkeypatch.setenv("WHOSCORED_PROXY_APPROVAL_HMAC_SECRET", "a" * 32)
    monkeypatch.setenv("WHOSCORED_PAID_ALERT_HMAC_SECRET", "h" * 32)
    monkeypatch.setattr(runtime, "WHOSCORED_FULL_PAID_CRAWL_AVAILABLE", True)
    monkeypatch.setattr(runtime, "WHOSCORED_PROVIDER_INVOICE_HARD_CAP_AVAILABLE", True)
    monkeypatch.setattr(runtime, "WHOSCORED_PAID_APPLICATION_GATEWAY_AVAILABLE", True)
    monkeypatch.delenv("WHOSCORED_PROXY_APPROVAL_PATH", raising=False)
    monkeypatch.setattr(runtime, "_verify_release_pins", lambda _approval: None)
    conf = {
        "transport_policy": "direct_then_paid",
        "paid_approval_id": signed["approval_id"],
        "paid_approval_sha256": signed["approval_sha256"],
        # This untrusted value must never replace deployment-owned path.
        "proxy_approval_path": "/tmp/attacker-controlled.json",
    }

    base = runtime.resolve_paid_runtime(_context(conf))
    paid = base.for_allocation(
        task_id="ingest_active_scope",
        work_item_id=work_item,
    )
    cli = paid.cli_args(work_item_id=work_item)

    assert paid.is_paid
    assert paid.approval_path == str(approval_path)
    assert "attacker-controlled" not in cli
    assert "--transport-policy direct_then_paid" in cli
    assert f"--proxy-work-item-id {work_item}" in cli
    assert SECRET not in cli

    context = _context(conf)
    with runtime.projected_transport_environment(paid, context):
        assert runtime.os.environ["WHOSCORED_TRANSPORT_POLICY"] == "direct_then_paid"
        assert runtime.os.environ["WHOSCORED_PROXY_ALLOCATION_ID"] == "capture-scope"
        assert runtime.os.environ["WHOSCORED_PROXY_ATTEMPT_ID"].startswith("attempt-")
        assert runtime.os.environ[runtime.PAID_GATEWAY_URL_ENV] == (
            "http://paid-gateway:8898"
        )
        assert runtime.os.environ[runtime.PAID_GATEWAY_TOKEN_ENV] == "g" * 32
        for name in runtime._RUNNER_FORBIDDEN_AUTHORITY_ENV_NAMES:
            assert name not in runtime.os.environ
    assert "WHOSCORED_PROXY_ALLOCATION_ID" not in runtime.os.environ
    assert runtime.os.environ["WHOSCORED_PAID_PROXY_URL"] == "http://raw-proxy:8900"
    assert runtime.os.environ["WHOSCORED_PROXY_CONTROL_URL"] == (
        "http://raw-control:8899"
    )
    assert runtime.os.environ["WHOSCORED_PROXY_CONTROL_TOKEN"] == SECRET
    assert runtime.os.environ["WHOSCORED_PROXY_APPROVAL_HMAC_SECRET"] == "a" * 32
    assert runtime.os.environ["WHOSCORED_PAID_ALERT_HMAC_SECRET"] == "h" * 32


@pytest.mark.unit
def test_paid_approval_swap_after_fstat_cannot_change_fd_pinned_bytes(
    monkeypatch, tmp_path
):
    work_item = runtime.stable_scope_work_item("WS-252-2=2526")
    signed = _signed_approval(scope_work_item=work_item)
    approval_path = tmp_path / f"{signed['approval_id']}.json"
    approval_path.write_text(json.dumps(signed), encoding="utf-8")
    approval_path.chmod(0o600)
    replacement = tmp_path / "replacement.json"
    replacement.write_text("{}", encoding="utf-8")
    replacement.chmod(0o600)
    monkeypatch.setattr(runtime, "_verify_release_pins", lambda _approval: None)

    real_fstat = runtime.os.fstat
    calls = 0

    def swap_after_file_metadata(descriptor):
        nonlocal calls
        metadata = real_fstat(descriptor)
        calls += 1
        if calls == 2:
            runtime.os.replace(replacement, approval_path)
        return metadata

    monkeypatch.setattr(runtime.os, "fstat", swap_after_file_metadata)
    conf = {
        "transport_policy": "direct_then_paid",
        "paid_approval_id": signed["approval_id"],
        "paid_approval_sha256": signed["approval_sha256"],
    }

    resolved = runtime.resolve_paid_runtime(_context(conf))

    assert resolved.approval is not None
    assert resolved.approval.approval_sha256 == signed["approval_sha256"]
    assert json.loads(approval_path.read_text(encoding="utf-8")) == {}


@pytest.mark.unit
def test_direct_projection_removes_and_restores_every_paid_authority(monkeypatch):
    monkeypatch.setenv(runtime.PAID_GATEWAY_URL_ENV, "http://paid-gateway:8898")
    monkeypatch.setenv(runtime.PAID_GATEWAY_TOKEN_ENV, "g" * 32)
    monkeypatch.setenv("WHOSCORED_PAID_PROXY_URL", "http://raw-proxy:8900")
    monkeypatch.setenv("WHOSCORED_PROXY_CONTROL_URL", "http://raw-control:8899")
    monkeypatch.setenv("WHOSCORED_PROXY_CONTROL_TOKEN", SECRET)
    monkeypatch.setenv("WHOSCORED_PROXY_APPROVAL_HMAC_SECRET", "a" * 32)
    monkeypatch.setenv("WHOSCORED_PAID_ALERT_HMAC_SECRET", "h" * 32)
    direct = runtime.PaidRuntime(policy=runtime.TRANSPORT_POLICY_DIRECT_ONLY)

    with runtime.projected_transport_environment(direct, _context()):
        assert runtime.os.environ["WHOSCORED_TRANSPORT_POLICY"] == "direct_only"
        for name in (
            runtime.PAID_GATEWAY_URL_ENV,
            runtime.PAID_GATEWAY_TOKEN_ENV,
            "WHOSCORED_PAID_PROXY_URL",
            "WHOSCORED_PROXY_CONTROL_URL",
            "WHOSCORED_PROXY_CONTROL_TOKEN",
            "WHOSCORED_PROXY_APPROVAL_HMAC_SECRET",
            "WHOSCORED_PAID_ALERT_HMAC_SECRET",
        ):
            assert name not in runtime.os.environ

    assert runtime.os.environ[runtime.PAID_GATEWAY_URL_ENV] == (
        "http://paid-gateway:8898"
    )
    assert runtime.os.environ[runtime.PAID_GATEWAY_TOKEN_ENV] == "g" * 32
    assert runtime.os.environ["WHOSCORED_PAID_PROXY_URL"] == "http://raw-proxy:8900"
    assert runtime.os.environ["WHOSCORED_PROXY_CONTROL_URL"] == (
        "http://raw-control:8899"
    )
    assert runtime.os.environ["WHOSCORED_PROXY_CONTROL_TOKEN"] == SECRET
    assert runtime.os.environ["WHOSCORED_PROXY_APPROVAL_HMAC_SECRET"] == "a" * 32
    assert runtime.os.environ["WHOSCORED_PAID_ALERT_HMAC_SECRET"] == "h" * 32


@pytest.mark.unit
def test_backfill_paid_crawl_gate_is_code_owned(monkeypatch, tmp_path):
    # Backfill stays behind the full-crawl gate; an env var cannot enlarge it.
    work_item = runtime.stable_scope_work_item("WS-252-2=2526")
    signed = _signed_approval(
        scope_work_item=work_item, dag_id="dag_backfill_whoscored"
    )
    approval_path = tmp_path / f"{signed['approval_id']}.json"
    approval_path.write_text(json.dumps(signed), encoding="utf-8")
    approval_path.chmod(0o600)
    monkeypatch.setenv("WHOSCORED_PROXY_CONTROL_TOKEN", SECRET)
    monkeypatch.setenv("WHOSCORED_PROXY_APPROVAL_PATH", str(approval_path))
    monkeypatch.setenv("WHOSCORED_FULL_PAID_CRAWL_AVAILABLE", "true")
    monkeypatch.setattr(runtime, "_verify_release_pins", lambda _approval: None)
    conf = {
        "transport_policy": "direct_then_paid",
        "paid_approval_id": signed["approval_id"],
        "paid_approval_sha256": signed["approval_sha256"],
    }

    with pytest.raises(
        runtime.WhoScoredProxyRuntimeError,
        match="full paid crawl is disabled",
    ):
        runtime.resolve_paid_runtime(_context(conf, dag_id="dag_backfill_whoscored"))


@pytest.mark.unit
def test_daily_ingest_paid_crawl_is_admitted_by_code(monkeypatch, tmp_path):
    # Daily ingest is admitted by its own code-owned gate, without flipping the
    # full-crawl sentinel (which stays False and keeps backfill locked).
    assert runtime.WHOSCORED_FULL_PAID_CRAWL_AVAILABLE is False
    work_item = runtime.stable_scope_work_item("WS-252-2=2526")
    signed = _signed_approval(scope_work_item=work_item)
    approval_path = tmp_path / f"{signed['approval_id']}.json"
    approval_path.write_text(json.dumps(signed), encoding="utf-8")
    approval_path.chmod(0o600)
    monkeypatch.setenv("WHOSCORED_PROXY_CONTROL_TOKEN", SECRET)
    monkeypatch.setenv("WHOSCORED_PROXY_APPROVAL_PATH", str(approval_path))
    monkeypatch.setattr(runtime, "_verify_release_pins", lambda _approval: None)
    conf = {
        "transport_policy": "direct_then_paid",
        "paid_approval_id": signed["approval_id"],
        "paid_approval_sha256": signed["approval_sha256"],
    }

    base = runtime.resolve_paid_runtime(_context(conf))
    paid = base.for_allocation(task_id="ingest_active_scope", work_item_id=work_item)
    assert paid.is_paid


@pytest.mark.unit
def test_runtime_rejects_signed_non_exact_canary(monkeypatch, tmp_path):
    signed = _signed_approval(scope_work_item="scope-canary")
    unsigned = {
        key: value
        for key, value in signed.items()
        if key not in {"approval_sha256", "signature"}
    }
    unsigned["approval_id"] = "approval-non-exact-canary"
    unsigned["campaign_id"] = "non-exact-canary"
    unsigned["run_id"] = "manual__non-exact-canary"
    unsigned["allowed_dag_ids"] = ["dag_canary_whoscored_proxy"]
    signed = sign_proxy_campaign_approval(unsigned, SECRET)
    approval_path = tmp_path / f"{signed['approval_id']}.json"
    approval_path.write_text(json.dumps(signed), encoding="utf-8")
    approval_path.chmod(0o600)
    monkeypatch.setenv("WHOSCORED_PROXY_CONTROL_TOKEN", SECRET)
    monkeypatch.setenv("WHOSCORED_PROXY_APPROVAL_PATH", str(approval_path))
    monkeypatch.setattr(runtime, "_verify_release_pins", lambda _approval: None)
    context = {
        "dag": SimpleNamespace(dag_id="dag_canary_whoscored_proxy"),
        "dag_run": SimpleNamespace(
            dag_id="dag_canary_whoscored_proxy",
            run_id="manual__non-exact-canary",
            conf={
                "transport_policy": "direct_then_paid",
                "paid_approval_id": signed["approval_id"],
                "paid_approval_sha256": signed["approval_sha256"],
            },
        ),
        "run_id": "manual__non-exact-canary",
    }

    with pytest.raises(
        runtime.WhoScoredProxyRuntimeError,
        match="exact 1 GB contract",
    ):
        runtime.resolve_paid_runtime(context)


@pytest.mark.unit
def test_paid_approval_path_rejects_mutable_link_wrong_name_and_mode(tmp_path):
    approval_id = "approval-daily-1"
    target = tmp_path / f"{approval_id}.json"
    target.write_text("{}", encoding="utf-8")
    target.chmod(0o600)
    link = tmp_path / "current.json"
    link.symlink_to(target)

    with pytest.raises(runtime.WhoScoredProxyRuntimeError, match="symlink"):
        runtime._private_approval_path(
            str(link), approval_id=approval_id, raw_root=str(tmp_path)
        )
    wrong = tmp_path / "wrong.json"
    wrong.write_text("{}", encoding="utf-8")
    wrong.chmod(0o600)
    with pytest.raises(runtime.WhoScoredProxyRuntimeError, match="filename"):
        runtime._private_approval_path(
            str(wrong), approval_id=approval_id, raw_root=str(tmp_path)
        )

    target.chmod(0o640)
    with pytest.raises(runtime.WhoScoredProxyRuntimeError, match="0600"):
        runtime._private_approval_path(
            str(target), approval_id=approval_id, raw_root=str(tmp_path)
        )


@pytest.mark.unit
def test_alert_delivery_is_skipped_for_direct_and_real_for_paid(monkeypatch):
    from scrapers.whoscored import transport

    calls = []
    assert runtime.validate_transport_alert_delivery(**_context())["status"] == (
        "not_required"
    )
    assert calls == []

    paid = runtime.PaidRuntime(
        policy="direct_then_paid",
        approval=SimpleNamespace(
            campaign_id="campaign-1",
            approval_id="approval-1",
            approval_sha256="a" * 64,
        ),
    )
    monkeypatch.setattr(runtime, "resolve_paid_runtime", lambda _context: paid)
    monkeypatch.setenv(runtime.PAID_GATEWAY_URL_ENV, runtime.EXPECTED_PAID_GATEWAY_URL)
    monkeypatch.setenv(runtime.PAID_GATEWAY_TOKEN_ENV, "g" * 32)
    monkeypatch.setattr(
        transport.PaidCampaignContext,
        "from_approval",
        classmethod(lambda _cls, approval: ("campaign-context", approval)),
    )

    class Gateway:
        def __init__(self, url, *, token):
            calls.append(("init", url, token))

        def preflight_alert(self, *, context):
            calls.append(("preflight", context))
            return {"status": "delivered"}

        def close(self):
            calls.append(("close",))

    monkeypatch.setattr(transport, "PaidGatewayClient", Gateway)
    result = runtime.validate_transport_alert_delivery(
        **_context(task_id=runtime.PAID_ALERT_PREFLIGHT_TASK_ID)
    )

    assert result["status"] == "delivered"
    assert calls == [
        ("init", runtime.EXPECTED_PAID_GATEWAY_URL, "g" * 32),
        ("preflight", ("campaign-context", paid.approval)),
        ("close",),
    ]


@pytest.mark.unit
def test_campaign_operations_use_only_the_admitted_gateway(monkeypatch):
    from scrapers.whoscored import transport

    approval = object()
    calls = []
    monkeypatch.setenv(runtime.PAID_GATEWAY_URL_ENV, runtime.EXPECTED_PAID_GATEWAY_URL)
    monkeypatch.setenv(runtime.PAID_GATEWAY_TOKEN_ENV, "g" * 32)
    monkeypatch.setattr(
        transport.PaidCampaignContext,
        "from_approval",
        classmethod(lambda _cls, value: ("campaign-context", value)),
    )

    class Gateway:
        def __init__(self, url, *, token):
            calls.append(("init", url, token))

        def sealed_snapshot(self, *, context):
            calls.append(("sealed_snapshot", context))
            return {"status": "sealed"}

        def close(self):
            calls.append(("close",))

    monkeypatch.setattr(transport, "PaidGatewayClient", Gateway)

    assert runtime.paid_campaign_gateway_call(approval, "sealed_snapshot") == {
        "status": "sealed"
    }
    assert calls == [
        ("init", runtime.EXPECTED_PAID_GATEWAY_URL, "g" * 32),
        ("sealed_snapshot", ("campaign-context", approval)),
        ("close",),
    ]
    assert {
        "TELEGRAM_BOT_TOKEN",
        "TELEGRAM_CHAT_ID",
        "PROXY_FILTER_CONTROL_TOKEN",
        "SOFASCORE_PROXY_CONTROL_TOKEN",
        "TM_PROXY_CONTROL_TOKEN",
        "WHOSCORED_PROXY_LEDGER_HMAC_SECRET",
        "WHOSCORED_PAID_ALERT_HMAC_SECRET",
    }.issubset(runtime._RUNNER_FORBIDDEN_AUTHORITY_ENV_NAMES)


@pytest.mark.unit
def test_paid_source_guard_revalidates_receipt_and_emits_in_task_guard(monkeypatch):
    from dags.utils import alerts

    paid = runtime.PaidRuntime(
        policy="direct_then_paid",
        approval=SimpleNamespace(
            campaign_id="campaign-1",
            approval_id="approval-1",
            approval_sha256="a" * 64,
        ),
    )
    metadata = {
        "status": "delivered",
        "campaign_id": "campaign-1",
        "approval_id": "approval-1",
        "approval_sha256": "a" * 64,
        "target_sha256": "b" * 64,
        "telegram_message_id": 7,
        "telegram_message_date": 1_700_000_000,
        "receipt_path": "/immutable/receipt.json",
        "receipt_sha256": "c" * 64,
    }
    calls = []
    monkeypatch.setattr(
        alerts,
        "validate_paid_alert_metadata",
        lambda *args, **kwargs: calls.append((args, kwargs)) or dict(args[0]),
    )

    command = runtime.paid_alert_source_guard_command(
        paid,
        metadata,
        _context(),
    )

    assert calls
    assert calls[0][1]["campaign_id"] == "campaign-1"
    assert "/opt/airflow/dags/utils/alerts.py" not in command
    assert command.startswith("export ")
    assert "export WHOSCORED_PAID_ALERT_APPROVAL_ID=approval-1" in command
    assert "WHOSCORED_PAID_ALERT_RECEIPT_SHA256=" + "c" * 64 in command
    assert "unset " + " ".join(runtime._RUNNER_FORBIDDEN_AUTHORITY_ENV_NAMES) in command
    assert command.endswith(" && ")


# --- Scheduled paid pointer + catalog-drift resilience (#954 automation) ------

_SCHEDULED_RUN_ID = "scheduled__2026-07-19T10:00:00+00:00"


def _scheduled_context(
    *,
    dag_id="dag_ingest_whoscored",
    run_type="scheduled",
    run_id=_SCHEDULED_RUN_ID,
    conf=None,
):
    return {
        "dag": SimpleNamespace(dag_id=dag_id),
        "dag_run": SimpleNamespace(
            dag_id=dag_id, run_id=run_id, run_type=run_type, conf=dict(conf or {})
        ),
        "run_id": run_id,
        "ti": SimpleNamespace(task_id="ingest_active_scope", map_index=0, try_number=1),
        "params": {"direct_only": True, "require_zero_paid": True},
    }


def _write_pointer(
    tmp_path,
    *,
    run_id=_SCHEDULED_RUN_ID,
    dag_id="dag_ingest_whoscored",
    approval_id="wsdaily-approval-x",
    approval_sha256="c" * 64,
    mode=0o600,
    schema_version=1,
):
    root = tmp_path / "pointers"
    root.mkdir(exist_ok=True)
    name = hashlib.sha256(run_id.encode("utf-8")).hexdigest() + ".json"
    path = root / name
    path.write_text(
        json.dumps(
            {
                "schema_version": schema_version,
                "dag_id": dag_id,
                "run_id": run_id,
                "approval_id": approval_id,
                "approval_sha256": approval_sha256,
            }
        )
    )
    path.chmod(mode)
    return root, path


@pytest.mark.unit
def test_scheduled_pointer_injects_signed_pins_for_ingest(monkeypatch, tmp_path):
    root, _ = _write_pointer(tmp_path)
    monkeypatch.setenv(runtime.SCHEDULED_PAID_POINTER_ROOT_ENV, str(root))
    monkeypatch.setenv(
        runtime.SCHEDULED_PAID_MODE_ENV, runtime.SCHEDULED_PAID_MODE_REQUIRED
    )
    conf = runtime._effective_transport_conf(_scheduled_context())
    assert conf["transport_policy"] == runtime.TRANSPORT_POLICY_DIRECT_THEN_PAID
    assert conf[runtime.PAID_APPROVAL_ID_CONF] == "wsdaily-approval-x"
    assert conf[runtime.PAID_APPROVAL_SHA256_CONF] == "c" * 64
    assert runtime.resolve_transport_policy(_scheduled_context()) == (
        runtime.TRANSPORT_POLICY_DIRECT_THEN_PAID
    )


@pytest.mark.unit
def test_manual_run_ignores_scheduled_pointer(monkeypatch, tmp_path):
    root, _ = _write_pointer(tmp_path)
    monkeypatch.setenv(runtime.SCHEDULED_PAID_POINTER_ROOT_ENV, str(root))
    conf = runtime._effective_transport_conf(
        _scheduled_context(run_type="manual", run_id="manual__x")
    )
    assert "transport_policy" not in conf


@pytest.mark.unit
def test_backfill_never_reads_pointer_child_lock(monkeypatch, tmp_path):
    root, _ = _write_pointer(tmp_path, dag_id="dag_backfill_whoscored")
    monkeypatch.setenv(runtime.SCHEDULED_PAID_POINTER_ROOT_ENV, str(root))
    conf = runtime._effective_transport_conf(
        _scheduled_context(dag_id="dag_backfill_whoscored")
    )
    assert "transport_policy" not in conf


@pytest.mark.unit
def test_missing_pointer_stays_direct(monkeypatch, tmp_path):
    (tmp_path / "pointers").mkdir()
    monkeypatch.setenv(
        runtime.SCHEDULED_PAID_POINTER_ROOT_ENV, str(tmp_path / "pointers")
    )
    conf = runtime._effective_transport_conf(_scheduled_context())
    assert "transport_policy" not in conf


@pytest.mark.unit
def test_paid_required_missing_pointer_fails_before_transport(monkeypatch, tmp_path):
    root = tmp_path / "pointers"
    root.mkdir()
    monkeypatch.setenv(runtime.SCHEDULED_PAID_POINTER_ROOT_ENV, str(root))
    monkeypatch.setenv(
        runtime.SCHEDULED_PAID_MODE_ENV, runtime.SCHEDULED_PAID_MODE_REQUIRED
    )

    with pytest.raises(
        runtime.WhoScoredProxyRuntimeError, match="cannot be opened safely"
    ):
        runtime.resolve_transport_policy(_scheduled_context())


@pytest.mark.unit
def test_paid_required_rejects_any_dagrun_conf_before_pointer(monkeypatch, tmp_path):
    root, _ = _write_pointer(tmp_path)
    monkeypatch.setenv(runtime.SCHEDULED_PAID_POINTER_ROOT_ENV, str(root))
    monkeypatch.setenv(
        runtime.SCHEDULED_PAID_MODE_ENV, runtime.SCHEDULED_PAID_MODE_REQUIRED
    )

    with pytest.raises(
        runtime.WhoScoredProxyRuntimeError, match="conf must be exactly empty"
    ):
        runtime.resolve_transport_policy(
            _scheduled_context(conf={"unrelated_selector": "mutable"})
        )


@pytest.mark.unit
def test_manual_default_stays_direct_when_scheduled_paid_is_required(monkeypatch):
    monkeypatch.setenv(
        runtime.SCHEDULED_PAID_MODE_ENV, runtime.SCHEDULED_PAID_MODE_REQUIRED
    )
    assert (
        runtime.resolve_transport_policy(
            _scheduled_context(run_type="manual", run_id="manual__operator")
        )
        == runtime.TRANSPORT_POLICY_DIRECT_ONLY
    )


@pytest.mark.unit
def test_disabled_scheduled_mode_rejects_explicit_paid_conf_without_pointer_read(
    monkeypatch, tmp_path
):
    root, _ = _write_pointer(tmp_path)
    monkeypatch.setenv(runtime.SCHEDULED_PAID_POINTER_ROOT_ENV, str(root))
    monkeypatch.setenv(
        runtime.SCHEDULED_PAID_MODE_ENV, runtime.SCHEDULED_PAID_MODE_DISABLED
    )
    monkeypatch.setattr(
        runtime,
        "_read_private_root_artifact",
        lambda *_args, **_kwargs: pytest.fail("disabled mode read a pointer"),
    )
    context = _scheduled_context(
        conf={
            "transport_policy": runtime.TRANSPORT_POLICY_DIRECT_THEN_PAID,
            runtime.PAID_APPROVAL_ID_CONF: "attacker-selected",
            runtime.PAID_APPROVAL_SHA256_CONF: "a" * 64,
        }
    )

    with pytest.raises(
        runtime.WhoScoredProxyRuntimeError,
        match="paid-disabled mode rejects explicit transport authority",
    ):
        runtime.resolve_transport_policy(context)


@pytest.mark.unit
def test_pointer_run_id_mismatch_raises(monkeypatch, tmp_path):
    # Pointer keyed by this run_id but declaring a different one inside.
    root, _ = _write_pointer(tmp_path, run_id=_SCHEDULED_RUN_ID)
    # Rewrite the body to carry a mismatched run_id.
    name = hashlib.sha256(_SCHEDULED_RUN_ID.encode()).hexdigest() + ".json"
    (root / name).write_text(
        json.dumps(
            {
                "schema_version": 1,
                "dag_id": "dag_ingest_whoscored",
                "run_id": "scheduled__2000-01-01T00:00:00+00:00",
                "approval_id": "x",
                "approval_sha256": "c" * 64,
            }
        )
    )
    (root / name).chmod(0o600)
    monkeypatch.setenv(runtime.SCHEDULED_PAID_POINTER_ROOT_ENV, str(root))
    monkeypatch.setenv(
        runtime.SCHEDULED_PAID_MODE_ENV, runtime.SCHEDULED_PAID_MODE_REQUIRED
    )
    with pytest.raises(runtime.WhoScoredProxyRuntimeError):
        runtime._effective_transport_conf(_scheduled_context())


@pytest.mark.unit
def test_pointer_wrong_mode_raises(monkeypatch, tmp_path):
    root, _ = _write_pointer(tmp_path, mode=0o644)
    monkeypatch.setenv(runtime.SCHEDULED_PAID_POINTER_ROOT_ENV, str(root))
    monkeypatch.setenv(
        runtime.SCHEDULED_PAID_MODE_ENV, runtime.SCHEDULED_PAID_MODE_REQUIRED
    )
    with pytest.raises(runtime.WhoScoredProxyRuntimeError):
        runtime._effective_transport_conf(_scheduled_context())


@pytest.mark.unit
def test_for_allocation_missing_ok_degrades_to_direct():
    from scrapers.whoscored.proxy_campaign import ProxyCampaignApproval

    approval = ProxyCampaignApproval.from_dict(
        _signed_approval(scope_work_item="scope-present")
    )
    base = runtime.PaidRuntime(
        policy=runtime.TRANSPORT_POLICY_DIRECT_THEN_PAID,
        approval_path="/tmp/x.json",
        approval=approval,
    )
    # Unknown scope + missing_ok -> direct-only, never paid.
    degraded = base.for_allocation(
        task_id="ingest_active_scope", work_item_id="scope-unknown", missing_ok=True
    )
    assert not degraded.is_paid
    # Unknown scope without missing_ok -> hard error.
    with pytest.raises(runtime.WhoScoredProxyRuntimeError):
        base.for_allocation(task_id="ingest_active_scope", work_item_id="scope-unknown")
    # Exact match still binds.
    bound = base.for_allocation(
        task_id="ingest_active_scope", work_item_id="scope-present", missing_ok=True
    )
    assert bound.is_paid and bound.allocation is not None


@pytest.mark.unit
def test_stable_profiles_work_item_is_constant():
    assert runtime.stable_profiles_work_item() == "profiles-daily"


@pytest.mark.unit
@pytest.mark.parametrize("pointer_mode", (0o600, 0o644))
def test_disabled_mode_never_reads_even_valid_or_tampered_pointer(
    monkeypatch, tmp_path, pointer_mode
):
    root, _ = _write_pointer(tmp_path, mode=pointer_mode)
    monkeypatch.setenv(runtime.SCHEDULED_PAID_POINTER_ROOT_ENV, str(root))
    monkeypatch.setenv(
        runtime.SCHEDULED_PAID_MODE_ENV, runtime.SCHEDULED_PAID_MODE_DISABLED
    )
    monkeypatch.setattr(
        runtime,
        "_read_private_root_artifact",
        lambda *_args, **_kwargs: pytest.fail("disabled mode read a pointer"),
    )
    conf = runtime._effective_transport_conf(_scheduled_context())
    assert "transport_policy" not in conf
