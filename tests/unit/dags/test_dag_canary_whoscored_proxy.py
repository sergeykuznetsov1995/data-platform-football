from __future__ import annotations

import hashlib
import importlib
import json
import sys
from pathlib import Path
from types import SimpleNamespace

import pytest
import requests


def _load_module():
    sys.modules.pop("dag_canary_whoscored_proxy", None)
    sys.modules.pop("dags.dag_canary_whoscored_proxy", None)
    return importlib.import_module("dag_canary_whoscored_proxy")


@pytest.fixture(autouse=True)
def _clear_python_operators(monkeypatch):
    from airflow.operators.python import PythonOperator

    monkeypatch.setenv(
        "WHOSCORED_PAID_GATEWAY_URL", "http://whoscored_paid_gateway:8898"
    )
    monkeypatch.setenv("WHOSCORED_PAID_GATEWAY_TOKEN", "g" * 32)
    PythonOperator._instances.clear()
    yield


@pytest.mark.unit
def test_canary_requires_the_admitted_gateway_endpoint_and_token():
    mod = _load_module()

    mod._validate_paid_gateway_environment(
        {
            mod.PAID_GATEWAY_URL_ENV: mod.EXPECTED_PAID_GATEWAY_URL,
            mod.PAID_GATEWAY_TOKEN_ENV: "g" * 32,
        }
    )
    with pytest.raises(mod.AirflowException, match="URL differs"):
        mod._validate_paid_gateway_environment(
            {
                mod.PAID_GATEWAY_URL_ENV: "http://attacker:8898",
                mod.PAID_GATEWAY_TOKEN_ENV: "g" * 32,
            }
        )
    with pytest.raises(mod.AirflowException, match="at least 32"):
        mod._validate_paid_gateway_environment(
            {
                mod.PAID_GATEWAY_URL_ENV: mod.EXPECTED_PAID_GATEWAY_URL,
                mod.PAID_GATEWAY_TOKEN_ENV: "short",
            }
        )


def _canary_provider_fixture(mod, event_types):
    allocation = SimpleNamespace(work_item_id="representative-cohort")

    class Approval(SimpleNamespace):
        def allocation(self, allocation_id):
            assert allocation_id == mod.CANARY_ALLOCATION_ID
            return allocation

    approval = Approval(
        campaign_id="campaign-1",
        approval_id="approval-1",
        approval_sha256="a" * 64,
        run_id="manual__campaign-1",
    )
    attempt_id = mod.deterministic_proxy_attempt_id(
        dag_id=mod.DAG_ID,
        run_id=approval.run_id,
        task_id=mod.CANARY_TASK_ID,
        map_index=-1,
        try_number=1,
    )
    common = {
        "event_version": "paid-proxy-v2",
        "proxy_campaign_id": approval.campaign_id,
        "proxy_approval_id": approval.approval_id,
        "proxy_approval_sha256": approval.approval_sha256,
        "provider_meter": mod.PROXY_CAMPAIGN_METER,
        "dag_id": mod.DAG_ID,
        "run_id": approval.run_id,
        "task_id": mod.CANARY_TASK_ID,
        "map_index": -1,
        "try_number": 1,
        "proxy_attempt_id": attempt_id,
        "proxy_work_item_id": allocation.work_item_id,
        "allocation_id": mod.CANARY_ALLOCATION_ID,
        "lease_id": "lease-1",
        "canonical_url": "https://www.whoscored.com/Matches/1/Live",
    }
    events = []
    byte_total = 0
    for index, event_type in enumerate(event_types, start=1):
        event = {
            **common,
            "event_id": f"{index:024d}",
            "event_type": event_type,
        }
        if event_type == "lease_created":
            event["max_bytes"] = 1_000
        elif event_type == "bytes":
            event.update({"direction": "down", "bytes": 100})
            byte_total += 100
        elif event_type == "lease_closed":
            event["total_bytes"] = byte_total
        events.append(event)
    return approval, attempt_id, events


def _write_provider_events(path, events):
    path.write_text(
        "".join(
            json.dumps(event, sort_keys=True, separators=(",", ":")) + "\n"
            for event in events
        ),
        encoding="utf-8",
    )


@pytest.mark.unit
def test_canary_dag_is_manual_paused_serial_and_uses_shared_pool():
    mod = _load_module()
    from scrapers.whoscored.proxy_campaign import WHOSCORED_FULL_PAID_CRAWL_AVAILABLE

    assert mod.dag.dag_id == "dag_canary_whoscored_proxy"
    assert mod.WHOSCORED_PROVIDER_INVOICE_HARD_CAP_AVAILABLE is True
    assert mod.WHOSCORED_PAID_APPLICATION_GATEWAY_AVAILABLE is True
    assert WHOSCORED_FULL_PAID_CRAWL_AVAILABLE is False
    assert mod.dag._dag_kwargs["schedule"] is None
    assert mod.dag._dag_kwargs["is_paused_upon_creation"] is True
    assert mod.dag._dag_kwargs["max_active_runs"] == 1
    assert mod.dag._dag_kwargs["params"]["paid_approval_id"] == ""

    from airflow.operators.python import PythonOperator

    tasks = {item.task_id: item for item in PythonOperator._instances}
    assert set(tasks) == {
        "validate_whoscored_proxy_canary",
        "deliver_whoscored_proxy_canary_alert",
        "run_whoscored_proxy_canary",
        "persist_whoscored_proxy_canary_measurement",
        "final_whoscored_proxy_canary_gate",
    }
    assert tasks["validate_whoscored_proxy_canary"]._init_kwargs["pool"] == (
        "whoscored_direct_pool"
    )
    assert tasks["validate_whoscored_proxy_canary"]._init_kwargs["pool_slots"] == 2
    assert tasks["run_whoscored_proxy_canary"]._init_kwargs["pool_slots"] == 2
    assert (
        tasks["persist_whoscored_proxy_canary_measurement"]._init_kwargs["trigger_rule"]
        == "all_done"
    )
    assert tasks["deliver_whoscored_proxy_canary_alert"].upstream_task_ids == {
        "validate_whoscored_proxy_canary"
    }
    assert tasks["run_whoscored_proxy_canary"].upstream_task_ids == {
        "deliver_whoscored_proxy_canary_alert"
    }


@pytest.mark.unit
def test_canary_alert_binds_exact_approval_and_returns_receipt(monkeypatch):
    mod = _load_module()
    from scrapers.whoscored import transport

    calls = []
    receipt = {
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
    approval = SimpleNamespace(
        campaign_id="campaign-1",
        approval_id="approval-1",
        approval_sha256="a" * 64,
    )
    monkeypatch.setattr(
        mod,
        "_load_canary_approval",
        lambda _context: (approval, Path("/immutable/approval-1.json")),
    )
    monkeypatch.setattr(
        transport.PaidCampaignContext,
        "from_approval",
        classmethod(lambda _cls, value: ("campaign-context", value)),
    )

    class Gateway:
        def preflight_alert(self, *, context):
            calls.append(context)
            return receipt

        def close(self):
            calls.append("closed")

    monkeypatch.setattr(mod, "_paid_gateway_client", Gateway)

    result = mod.deliver_canary_alert(
        {
            "status": "approved",
            "campaign_id": "campaign-1",
            "approval_id": "approval-1",
            "approval_sha256": "a" * 64,
            "approval_path": "/immutable/approval-1.json",
        },
        ti=SimpleNamespace(task_id=mod.CANARY_ALERT_TASK_ID),
        run_id="manual__campaign-1",
    )

    assert result == receipt
    assert calls == [("campaign-context", approval), "closed"]


@pytest.mark.unit
def test_canary_source_rechecks_exact_receipt_before_ledger_or_network(
    monkeypatch, tmp_path
):
    mod = _load_module()
    from dags.utils import alerts

    approval = SimpleNamespace(
        campaign_id="campaign-1",
        approval_id="approval-1",
        approval_sha256="a" * 64,
    )
    monkeypatch.setattr(
        mod,
        "_load_canary_approval",
        lambda _context: (approval, tmp_path / "approval.json"),
    )
    monkeypatch.setattr(mod, "_validate_canary_release_pins", lambda _approval: None)
    monkeypatch.setattr(
        alerts,
        "validate_paid_alert_metadata",
        lambda *_args, **_kwargs: (_ for _ in ()).throw(
            alerts.PaidAlertError("rotated secret")
        ),
    )
    monkeypatch.setattr(
        mod,
        "_paid_campaign_gateway_call",
        lambda *_args, **_kwargs: (_ for _ in ()).throw(
            AssertionError("gateway reached")
        ),
    )

    with pytest.raises(mod.AirflowException, match="exact alert receipt"):
        mod.execute_measurement_canary(
            {
                "status": "approved",
                "campaign_id": "campaign-1",
                "approval_id": "approval-1",
                "approval_sha256": "a" * 64,
            },
            {
                "status": "delivered",
                "campaign_id": "campaign-1",
                "approval_id": "approval-1",
                "approval_sha256": "a" * 64,
            },
            run_id="manual__campaign-1",
        )


@pytest.mark.unit
def test_canary_provider_ledger_requires_one_complete_ordered_lease(tmp_path):
    mod = _load_module()
    approval, attempt_id, events = _canary_provider_fixture(
        mod,
        ("lease_created", "bytes", "lease_closed"),
    )
    path = tmp_path / "provider.jsonl"
    _write_provider_events(path, events)

    totals, by_lease_url = mod._provider_ledger_bytes(
        path,
        approval,
        expected_attempts={mod.CANARY_ALLOCATION_ID: attempt_id},
    )

    assert totals == {mod.CANARY_ALLOCATION_ID: 100}
    assert by_lease_url == {
        mod.CANARY_ALLOCATION_ID: {
            (
                "lease-1",
                "https://www.whoscored.com/Matches/1/Live",
            ): 100
        }
    }


@pytest.mark.unit
@pytest.mark.parametrize(
    ("event_types", "message"),
    (
        (("bytes", "lease_closed"), "missing or out of order"),
        (("lease_created", "bytes"), "incomplete lease lifecycle"),
        (
            ("lease_created", "lease_created", "bytes", "lease_closed"),
            "duplicate/invalid lease creation",
        ),
        (
            ("lease_created", "bytes", "lease_closed", "lease_closed"),
            "missing or out of order",
        ),
        (
            ("lease_closed", "lease_created", "bytes"),
            "missing or out of order",
        ),
    ),
)
def test_canary_provider_ledger_rejects_incomplete_duplicate_or_reordered_lifecycle(
    tmp_path,
    event_types,
    message,
):
    mod = _load_module()
    approval, attempt_id, events = _canary_provider_fixture(mod, event_types)
    path = tmp_path / "provider.jsonl"
    _write_provider_events(path, events)

    with pytest.raises(mod.AirflowException, match=message):
        mod._provider_ledger_bytes(
            path,
            approval,
            expected_attempts={mod.CANARY_ALLOCATION_ID: attempt_id},
        )


@pytest.mark.unit
def test_classifier_hash_binds_code_and_competition_registry(tmp_path):
    mod = _load_module()
    registry = tmp_path / "configs" / "medallion" / "competitions.yaml"
    catalog = tmp_path / "scrapers" / "whoscored" / "catalog.py"
    domain = tmp_path / "scrapers" / "whoscored" / "domain.py"
    registry.parent.mkdir(parents=True)
    catalog.parent.mkdir(parents=True)
    registry.write_bytes(b"competitions-v1")
    catalog.write_bytes(b"catalog-v1")
    domain.write_bytes(b"domain-v1")
    expected_files = {
        "configs/medallion/competitions.yaml": hashlib.sha256(
            b"competitions-v1"
        ).hexdigest(),
        "scrapers/whoscored/catalog.py": hashlib.sha256(b"catalog-v1").hexdigest(),
        "scrapers/whoscored/domain.py": hashlib.sha256(b"domain-v1").hexdigest(),
    }
    expected = hashlib.sha256(
        json.dumps(
            expected_files,
            sort_keys=True,
            separators=(",", ":"),
        ).encode("utf-8")
    ).hexdigest()

    assert mod.classifier_sha256(tmp_path) == expected


@pytest.mark.unit
def test_runtime_classifier_uses_only_startup_attested_hashes(monkeypatch):
    mod = _load_module()
    expected_files = {
        relative: hashlib.sha256(relative.encode("utf-8")).hexdigest()
        for relative in mod.CLASSIFIER_FILES
    }
    calls = []

    def attested(relative, *, runtime_root):
        calls.append((relative, runtime_root))
        return expected_files[relative]

    monkeypatch.setattr(
        mod._WHOSCORED_RUNTIME_CONTRACT,
        "attested_runtime_file_sha256",
        attested,
    )
    monkeypatch.setattr(
        mod.Path,
        "read_bytes",
        lambda _path: pytest.fail("runtime classifier reopened a path"),
    )

    expected = hashlib.sha256(mod.canonical_json_bytes(expected_files)).hexdigest()
    runtime_root = mod.Path(mod._whoscored_root)
    monkeypatch.setenv("WHOSCORED_RUNTIME_ROOT", "/opt/airflow/../airflow")
    assert mod.classifier_sha256() == expected
    assert calls == [(relative, runtime_root) for relative in mod.CLASSIFIER_FILES]


@pytest.mark.unit
def test_budget_model_uses_nearest_rank_p95_and_exact_25_percent_ceiling():
    mod = _load_module()
    observations = {name: [100] * 20 for name in mod.REQUIRED_MODEL_CLASSES}
    observations["stage_feed"] = list(range(1, 21))
    targets = {name: 1 for name in mod.REQUIRED_MODEL_CLASSES}

    result = mod.build_budget_model(observations, targets)

    assert result["status"] == "ready"
    assert result["classes"]["stage_feed"]["p95_provider_billed_bytes"] == 19
    assert result["weighted_p95_provider_bytes"] == 619
    assert result["proposed_full_cap_provider_bytes"] == 774
    assert result["requires_separate_signed_approval"] is True


@pytest.mark.unit
def test_budget_model_refuses_to_price_undersampled_remaining_class():
    mod = _load_module()
    observations = {name: [10] * 20 for name in mod.REQUIRED_MODEL_CLASSES}
    observations["stage_feed"] = [10] * 19
    targets = {name: 1 for name in mod.REQUIRED_MODEL_CLASSES}

    result = mod.build_budget_model(observations, targets)

    assert result["status"] == "insufficient_samples"
    assert result["missing_or_undersampled_classes"] == ["stage_feed"]
    assert result["proposed_full_cap_provider_bytes"] is None


@pytest.mark.unit
def test_budget_model_never_prices_full_population_upper_bounds_as_remaining():
    mod = _load_module()
    observations = {name: [100] * 20 for name in mod.REQUIRED_MODEL_CLASSES}
    targets = {name: 1_000 for name in mod.REQUIRED_MODEL_CLASSES}

    result = mod.build_budget_model(
        observations,
        targets,
        target_basis=mod.FULL_TARGET_UPPER_BOUND_BASIS,
    )

    assert result["status"] == "unverified_remaining_targets"
    assert result["proposed_full_cap_provider_bytes"] is None


@pytest.mark.unit
def test_budget_model_never_prices_a_required_class_subset():
    mod = _load_module()

    result = mod.build_budget_model(
        {"stage_feed": [10] * 20},
        {"stage_feed": 100},
    )

    assert result["status"] == "incomplete_remaining_targets"
    assert result["proposed_full_cap_provider_bytes"] is None
    assert set(result["missing_or_zero_target_classes"]) == (
        set(mod.REQUIRED_MODEL_CLASSES) - {"stage_feed"}
    )


@pytest.mark.unit
def test_representative_cohort_is_bounded_and_covers_stages_matches_players():
    mod = _load_module()
    seasons = [
        SimpleNamespace(
            scope=SimpleNamespace(spec="WS-1=2020"),
            stage_ids=tuple(range(1, 13)),
        ),
        SimpleNamespace(
            scope=SimpleNamespace(spec="WS-2=2021"),
            stage_ids=tuple(range(20, 30)),
        ),
        SimpleNamespace(
            scope=SimpleNamespace(spec="WS-3=2022"),
            stage_ids=(40,),
        ),
    ]
    populations = [
        ("WS-1", "2020", 75, 80),
        ("WS-2", "2021", 50, 60),
        ("WS-3", "2022", 1_000, 3_000),
    ]

    result = mod.select_representative_cohort(
        seasons,
        populations,
        fixed_scopes=("WS-1=2020", "WS-2=2021"),
    )

    assert [item["scope"] for item in result] == ["WS-1=2020", "WS-2=2021"]
    assert sum(item["stage_count"] for item in result) >= 20
    assert sum(item["completed_match_count"] for item in result) >= 100
    assert sum(item["roster_player_count"] for item in result) >= 100
    assert sum(item["estimated_work_items"] for item in result) <= 90


@pytest.mark.unit
def test_final_gate_rejects_zero_spend_and_records_incomplete_model():
    mod = _load_module()
    task_ids = (
        "validate_whoscored_proxy_canary",
        "deliver_whoscored_proxy_canary_alert",
        mod.CANARY_TASK_ID,
        "persist_whoscored_proxy_canary_measurement",
    )
    dag_run = SimpleNamespace(
        get_task_instances=lambda: [
            SimpleNamespace(task_id=task_id, state="success") for task_id in task_ids
        ]
    )

    with pytest.raises(mod.AirflowException, match="byte cap"):
        mod.enforce_canary_gate(
            {"status": "success"},
            {
                "status": "measurement_persisted",
                "provider_billed_bytes": 0,
                "budget_model_status": "ready",
                "proposed_full_cap_provider_bytes": 1,
            },
            dag_run=dag_run,
        )
    result = mod.enforce_canary_gate(
        {"status": "retryable"},
        {
            "status": "measurement_persisted",
            "provider_billed_bytes": 1,
            "budget_model_status": "insufficient_samples",
            "proposed_full_cap_provider_bytes": None,
        },
        dag_run=dag_run,
    )
    assert result["status"] == "measurement_recorded_non_authorizing"
    assert result["full_approval_eligible"] is False
    assert result["requires_followup_measurement"] is True


@pytest.mark.unit
def test_final_gate_rejects_valid_payload_when_an_upstream_failed():
    mod = _load_module()
    states = {
        "validate_whoscored_proxy_canary": "success",
        "deliver_whoscored_proxy_canary_alert": "success",
        mod.CANARY_TASK_ID: "failed",
        "persist_whoscored_proxy_canary_measurement": "success",
    }
    dag_run = SimpleNamespace(
        get_task_instances=lambda: [
            SimpleNamespace(task_id=task_id, state=state)
            for task_id, state in states.items()
        ]
    )

    with pytest.raises(mod.AirflowException, match="unsuccessful upstreams"):
        mod.enforce_canary_gate(
            {"status": "success"},
            {
                "status": "measurement_persisted",
                "provider_billed_bytes": 1,
                "budget_model_status": "ready",
                "proposed_full_cap_provider_bytes": 1,
            },
            dag_run=dag_run,
        )


@pytest.mark.unit
def test_request_measurement_requires_exact_campaign_identity(tmp_path):
    mod = _load_module()
    approval = type(
        "Approval",
        (),
        {
            "campaign_id": "campaign-1",
            "approval_id": "approval-1",
            "approval_sha256": "a" * 64,
        },
    )()
    path = tmp_path / "requests.jsonl"
    event = {
        "event_version": "whoscored-request-v1",
        "event_id": "1" * 32,
        "status": "accounted",
        "lease_id": "lease-1",
        "url": "https://www.whoscored.com/stagestatfeed/12/stageteams/",
        "route": "paid_lease",
        "request_bytes": 10,
        "response_bytes": 90,
        "paid_proxy_bytes": 100,
        "proxy_campaign_id": "campaign-1",
        "proxy_approval_id": "approval-1",
        "proxy_approval_sha256": "a" * 64,
        "proxy_allocation_id": mod.CANARY_ALLOCATION_ID,
        "transport_policy": "direct_then_paid",
    }
    path.write_text(json.dumps(event) + "\n", encoding="utf-8")

    total, traffic, observations, paid_by_lease = mod._request_measurement(
        path, approval
    )

    assert total == 100
    assert observations == {"stage_feed": [100]}
    assert traffic["route_requests"] == {"paid_lease": 1}
    assert traffic["route_wire_bytes"] == {"paid_lease": 100}
    assert paid_by_lease == {
        (
            "lease-1",
            "https://www.whoscored.com/stagestatfeed/12/stageteams/",
        ): 100
    }
    event["proxy_campaign_id"] = "campaign-2"
    path.write_text(json.dumps(event) + "\n", encoding="utf-8")
    with pytest.raises(mod.AirflowException, match="another campaign"):
        mod._request_measurement(path, approval)


@pytest.mark.unit
def test_ledger_reader_rejects_an_oversized_event(tmp_path):
    mod = _load_module()
    path = tmp_path / "oversized.jsonl"
    path.write_bytes(b'{"padding":"' + b"x" * mod.MAX_LEDGER_EVENT_BYTES + b'"}\n')

    with pytest.raises(mod.AirflowException, match="oversized ledger event"):
        list(mod._iter_json_lines(path))


@pytest.mark.unit
def test_canary_reconciliation_rejects_cross_allocation_byte_swaps():
    mod = _load_module()
    discovery_request = {("lease-discovery", "https://www.whoscored.com/Regions"): 40}
    discovery_proxy = {("lease-discovery", "https://www.whoscored.com/Regions"): 60}

    with pytest.raises(mod.AirflowException, match="discovery allocation.*differ"):
        mod._reconcile_canary_allocation(
            phase="discovery",
            task_report_provider_bytes=40,
            request_ledger_provider_bytes=40,
            proxy_ledger_provider_bytes=60,
            campaign_allocation_provider_bytes=60,
            task_report_bytes_by_url={"https://www.whoscored.com/Regions": 40},
            request_bytes_by_lease_url=discovery_request,
            proxy_bytes_by_lease_url=discovery_proxy,
            campaign_attempts=[],
            expected_attempt_id="attempt-discovery",
        )

    with pytest.raises(mod.AirflowException, match="lease and URL bytes differ"):
        mod._reconcile_canary_allocation(
            phase="capture",
            task_report_provider_bytes=100,
            request_ledger_provider_bytes=100,
            proxy_ledger_provider_bytes=100,
            campaign_allocation_provider_bytes=100,
            task_report_bytes_by_url={"https://www.whoscored.com/Players/1": 100},
            request_bytes_by_lease_url={
                ("lease-a", "https://www.whoscored.com/Players/1"): 100
            },
            proxy_bytes_by_lease_url={
                ("lease-b", "https://www.whoscored.com/Players/1"): 100
            },
            campaign_attempts=[],
            expected_attempt_id="attempt-capture",
        )


@pytest.mark.unit
def test_canary_reconciliation_joins_campaign_attempt_to_exact_lease_and_url():
    mod = _load_module()
    attempt_id = "attempt-capture"
    lease_id = "lease-capture"
    url = "https://www.whoscored.com/Players/1"
    paid_map = {(lease_id, url): 100}
    campaign_attempt = {
        "lease_id_hash": hashlib.sha256(lease_id.encode("utf-8")).hexdigest(),
        "attempt_id_hash": hashlib.sha256(attempt_id.encode("utf-8")).hexdigest(),
        "canonical_url_sha256": hashlib.sha256(url.encode("utf-8")).hexdigest(),
        "provider_billed_bytes": 100,
        "provider_requests": 1,
        "completed": False,
        "expired": False,
        "finished_at": "2026-07-15T12:00:00+00:00",
    }

    assert (
        mod._reconcile_canary_allocation(
            phase="capture",
            task_report_provider_bytes=100,
            request_ledger_provider_bytes=100,
            proxy_ledger_provider_bytes=100,
            campaign_allocation_provider_bytes=100,
            task_report_bytes_by_url={url: 100},
            request_bytes_by_lease_url=paid_map,
            proxy_bytes_by_lease_url=paid_map,
            campaign_attempts=[campaign_attempt],
            expected_attempt_id=attempt_id,
        )
        == 100
    )

    campaign_attempt["lease_id_hash"] = hashlib.sha256(b"another-lease").hexdigest()
    with pytest.raises(mod.AirflowException, match="campaign lease attempts differ"):
        mod._reconcile_canary_allocation(
            phase="capture",
            task_report_provider_bytes=100,
            request_ledger_provider_bytes=100,
            proxy_ledger_provider_bytes=100,
            campaign_allocation_provider_bytes=100,
            task_report_bytes_by_url={url: 100},
            request_bytes_by_lease_url=paid_map,
            proxy_bytes_by_lease_url=paid_map,
            campaign_attempts=[campaign_attempt],
            expected_attempt_id=attempt_id,
        )


@pytest.mark.unit
def test_runner_phase_resolves_signed_work_item_and_binds_attempt(
    tmp_path, monkeypatch
):
    mod = _load_module()
    monkeypatch.setenv("WHOSCORED_PAID_PROXY_URL", "http://raw-proxy:8900")
    monkeypatch.setenv("WHOSCORED_PROXY_CONTROL_URL", "http://raw-control:8899")
    monkeypatch.setenv("WHOSCORED_PROXY_CONTROL_TOKEN", "c" * 32)
    monkeypatch.setenv("WHOSCORED_PROXY_APPROVAL_HMAC_SECRET", "a" * 32)
    monkeypatch.setenv("WHOSCORED_PAID_ALERT_HMAC_SECRET", "h" * 32)
    monkeypatch.setattr(mod, "RUN_ROOT", str(tmp_path / "runs"))
    allocation = SimpleNamespace(
        task_id=mod.CANARY_TASK_ID,
        work_item_id=mod.CANARY_DISCOVERY_WORK_ITEM_ID,
    )
    approval = SimpleNamespace(
        approval_id="approval-1",
        approval_sha256="a" * 64,
        allocation=lambda allocation_id: allocation,
    )
    run_id = "manual__campaign-1"
    ti = SimpleNamespace(task_id=mod.CANARY_TASK_ID, try_number=1, map_index=-1)
    observed = {}

    class FakeProcess:
        pid = 12345
        returncode = None

        def __init__(self, command, *, env, start_new_session, **kwargs):
            assert start_new_session is True
            observed["command"] = list(command)
            observed["env"] = dict(env)

        def wait(self, timeout=None):
            assert timeout == mod.CANARY_DISCOVERY_TIMEOUT.total_seconds()
            command = observed["command"]
            env = observed["env"]
            expected_attempt = mod.deterministic_proxy_attempt_id(
                dag_id=mod.DAG_ID,
                run_id=run_id,
                task_id=mod.CANARY_TASK_ID,
                map_index=-1,
                try_number=1,
            )
            report_path = command[command.index("--output") + 1]
            with open(report_path, "w", encoding="utf-8") as stream:
                json.dump(
                    {
                        "status": "success",
                        "airflow": {
                            "dag_id": mod.DAG_ID,
                            "dag_run_id": run_id,
                            "task_id": mod.CANARY_TASK_ID,
                        },
                        "transport_policy": "direct_then_paid",
                        "proxy_approval_id": approval.approval_id,
                        "proxy_approval_sha256": approval.approval_sha256,
                        "proxy_allocation_id": mod.CANARY_DISCOVERY_ALLOCATION_ID,
                        "proxy_work_item_id": mod.CANARY_DISCOVERY_WORK_ITEM_ID,
                        "proxy_attempt_id": expected_attempt,
                    },
                    stream,
                )
            with open(env["WHOSCORED_REQUEST_LEDGER_PATH"], "wb"):
                pass
            self.returncode = 0
            return 0

        def poll(self):
            return self.returncode

    monkeypatch.setattr(mod.subprocess, "Popen", FakeProcess)
    monkeypatch.setattr(
        mod,
        "_cleanup_supervised_sessions",
        lambda *args, **kwargs: [],
    )
    run_dir = tmp_path / "runs" / mod.stable_safe_token(run_id)
    run_dir.mkdir(parents=True)
    result = mod._execute_runner_phase(
        context={"run_id": run_id, "ti": ti},
        approval=approval,
        approval_path=tmp_path / "approval-1.json",
        allocation_id=mod.CANARY_DISCOVERY_ALLOCATION_ID,
        runner_args=("discover", "--full-history"),
        report_name="discovery.json",
        request_ledger_name="requests.jsonl",
        timeout=mod.CANARY_DISCOVERY_TIMEOUT,
        alert_metadata={
            "status": "delivered",
            "campaign_id": "campaign-1",
            "approval_id": approval.approval_id,
            "approval_sha256": approval.approval_sha256,
            "target_sha256": "b" * 64,
            "telegram_message_id": 7,
            "telegram_message_date": 1_700_000_000,
            "receipt_path": "/immutable/receipt.json",
            "receipt_sha256": "c" * 64,
        },
    )

    command = observed["command"]
    assert "--proxy-work-item-id" in command
    assert command[command.index("--proxy-work-item-id") + 1] == (
        mod.CANARY_DISCOVERY_WORK_ITEM_ID
    )
    assert "--proxy-allocation-id" not in command
    assert observed["env"]["WHOSCORED_PAID_ALERT_RECEIPT_SHA256"] == "c" * 64
    assert observed["env"][mod.PAID_GATEWAY_URL_ENV] == (mod.EXPECTED_PAID_GATEWAY_URL)
    assert observed["env"][mod.PAID_GATEWAY_TOKEN_ENV] == "g" * 32
    assert "WHOSCORED_PAID_PROXY_URL" not in observed["env"]
    assert "WHOSCORED_PROXY_CONTROL_URL" not in observed["env"]
    assert "WHOSCORED_PROXY_CONTROL_TOKEN" not in observed["env"]
    assert "WHOSCORED_PROXY_APPROVAL_HMAC_SECRET" not in observed["env"]
    assert "WHOSCORED_PAID_ALERT_HMAC_SECRET" not in observed["env"]
    assert observed["env"]["WHOSCORED_SUPERVISOR_SESSION_OWNER"]
    assert observed["env"]["WHOSCORED_SUPERVISOR_RESOURCE_LEDGER_PATH"].endswith(
        ".remote-resources.jsonl"
    )
    assert result["allocation_id"] == mod.CANARY_DISCOVERY_ALLOCATION_ID
    assert len(result["report_sha256"]) == 64
    assert len(result["request_ledger_sha256"]) == 64


@pytest.mark.unit
def test_supervised_process_timeout_kills_the_whole_process_group(monkeypatch):
    mod = _load_module()
    observed = {"signals": []}

    class TimedOutProcess:
        pid = 43210

        def __init__(self, command, *, start_new_session, **kwargs):
            assert command == ["runner"]
            assert start_new_session is True
            self.terminated = False

        def poll(self):
            return -15 if self.terminated else None

        def wait(self, timeout=None):
            if not self.terminated:
                raise mod.subprocess.TimeoutExpired(["runner"], timeout)
            return -15

    process_holder = {}
    group_alive = {"value": True}

    def fake_popen(*args, **kwargs):
        process = TimedOutProcess(*args, **kwargs)
        process_holder["process"] = process
        return process

    def fake_killpg(pid, sent_signal):
        assert pid == 43210
        if sent_signal == 0:
            if not group_alive["value"]:
                raise ProcessLookupError
            return
        observed["signals"].append(sent_signal)
        process_holder["process"].terminated = True
        group_alive["value"] = False

    monkeypatch.setattr(mod.subprocess, "Popen", fake_popen)
    monkeypatch.setattr(mod.os, "killpg", fake_killpg)

    with pytest.raises(mod.subprocess.TimeoutExpired):
        mod._run_supervised_process(
            ["runner"],
            environment={},
            timeout_seconds=0.01,
        )

    assert observed["signals"] == [mod.signal.SIGTERM]


@pytest.mark.unit
def test_supervised_process_sigkills_a_surviving_grandchild(monkeypatch):
    mod = _load_module()
    observed = {"signals": []}
    group_alive = {"value": True}

    class GrandchildProcess:
        pid = 43211

        def __init__(self, command, *, start_new_session, **kwargs):
            assert command == ["runner"]
            assert start_new_session is True
            self.child_exited = False

        def wait(self, timeout=None):
            if not self.child_exited:
                raise mod.subprocess.TimeoutExpired(["runner"], timeout)
            return -15

    process_holder = {}

    def fake_popen(*args, **kwargs):
        process = GrandchildProcess(*args, **kwargs)
        process_holder["process"] = process
        return process

    def fake_killpg(pid, sent_signal):
        assert pid == 43211
        if sent_signal == 0:
            if not group_alive["value"]:
                raise ProcessLookupError
            return
        observed["signals"].append(sent_signal)
        if sent_signal == mod.signal.SIGTERM:
            process_holder["process"].child_exited = True
        elif sent_signal == mod.signal.SIGKILL:
            group_alive["value"] = False

    monkeypatch.setattr(mod.subprocess, "Popen", fake_popen)
    monkeypatch.setattr(mod.os, "killpg", fake_killpg)
    monkeypatch.setattr(mod, "PROCESS_GROUP_TERMINATE_GRACE_SECONDS", 0.0)
    monkeypatch.setattr(mod, "PROCESS_GROUP_KILL_CONFIRM_SECONDS", 0.0)

    with pytest.raises(mod.subprocess.TimeoutExpired):
        mod._run_supervised_process(
            ["runner"],
            environment={},
            timeout_seconds=0.01,
        )

    assert observed["signals"] == [mod.signal.SIGTERM, mod.signal.SIGKILL]


@pytest.mark.unit
def test_supervised_process_cleans_group_on_base_exception(monkeypatch):
    mod = _load_module()
    observed = {"signals": []}
    group_alive = {"value": True}

    class CancelledProcess:
        pid = 43212

        def __init__(self, command, *, start_new_session, **kwargs):
            assert command == ["runner"]
            assert start_new_session is True
            self.cancelled = False

        def wait(self, timeout=None):
            if not self.cancelled:
                self.cancelled = True
                raise KeyboardInterrupt
            return -15

    def fake_killpg(pid, sent_signal):
        assert pid == 43212
        if sent_signal == 0:
            if not group_alive["value"]:
                raise ProcessLookupError
            return
        observed["signals"].append(sent_signal)
        group_alive["value"] = False

    monkeypatch.setattr(mod.subprocess, "Popen", CancelledProcess)
    monkeypatch.setattr(mod.os, "killpg", fake_killpg)

    with pytest.raises(KeyboardInterrupt):
        mod._run_supervised_process(
            ["runner"],
            environment={},
            timeout_seconds=1.0,
        )

    assert observed["signals"] == [mod.signal.SIGTERM]


@pytest.mark.unit
def test_runner_phase_cleans_remote_owner_before_reraising_cancellation(
    tmp_path, monkeypatch
):
    mod = _load_module()
    monkeypatch.setattr(mod, "RUN_ROOT", str(tmp_path / "runs"))
    allocation = SimpleNamespace(
        task_id=mod.CANARY_TASK_ID,
        work_item_id=mod.CANARY_DISCOVERY_WORK_ITEM_ID,
    )
    approval = SimpleNamespace(
        approval_id="approval-1",
        approval_sha256="a" * 64,
        allocation=lambda allocation_id: allocation,
    )
    run_id = "manual__campaign-1"
    ti = SimpleNamespace(task_id=mod.CANARY_TASK_ID, try_number=1, map_index=-1)
    cleanup_calls = []

    def cancelled(*args, **kwargs):
        raise KeyboardInterrupt

    def cleanup(path, *, owner, flaresolverr_url):
        cleanup_calls.append((path, owner, flaresolverr_url))
        return []

    monkeypatch.setattr(mod, "_run_supervised_process", cancelled)
    monkeypatch.setattr(mod, "_cleanup_supervised_sessions", cleanup)
    run_dir = tmp_path / "runs" / mod.stable_safe_token(run_id)
    run_dir.mkdir(parents=True)

    with pytest.raises(KeyboardInterrupt):
        mod._execute_runner_phase(
            context={"run_id": run_id, "ti": ti},
            approval=approval,
            approval_path=tmp_path / "approval-1.json",
            allocation_id=mod.CANARY_DISCOVERY_ALLOCATION_ID,
            runner_args=("discover", "--full-history"),
            report_name="discovery.json",
            request_ledger_name="requests.jsonl",
            timeout=mod.CANARY_DISCOVERY_TIMEOUT,
            alert_metadata={
                "status": "delivered",
                "campaign_id": "campaign-1",
                "approval_id": approval.approval_id,
                "approval_sha256": approval.approval_sha256,
                "target_sha256": "b" * 64,
                "telegram_message_id": 7,
                "telegram_message_date": 1_700_000_000,
                "receipt_path": "/immutable/receipt.json",
                "receipt_sha256": "c" * 64,
            },
        )

    assert len(cleanup_calls) == 1
    assert cleanup_calls[0][0].name.endswith(".remote-resources.jsonl")
    assert cleanup_calls[0][1]


@pytest.mark.unit
def test_supervisor_replays_owned_session_and_destroys_remote_orphan(
    tmp_path,
    monkeypatch,
):
    mod = _load_module()
    owner = "a" * 24
    session_id = f"ws-cap-{owner}-paid_flaresolverr-deadbeef00"
    ledger_path = tmp_path / "remote.jsonl"
    event = {
        "schema_version": 1,
        "event": "owned",
        "resource": "flaresolverr_session",
        "owner": owner,
        "session_id": session_id,
        "dag_id": mod.DAG_ID,
        "run_id": "manual__campaign-1",
        "task_id": mod.CANARY_TASK_ID,
        "try_number": 1,
        "recorded_at": "2026-01-01T00:00:00+00:00",
    }
    ledger_path.write_text(json.dumps(event) + "\n", encoding="utf-8")

    extension_sha256 = "d" * 64
    identity_calls = []

    def attested(relative, *, runtime_root):
        identity_calls.append((relative, runtime_root))
        return extension_sha256

    monkeypatch.setattr(
        mod._WHOSCORED_RUNTIME_CONTRACT,
        "attested_runtime_file_sha256",
        attested,
    )
    original_read_bytes = mod.Path.read_bytes

    def reject_extension_reopen(path):
        if path.name == "flaresolverr_extended.py":
            pytest.fail("cleanup reopened the extension path")
        return original_read_bytes(path)

    monkeypatch.setattr(mod.Path, "read_bytes", reject_extension_reopen)

    class FakeResponse:
        status_code = 200

        def json(self):
            return {
                "status": "ok",
                "version": "3.4.6",
                "extension_sha256": extension_sha256,
                "active": 0,
                "pending_create": 0,
                "pending_destroy": 0,
                "failed_create": 0,
                "failed_destroy": 0,
                "failure_generation": 0,
                "cleanup_scheduled": True,
            }

    class FakeClient:
        closed_count = 0
        posts = []

        def __init__(self):
            self.trust_env = True

        def mount(self, scheme, adapter):
            assert scheme in {"http://", "https://"}

        def post(self, url, **kwargs):
            self.posts.append((url, kwargs))
            return FakeResponse()

        def close(self):
            type(self).closed_count += 1

    monkeypatch.setattr(requests, "Session", FakeClient)
    monkeypatch.setattr(mod, "SESSION_SUPERVISOR_QUIET_SECONDS", 0.0)
    monkeypatch.setattr(mod, "SESSION_SUPERVISOR_POLL_SECONDS", 0.0)

    cleaned = mod._cleanup_supervised_sessions(
        ledger_path,
        owner=owner,
        flaresolverr_url="http://flaresolverr:8191",
    )

    assert cleaned == [session_id]
    assert FakeClient.closed_count == 1
    assert len(FakeClient.posts) >= 3
    assert all(item[1]["json"] == {"owner": owner} for item in FakeClient.posts)
    assert identity_calls == [
        ("scripts/flaresolverr_extended.py", mod.Path(mod._whoscored_root))
    ]


@pytest.mark.unit
def test_supervisor_rejects_truncated_or_out_of_order_resource_wal(tmp_path):
    mod = _load_module()
    owner = "b" * 24
    path = tmp_path / "remote.jsonl"
    path.write_text('{"event":"owned"}', encoding="utf-8")
    with pytest.raises(mod.AirflowException, match="truncated"):
        mod._supervised_session_ids(path, owner=owner)


@pytest.mark.unit
def test_supervisor_cleans_owner_even_when_resource_wal_is_truncated(
    tmp_path,
    monkeypatch,
):
    mod = _load_module()
    owner = "b" * 24
    path = tmp_path / "remote.jsonl"
    path.write_text('{"event":"owned"}', encoding="utf-8")
    extension_path = (
        mod.Path(mod.__file__).resolve().parents[1]
        / "scripts"
        / "flaresolverr_extended.py"
    )
    extension_sha256 = hashlib.sha256(extension_path.read_bytes()).hexdigest()
    monkeypatch.setattr(
        mod._WHOSCORED_RUNTIME_CONTRACT,
        "attested_runtime_file_sha256",
        lambda relative, *, runtime_root: extension_sha256,
    )

    class FakeResponse:
        status_code = 200

        def json(self):
            return {
                "status": "ok",
                "version": "3.4.6",
                "extension_sha256": extension_sha256,
                "active": 0,
                "pending_create": 0,
                "pending_destroy": 0,
                "failed_create": 0,
                "failed_destroy": 0,
                "failure_generation": 0,
                "cleanup_scheduled": True,
            }

    class FakeClient:
        posts = []

        def __init__(self):
            self.trust_env = True

        def mount(self, scheme, adapter):
            assert scheme in {"http://", "https://"}

        def post(self, url, **kwargs):
            type(self).posts.append((url, kwargs))
            return FakeResponse()

        def close(self):
            pass

    monkeypatch.setattr(requests, "Session", FakeClient)
    monkeypatch.setattr(mod, "SESSION_SUPERVISOR_QUIET_SECONDS", 0.0)
    monkeypatch.setattr(mod, "SESSION_SUPERVISOR_POLL_SECONDS", 0.0)

    with pytest.raises(mod.AirflowException, match="evidence is invalid"):
        mod._cleanup_supervised_sessions(
            path,
            owner=owner,
            flaresolverr_url="http://flaresolverr:8191",
        )

    assert len(FakeClient.posts) >= 3
    assert all(item[1]["json"] == {"owner": owner} for item in FakeClient.posts)
