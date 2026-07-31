"""Airflow topology/config contracts for ESPN Native Bronze v2."""

from __future__ import annotations

import importlib
from contextlib import nullcontext
from datetime import datetime, timezone
import hashlib
import json
from pathlib import Path
import sys
from types import SimpleNamespace

import pytest


ROOT = Path(__file__).resolve().parents[3]


def _reload(module_name: str):
    from airflow.operators.python import PythonOperator

    PythonOperator._instances.clear()
    sys.modules.pop(module_name, None)
    sys.modules.pop(f"dags.{module_name}", None)
    return importlib.import_module(module_name)


def _tasks():
    from airflow.operators.python import PythonOperator

    return {task.task_id: task for task in PythonOperator._instances}


def _daily_parent_context():
    logical_date = datetime(2026, 7, 31, 12, tzinfo=timezone.utc)
    run_id = "scheduled__2026-07-31T12:00:00+00:00"
    child_run_id = f"espn_daily__dag_master_pipeline__{run_id}"
    parent = {
        "schema": "espn-master-parent-v1",
        "parent_dag_id": "dag_master_pipeline",
        "parent_run_id": run_id,
        "logical_date": logical_date.isoformat(),
        "data_interval_start": logical_date.isoformat(),
        "data_interval_end": logical_date.replace(hour=13).isoformat(),
        "child_run_id": child_run_id,
    }
    trigger = SimpleNamespace(state="running", run_id=run_id)
    master = SimpleNamespace(
        dag_id="dag_master_pipeline",
        run_id=run_id,
        logical_date=logical_date,
        data_interval_start=logical_date,
        data_interval_end=logical_date.replace(hour=13),
        get_task_instance=lambda **_kwargs: trigger,
    )
    context = {
        "dag_run": SimpleNamespace(conf={"espn_parent": parent}),
        "logical_date": logical_date,
        "run_id": child_run_id,
    }
    return context, master


def test_daily_has_one_master_owner_and_real_two_wave_chain():
    module = _reload("dag_ingest_espn")
    tasks = _tasks()

    assert module.dag.schedule is None
    assert module.dag._dag_kwargs["catchup"] is False
    assert module.DAG_MODE == "daily"
    assert tasks["fetch_scoreboard_batches"].is_mapped is True
    assert tasks["fetch_summary_batches"].is_mapped is True
    assert tasks["offline_parse"].is_mapped is True
    assert tasks["staging_dq"].is_mapped is True
    assert tasks["publish_scopes"].is_mapped is True
    assert tasks["published_dq"].is_mapped is True
    assert tasks["reduce_raw_manifests"]._init_kwargs["trigger_rule"] == "none_failed"
    for task_id in (
        "acquire_scope_leases",
        "build_signed_scope_plans",
        "plan_summary_batches",
        "persist_run_manifests",
    ):
        assert tasks[task_id]._init_kwargs["multiple_outputs"] is True

    expected_chain = (
        ("validate_registry_and_admission", "acquire_scope_leases"),
        ("acquire_scope_leases", "build_signed_scope_plans"),
        ("build_signed_scope_plans", "fetch_scoreboard_batches"),
        ("fetch_scoreboard_batches", "plan_summary_batches"),
        ("plan_summary_batches", "fetch_summary_batches"),
        ("reduce_raw_manifests", "offline_parse"),
        ("offline_parse", "staging_dq"),
        ("staging_dq", "publish_scopes"),
        ("publish_scopes", "persist_run_manifests"),
        ("persist_run_manifests", "published_dq"),
        ("published_dq", "terminal_verdict"),
        ("terminal_verdict", "record_health_metrics"),
        ("terminal_verdict", "release_scope_leases"),
        ("record_health_metrics", "propagate_terminal_failure"),
        ("release_scope_leases", "propagate_terminal_failure"),
    )
    for upstream, downstream in expected_chain:
        assert upstream in tasks[downstream].upstream_task_ids


def test_only_network_waves_use_one_slot_http_pool():
    _reload("dag_ingest_espn")
    tasks = _tasks()
    pooled = {
        task_id: task._init_kwargs.get("pool")
        for task_id, task in tasks.items()
        if task._init_kwargs.get("pool") is not None
    }

    assert pooled == {
        "fetch_scoreboard_batches": "espn_http_pool",
        "fetch_summary_batches": "espn_http_pool",
    }
    for task_id in pooled:
        assert tasks[task_id]._init_kwargs["pool_slots"] == 1


def test_terminal_health_release_and_propagator_cannot_mask_failure():
    _reload("dag_ingest_espn")
    tasks = _tasks()

    assert tasks["terminal_verdict"]._init_kwargs["trigger_rule"] == "all_done"
    assert tasks["record_health_metrics"]._init_kwargs["trigger_rule"] == "all_done"
    assert tasks["release_scope_leases"]._init_kwargs["trigger_rule"] == "all_done"
    assert (
        tasks["propagate_terminal_failure"]._init_kwargs["trigger_rule"] == "all_done"
    )
    assert tasks["propagate_terminal_failure"]._init_kwargs["op_kwargs"][
        "cleanup_task_ids"
    ] == ["record_health_metrics", "release_scope_leases"]
    assert set(
        tasks["terminal_verdict"]._init_kwargs["op_kwargs"]["producer_task_ids"]
    ) == {
        "validate_registry_and_admission",
        "acquire_scope_leases",
        "build_signed_scope_plans",
        "fetch_scoreboard_batches",
        "plan_summary_batches",
        "fetch_summary_batches",
        "reduce_raw_manifests",
        "offline_parse",
        "staging_dq",
        "publish_scopes",
        "persist_run_manifests",
        "published_dq",
    }
    assert set(tasks["terminal_verdict"]._init_kwargs["op_kwargs"]) == {
        "producer_task_ids"
    }
    assert tasks["release_scope_leases"]._init_kwargs["op_kwargs"] == {}


@pytest.mark.parametrize(
    ("module_name", "mode"),
    [
        ("dag_repair_espn", "repair"),
        ("dag_backfill_espn", "backfill"),
    ],
)
def test_manual_network_modes_reuse_same_contract_without_schedule(module_name, mode):
    module = _reload(module_name)
    tasks = _tasks()

    assert module.dag.schedule is None
    assert module.DAG_MODE == mode
    assert tasks["fetch_scoreboard_batches"]._init_kwargs["pool"] == "espn_http_pool"
    assert tasks["fetch_summary_batches"]._init_kwargs["pool"] == "espn_http_pool"
    assert "publish_scopes" in tasks


def test_replay_dag_has_no_network_operator_at_all():
    module = _reload("dag_replay_espn")
    tasks = _tasks()

    assert module.dag.schedule is None
    assert module.DAG_MODE == "replay"
    assert "fetch_scoreboard_batches" not in tasks
    assert "fetch_summary_batches" not in tasks
    assert "bind_replay_raw_manifests" in tasks
    assert all(task._init_kwargs.get("pool") is None for task in tasks.values())


def test_weekly_discovery_never_promotes_and_monitor_is_network_free():
    discovery = _reload("dag_discover_espn_registry")
    discovery_tasks = _tasks()
    assert discovery.dag.schedule == "0 7 * * 1"
    assert discovery_tasks["fetch_discovery_catalog"]._init_kwargs["pool"] == (
        "espn_http_pool"
    )
    assert (
        discovery_tasks["fetch_discovery_catalog"]._init_kwargs["multiple_outputs"]
        is True
    )
    assert "promote" not in " ".join(discovery_tasks).casefold()
    assert "write_reviewable_diff" in discovery_tasks

    monitor = _reload("dag_monitor_espn")
    monitor_tasks = _tasks()
    assert monitor.dag.schedule == "0 */6 * * *"
    assert all(task._init_kwargs.get("pool") is None for task in monitor_tasks.values())
    assert "check_36h_freshness_and_alerts" in monitor_tasks


def test_production_compose_provisions_exactly_one_espn_http_slot():
    compose = (ROOT / "compose.yaml").read_text(encoding="utf-8")
    command = compose.split("airflow-init:", 1)[1].split("airflow-scheduler:", 1)[0]
    scheduler = compose.split("airflow-scheduler:", 1)[1].split(
        "airflow-triggerer:", 1
    )[0]

    assert command.count("airflow pools set 'espn_http_pool' 1") == 1
    assert scheduler.count("target: /opt/airflow/configs/espn") == 1
    espn_mount = scheduler.split("source: ./configs/espn", 1)[1].split(
        "- type: bind", 1
    )[0]
    assert "read_only: true" in espn_mount
    assert "create_host_path: false" in espn_mount


def test_master_triggers_espn_as_exact_optional_independent_branch():
    _reload("dag_master_pipeline")
    tasks = _tasks()
    trigger = tasks["ingestion_triggers.trigger_espn"]
    required_gate = tasks["validate_required_sources"]
    conf = trigger._init_kwargs["conf"]["espn_parent"]

    assert trigger.upstream_task_ids == set()
    assert trigger.task_id not in required_gate.upstream_task_ids
    assert trigger._init_kwargs["allowed_states"] == ["success", "failed"]
    assert trigger._init_kwargs["failed_states"] == []
    assert trigger._init_kwargs["reset_dag_run"] is False
    assert trigger._init_kwargs["trigger_run_id"].startswith("espn_daily__")
    assert trigger._init_kwargs["logical_date"] == "{{ logical_date.isoformat() }}"
    assert conf == {
        "schema": "espn-master-parent-v1",
        "parent_dag_id": "dag_master_pipeline",
        "parent_run_id": "{{ run_id }}",
        "logical_date": "{{ logical_date.isoformat() }}",
        "data_interval_start": "{{ data_interval_start.isoformat() }}",
        "data_interval_end": "{{ data_interval_end.isoformat() }}",
        "child_run_id": trigger._init_kwargs["trigger_run_id"],
    }


def test_daily_admission_binds_real_exact_master_run(monkeypatch):
    from dags.utils import espn_native_tasks

    context, master = _daily_parent_context()
    monkeypatch.setattr(espn_native_tasks, "_exact_parent_run", lambda _run_id: master)

    parent = espn_native_tasks._daily_parent(context)

    assert parent["parent_run_id"] == master.run_id


def test_daily_admission_rejects_forged_child_or_stale_interval(monkeypatch):
    from dags.utils import espn_native_tasks

    context, master = _daily_parent_context()
    monkeypatch.setattr(espn_native_tasks, "_exact_parent_run", lambda _run_id: master)
    context["dag_run"].conf["espn_parent"]["child_run_id"] = "forged"
    with pytest.raises(espn_native_tasks.OperationsError, match="deterministic child"):
        espn_native_tasks._daily_parent(context)

    context, master = _daily_parent_context()
    monkeypatch.setattr(espn_native_tasks, "_exact_parent_run", lambda _run_id: master)
    context["dag_run"].conf["espn_parent"]["data_interval_end"] = (
        context["logical_date"].replace(hour=14).isoformat()
    )
    with pytest.raises(espn_native_tasks.OperationsError, match="data_interval_end"):
        espn_native_tasks._daily_parent(context)


def test_daily_admission_rejects_inactive_master_trigger(monkeypatch):
    from dags.utils import espn_native_tasks

    context, master = _daily_parent_context()
    master.get_task_instance = lambda **_kwargs: SimpleNamespace(
        state="failed", run_id=master.run_id
    )
    monkeypatch.setattr(espn_native_tasks, "_exact_parent_run", lambda _run_id: master)

    with pytest.raises(espn_native_tasks.OperationsError, match="not active"):
        espn_native_tasks._daily_parent(context)


def test_discovery_rollover_is_detected_from_frozen_detail_without_network():
    from dags.utils import espn_native_tasks
    from scrapers.espn.discovery import quarantine_new_editions

    snapshot = espn_native_tasks._discovery_snapshot(
        {
            "sports": [
                {
                    "leagues": [
                        {
                            "id": "700",
                            "slug": "eng.1",
                            "name": "English Premier League",
                            "gender": "MALE",
                            "season": {
                                "year": 2027,
                                "displayName": "2027-28 English Premier League",
                                "startDate": "2027-07-01T00:00:00Z",
                                "endDate": "2028-06-30T23:59:59Z",
                            },
                            "capabilities": {
                                "schedule": "proven",
                                "lineup": "proven",
                                "matchsheet": "proven",
                            },
                        }
                    ]
                }
            ]
        },
        captured_at="2027-07-01T00:00:00+00:00",
    )
    registry = SimpleNamespace(
        by_id={
            700: SimpleNamespace(
                current_edition=SimpleNamespace(source_season_year=2026)
            )
        }
    )

    assert quarantine_new_editions(snapshot, registry) == {"700:2027"}


def test_operational_metrics_read_exact_raw_and_summary_budget(monkeypatch):
    from dags.utils import espn_native_tasks

    descriptor_ref = {"uri": "descriptor", "sha256": "a" * 64}
    descriptor = {
        "kind": "espn-scope-plan-descriptor-v1",
        "scope_id": "700:2026",
        "raw_manifest_uri": "raw-manifest",
        "scoreboard_checkpoint_uri": "scoreboard-checkpoint",
    }
    monkeypatch.setattr(
        espn_native_tasks,
        "_read_ref",
        lambda ref, **_kwargs: descriptor if ref == descriptor_ref else {},
    )
    monkeypatch.setattr(
        espn_native_tasks.runner,
        "_read_artifact",
        lambda _uri: (
            b'{"checkpoints":[{"requests":[{"proxy_bytes":0},{"proxy_bytes":7}]}]}'
        ),
    )

    metrics = espn_native_tasks._scope_operational_metrics(
        {
            "scope_ids": ["700:2026"],
            "scope_plan_refs": [descriptor_ref],
            "mode": "daily",
        },
        {
            "scopes": [
                {
                    "scope_id": "700:2026",
                    "budget_used": 800,
                    "budget_limit": 1000,
                }
            ]
        },
    )

    assert metrics["700:2026"] == {
        "network_requests": 2,
        "proxy_bytes": 7,
        "budget_used": 800,
        "budget_limit": 1000,
        "schema_drift": False,
    }


def test_generation_binding_rejects_stale_current_run_snapshot():
    from dags.utils import espn_native_tasks

    now = datetime(2026, 7, 31, 12, tzinfo=timezone.utc)
    scope = SimpleNamespace(scope_id="700:2026")
    binding = SimpleNamespace(
        generation_id="generation-current",
        batch_id="batch-current",
        ingested_at=now,
        prior=None,
    )
    loaded = SimpleNamespace(
        plan=SimpleNamespace(run_id="run-current", registry_signature="b" * 64),
        registry_snapshot_uri="s3://artifacts/registry.json",
        signature="a" * 64,
        bindings={scope.scope_id: binding},
    )
    generation = SimpleNamespace(
        plan=scope,
        run_id="run-old",
        registry_snapshot_uri=loaded.registry_snapshot_uri,
        registry_signature=loaded.plan.registry_signature,
        plan_signature=loaded.signature,
        generation_id=binding.generation_id,
        batch_id=binding.batch_id,
        ingested_at=now,
    )

    with pytest.raises(espn_native_tasks.OperationsError, match="current signed plan"):
        espn_native_tasks._assert_generation_binding(
            generation=generation,
            loaded=loaded,
            scope=scope,
            state="staged",
        )


def test_generation_binding_accepts_only_exact_signed_noop_prior():
    from dags.utils import espn_native_tasks

    scope = SimpleNamespace(scope_id="700:2026")
    prior = SimpleNamespace(
        generation_id="generation-prior",
        generation_signature="c" * 64,
        manifest_sha256="d" * 64,
    )
    loaded = SimpleNamespace(bindings={scope.scope_id: SimpleNamespace(prior=prior)})
    generation = SimpleNamespace(
        plan=scope,
        generation_id=prior.generation_id,
        generation_signature=prior.generation_signature,
        manifest_sha256=prior.manifest_sha256,
    )

    espn_native_tasks._assert_generation_binding(
        generation=generation, loaded=loaded, scope=scope, state="noop"
    )
    generation.manifest_sha256 = "e" * 64
    with pytest.raises(espn_native_tasks.OperationsError, match="signed prior"):
        espn_native_tasks._assert_generation_binding(
            generation=generation,
            loaded=loaded,
            scope=scope,
            state="noop",
        )


def _artifact_ref(artifacts, uri, payload):
    body = (json.dumps(payload, sort_keys=True, separators=(",", ":")) + "\n").encode()
    artifacts[uri] = body
    return {"uri": uri, "sha256": hashlib.sha256(body).hexdigest()}


def test_summary_mapped_retry_resumes_only_missing_exact_request(monkeypatch):
    from dags.utils import espn_native_tasks

    artifacts = {}
    expected = espn_native_tasks.make_summary_batches(
        (101, 102, 103),
        run_id="run-current",
        attempt=2,
        scope_id="700:2026",
        plan_signature="a" * 64,
        max_events=3,
    )["batches"][0]
    batch_ref = _artifact_ref(
        artifacts,
        "s3://artifacts/summary-plan.json",
        {
            "kind": "espn-summary-batch-plan-v1",
            "schema_version": 1,
            "scope_binding_ref": {"uri": "binding", "sha256": "f" * 64},
            "expected_batch": expected,
            "checkpoint_uri": "s3://artifacts/summary-final.json",
        },
    )
    loaded = SimpleNamespace(
        plan=SimpleNamespace(run_id="run-current"), attempt=2, signature="a" * 64
    )
    scope = SimpleNamespace(scope_id="700:2026", slug="eng.1", espn_id=700)
    descriptor = {"raw_store_uri": "s3://raw"}
    calls = []
    failed = False

    class Client:
        def fetch_json(self, _url, _endpoint, params, **kwargs):
            nonlocal failed
            event_id = params["event"]
            calls.append((event_id, kwargs["force_refresh"]))
            if event_id == 103 and not failed:
                failed = True
                raise RuntimeError("late request failed")
            return SimpleNamespace(event_id=event_id)

    monkeypatch.setattr(
        espn_native_tasks.runner,
        "_read_artifact",
        lambda uri: (
            artifacts[uri]
            if uri in artifacts
            else (_ for _ in ()).throw(FileNotFoundError(uri))
        ),
    )
    monkeypatch.setattr(
        espn_native_tasks.runner,
        "_write_artifact",
        lambda uri, body, **_kwargs: artifacts.setdefault(uri, body),
    )
    monkeypatch.setattr(
        espn_native_tasks,
        "_binding",
        lambda _ref: ({}, descriptor, loaded, scope, object()),
    )
    monkeypatch.setattr(
        espn_native_tasks.EspnRawStore, "from_uri", lambda _uri: object()
    )
    monkeypatch.setattr(
        espn_native_tasks, "_http_client", lambda *_args, **_kwargs: Client()
    )
    monkeypatch.setattr(
        espn_native_tasks.runner,
        "_raw_request_from_fetch",
        lambda **kwargs: {
            "request_id": kwargs["request_id"],
            "scope_id": kwargs["scope_id"],
            "endpoint": kwargs["endpoint"],
            "event_id": kwargs["event_id"],
            "url_fingerprint": "b" * 64,
            "raw_uri": f"s3://raw/{kwargs['request_id']}.json.gz",
            "raw_sha256": "c" * 64,
            "fetched_at": "2026-07-31T12:00:00Z",
            "http_status": 200,
            "direct_bytes": 1,
            "proxy_bytes": 0,
            "query_start": None,
            "query_end": None,
        },
    )

    with pytest.raises(RuntimeError, match="late request"):
        espn_native_tasks.fetch_summary_batch(summary_batch_ref=batch_ref)
    assert len([uri for uri in artifacts if "-requests/" in uri]) == 2

    result = espn_native_tasks.fetch_summary_batch(summary_batch_ref=batch_ref)

    assert calls == [(101, True), (102, True), (103, True), (103, True)]
    assert result["summary_phase_ref"]["uri"].endswith(".phase.json")


def test_scoreboard_mapped_retry_resumes_only_missing_exact_request(monkeypatch):
    from dags.utils import espn_native_tasks

    artifacts = {}
    requests = [
        SimpleNamespace(
            request_id=f"scoreboard:{day}",
            url="https://example.test/scoreboard",
            params={"day": day},
            query_start=f"2026-07-0{day}",
            query_end=f"2026-07-0{day}",
        )
        for day in (1, 2)
    ]
    expected = espn_native_tasks.seal_raw_batch_descriptor(
        endpoint="scoreboard",
        run_id="run-current",
        attempt=1,
        scope_id="700:2026",
        plan_signature="a" * 64,
        batch_id="scoreboard-batch",
        request_ids=[item.request_id for item in requests],
        event_ids=(),
    )
    loaded = SimpleNamespace(
        plan=SimpleNamespace(run_id="run-current", as_of=datetime(2026, 7, 31).date()),
        attempt=1,
        signature="a" * 64,
        mode="daily",
        bindings={"700:2026": object()},
    )
    scope = SimpleNamespace(scope_id="700:2026", espn_id=700)
    descriptor = {
        "raw_store_uri": "s3://raw",
        "expected_scoreboard_batch": expected,
        "scoreboard_checkpoint_uri": "s3://artifacts/scoreboard-final.json",
        "scope_root": "s3://artifacts/scope",
    }
    calls = []
    failed = False

    class Client:
        def fetch_json(self, _url, _endpoint, params, **kwargs):
            nonlocal failed
            day = params["day"]
            calls.append((day, kwargs["force_refresh"]))
            if day == 2 and not failed:
                failed = True
                raise RuntimeError("late scoreboard failed")
            return SimpleNamespace(day=day)

    monkeypatch.setattr(
        espn_native_tasks.runner,
        "_read_artifact",
        lambda uri: (
            artifacts[uri]
            if uri in artifacts
            else (_ for _ in ()).throw(FileNotFoundError(uri))
        ),
    )
    monkeypatch.setattr(
        espn_native_tasks.runner,
        "_write_artifact",
        lambda uri, body, **_kwargs: artifacts.setdefault(uri, body),
    )
    monkeypatch.setattr(
        espn_native_tasks,
        "_binding",
        lambda _ref: ({}, descriptor, loaded, scope, object()),
    )
    monkeypatch.setattr(
        espn_native_tasks.runner,
        "_scoreboard_requests",
        lambda *_args, **_kwargs: requests,
    )
    monkeypatch.setattr(
        espn_native_tasks.runner, "_effective_mode", lambda _loaded: "daily"
    )
    monkeypatch.setattr(
        espn_native_tasks.EspnRawStore, "from_uri", lambda _uri: object()
    )
    monkeypatch.setattr(
        espn_native_tasks, "_http_client", lambda *_args, **_kwargs: Client()
    )
    monkeypatch.setattr(
        espn_native_tasks.runner,
        "_raw_request_from_fetch",
        lambda **kwargs: {
            "request_id": kwargs["request_id"],
            "scope_id": kwargs["scope_id"],
            "endpoint": kwargs["endpoint"],
            "event_id": None,
            "url_fingerprint": "b" * 64,
            "raw_uri": f"s3://raw/{kwargs['request_id']}.json.gz",
            "raw_sha256": "c" * 64,
            "fetched_at": "2026-07-31T12:00:00Z",
            "http_status": 200,
            "direct_bytes": 1,
            "proxy_bytes": 0,
            "query_start": kwargs["query_start"],
            "query_end": kwargs["query_end"],
        },
    )

    binding_ref = {"uri": "binding", "sha256": "f" * 64}
    with pytest.raises(RuntimeError, match="late scoreboard"):
        espn_native_tasks.fetch_scoreboard_batch(scope_binding_ref=binding_ref)
    assert len([uri for uri in artifacts if "-requests/" in uri]) == 1

    espn_native_tasks.fetch_scoreboard_batch(scope_binding_ref=binding_ref)

    assert calls == [(1, True), (2, True), (2, True)]


def test_complete_head_requires_exact_snapshot_and_physical_complete(monkeypatch):
    from dags.utils import espn_native_tasks
    from scrapers.espn.operations import ScopeHead

    published_at = datetime(2026, 7, 31, 12, tzinfo=timezone.utc)
    head = ScopeHead(
        dag_id="dag_ingest_espn",
        scope_id="700:2026",
        generation_id="generation-1",
        generation_signature="a" * 64,
        manifest_sha256="b" * 64,
        snapshot_uri="s3://artifacts/snapshot.json",
        snapshot_sha256="c" * 64,
        registry_signature="d" * 64,
        plan_signature="e" * 64,
        run_id="subject-run",
        published_at=published_at,
    )
    generation = SimpleNamespace(
        plan=SimpleNamespace(scope_id=head.scope_id),
        generation_id=head.generation_id,
        generation_signature=head.generation_signature,
        manifest_sha256=head.manifest_sha256,
        run_id=head.run_id,
        registry_signature=head.registry_signature,
        plan_signature=head.plan_signature,
    )
    monkeypatch.setattr(
        espn_native_tasks.runner,
        "load_scope_snapshot",
        lambda *_args, **_kwargs: generation,
    )
    monkeypatch.setattr(
        espn_native_tasks,
        "EspnBronzeRepository",
        lambda: SimpleNamespace(
            verify_published_scope=lambda value: SimpleNamespace(
                passed=value is generation
            )
        ),
    )

    assert espn_native_tasks._verified_complete_head(head) == (head, "complete")
    generation.run_id = "prior-or-stale-run"
    assert espn_native_tasks._verified_complete_head(head) == (None, "incomplete")


def test_publish_scope_recovers_crash_after_complete_from_durable_intent(
    monkeypatch,
):
    from dags.utils import espn_native_tasks
    from scrapers.espn.operations import PublicationFence
    from scrapers.espn.repository import ScopePublicationState

    artifacts = {}
    crash_evidence_once = True
    prepared_at = datetime(2026, 7, 31, 12, tzinfo=timezone.utc)
    clock_values = iter(prepared_at.replace(second=second) for second in range(8))
    scope = SimpleNamespace(scope_id="700:2026")
    loaded = SimpleNamespace(
        plan=SimpleNamespace(run_id="run-current", registry_signature="b" * 64),
        attempt=2,
        signature="a" * 64,
    )
    generation = SimpleNamespace(
        plan=scope,
        generation_id="generation-current",
        generation_signature="c" * 64,
        manifest_sha256="d" * 64,
    )
    lease = SimpleNamespace(epoch=7)
    descriptor = {
        "dag_id": "dag_ingest_espn",
        "scope_root": "s3://artifacts/scope",
    }
    dq = {
        "scope_binding_ref": {"uri": "binding", "sha256": "e" * 64},
        "snapshot_ref": {
            "uri": "s3://artifacts/scope/generation.json",
            "sha256": "f" * 64,
        },
        "state": "complete",
        "quality": {"passed": True},
        "direct_requests": 3,
        "proxy_bytes": 0,
    }
    recorded = []

    def write_artifact(uri, body, **_kwargs):
        nonlocal crash_evidence_once
        if uri.endswith("run-evidence.json") and crash_evidence_once:
            crash_evidence_once = False
            raise RuntimeError("crash after COMPLETE")
        existing = artifacts.setdefault(uri, body)
        if existing != body:
            raise AssertionError(f"immutable conflict at {uri}")

    class Store:
        def renew(self, current, **_kwargs):
            return current

        def read_scope_heads_owned(self, _leases, **_kwargs):
            return {}

        def publication_guard(self, _lease, **_kwargs):
            fence = PublicationFence(
                lambda: None,
                lambda head, evidence: recorded.append((head, evidence)),
                lambda: next(clock_values),
            )
            return nullcontext(fence)

    publication_states = iter(
        (ScopePublicationState.PUBLISHED, ScopePublicationState.IDEMPOTENT)
    )

    class Repository:
        def exact_complete_exists(self, _generation):
            return False

        def publish_scope(self, current_generation, *, publication_fence):
            publication_fence()
            return SimpleNamespace(
                state=next(publication_states),
                manifest_sha256=current_generation.manifest_sha256,
            )

    monkeypatch.setattr(espn_native_tasks, "_read_ref", lambda *_args, **_kwargs: dq)
    monkeypatch.setattr(
        espn_native_tasks,
        "_binding",
        lambda _ref: ({}, descriptor, loaded, scope, lease),
    )
    monkeypatch.setattr(
        espn_native_tasks.runner,
        "load_scope_snapshot",
        lambda *_args, **_kwargs: generation,
    )
    monkeypatch.setattr(
        espn_native_tasks, "_assert_generation_binding", lambda **_: None
    )
    monkeypatch.setattr(
        espn_native_tasks,
        "validate_scope_generation",
        lambda _generation: SimpleNamespace(passed=True),
    )
    monkeypatch.setattr(
        espn_native_tasks, "_quality_payload", lambda _quality: {"passed": True}
    )
    monkeypatch.setattr(
        espn_native_tasks.PostgresEspnControlStore,
        "from_env",
        classmethod(lambda _cls: Store()),
    )
    monkeypatch.setattr(espn_native_tasks, "EspnBronzeRepository", Repository)
    monkeypatch.setattr(
        espn_native_tasks.runner,
        "_read_artifact",
        lambda uri: (
            artifacts[uri]
            if uri in artifacts
            else (_ for _ in ()).throw(FileNotFoundError(uri))
        ),
    )
    monkeypatch.setattr(espn_native_tasks.runner, "_write_artifact", write_artifact)

    with pytest.raises(RuntimeError, match="crash after COMPLETE"):
        espn_native_tasks.publish_scope(
            staging_dq_ref={"uri": "dq", "sha256": "0" * 64}
        )

    result = espn_native_tasks.publish_scope(
        staging_dq_ref={"uri": "dq", "sha256": "0" * 64}
    )

    assert result["publication_ref"]["uri"].endswith("publication-result.json")
    assert recorded[0][0].published_at == prepared_at
    assert recorded[0][1].recorded_at == prepared_at
    assert any(uri.endswith("publication-intent.json") for uri in artifacts)


def test_publication_intent_cannot_be_created_after_exact_complete(monkeypatch):
    from dags.utils import espn_native_tasks

    writes = []
    loaded = SimpleNamespace(
        plan=SimpleNamespace(run_id="run-current", registry_signature="b" * 64),
        attempt=2,
        signature="a" * 64,
    )
    scope = SimpleNamespace(scope_id="700:2026")
    generation = SimpleNamespace(
        generation_id="generation-current",
        generation_signature="c" * 64,
        manifest_sha256="d" * 64,
    )
    snapshot_ref = {"uri": "s3://artifacts/generation.json", "sha256": "e" * 64}
    monkeypatch.setattr(
        espn_native_tasks.runner,
        "_read_artifact",
        lambda uri: (_ for _ in ()).throw(FileNotFoundError(uri)),
    )
    monkeypatch.setattr(
        espn_native_tasks.runner,
        "_write_artifact",
        lambda uri, body, **_kwargs: writes.append((uri, body)),
    )

    with pytest.raises(espn_native_tasks.OperationsError, match="predates intent"):
        espn_native_tasks._load_or_create_publication_intent(
            uri="s3://artifacts/publication-intent.json",
            dag_id="dag_ingest_espn",
            loaded=loaded,
            scope=scope,
            generation=generation,
            snapshot_ref=snapshot_ref,
            prepared_at=datetime(2026, 7, 31, 12, tzinfo=timezone.utc),
            complete_exists=lambda: True,
        )

    assert writes == []


def test_publication_intent_reference_is_revalidated_downstream(monkeypatch):
    from dags.utils import espn_native_tasks

    artifacts = {}
    loaded = SimpleNamespace(
        plan=SimpleNamespace(run_id="run-current", registry_signature="b" * 64),
        attempt=2,
        signature="a" * 64,
    )
    scope = SimpleNamespace(scope_id="700:2026")
    generation = SimpleNamespace(
        generation_id="generation-current",
        generation_signature="c" * 64,
        manifest_sha256="d" * 64,
    )
    snapshot_ref = {"uri": "s3://artifacts/generation.json", "sha256": "e" * 64}
    monkeypatch.setattr(
        espn_native_tasks.runner,
        "_read_artifact",
        lambda uri: (
            artifacts[uri]
            if uri in artifacts
            else (_ for _ in ()).throw(FileNotFoundError(uri))
        ),
    )
    monkeypatch.setattr(
        espn_native_tasks.runner,
        "_write_artifact",
        lambda uri, body, **_kwargs: artifacts.setdefault(uri, body),
    )
    _, intent_ref, _ = espn_native_tasks._load_or_create_publication_intent(
        uri="s3://artifacts/publication-intent.json",
        dag_id="dag_ingest_espn",
        loaded=loaded,
        scope=scope,
        generation=generation,
        snapshot_ref=snapshot_ref,
        prepared_at=datetime(2026, 7, 31, 12, tzinfo=timezone.utc),
        complete_exists=lambda: False,
    )
    loaded.plan.run_id = "different-run"

    with pytest.raises(espn_native_tasks.OperationsError, match="intent identity"):
        espn_native_tasks._validate_publication_intent_ref(
            intent_ref,
            dag_id="dag_ingest_espn",
            loaded=loaded,
            scope=scope,
            generation=generation,
            snapshot_ref=snapshot_ref,
        )


def test_monitor_uses_database_clock_and_distinguishes_missing_subject(monkeypatch):
    from dags.utils import espn_native_tasks

    logical = datetime(2026, 7, 1, tzinfo=timezone.utc)
    database_now = datetime(2026, 7, 31, 13, tzinfo=timezone.utc)
    observed = []

    class Store:
        def migrate(self):
            pass

        def read_scope_heads(self, _scope_ids):
            return {}

        def current_time(self):
            return database_now

    registry = SimpleNamespace(signature=lambda: "a" * 64)
    monkeypatch.setattr(espn_native_tasks, "load_registry", lambda _path: registry)
    monkeypatch.setattr(
        espn_native_tasks, "_selected_scopes", lambda *_args: ("700:2026",)
    )
    monkeypatch.setattr(
        espn_native_tasks.PostgresEspnControlStore,
        "from_env",
        classmethod(lambda _cls: Store()),
    )
    monkeypatch.setattr(
        espn_native_tasks, "_latest_discovery_flags", lambda _scope: (False, False)
    )
    monkeypatch.setattr(
        espn_native_tasks,
        "evaluate_alerts",
        lambda snapshot, *, observed_at: observed.append((snapshot, observed_at)) or (),
    )
    monkeypatch.setattr(
        espn_native_tasks,
        "_write_payload",
        lambda uri, payload, **_kwargs: {"uri": uri, "sha256": "f" * 64},
    )
    monkeypatch.setenv(espn_native_tasks.ARTIFACT_ROOT_ENV, "s3://artifacts")
    context = {
        "dag": SimpleNamespace(dag_id="dag_monitor_espn"),
        "run_id": "monitor-run",
        "logical_date": logical,
        "params": {"attempt": 1},
    }

    espn_native_tasks.check_36h_freshness_and_alerts(**context)

    snapshot, observed_at = observed[0]
    assert observed_at == database_now
    assert snapshot["run_id"] == "monitor-run"
    assert snapshot["subject_dag_id"] is None
    assert snapshot["subject_run_id"] is None
    assert snapshot["identity_kind"] == "monitor-subject"


def test_first_discovery_bootstraps_source_only_catalog_without_empty_diff():
    from dags.utils import espn_native_tasks
    from scrapers.espn.discovery import CatalogCandidate, CatalogSnapshot, diff_catalogs
    from scrapers.espn.models import (
        AgeClass,
        CapabilityState,
        EntityCapabilities,
        Gender,
    )

    capabilities = EntityCapabilities(
        CapabilityState.PROVEN, CapabilityState.PROVEN, CapabilityState.PROVEN
    )
    promoted = SimpleNamespace(
        espn_id=700,
        slug="eng.1",
        name="Premier League",
        gender=Gender.MALE,
        age_class=AgeClass.SENIOR,
        gender_evidence=("reviewed",),
        current_edition=SimpleNamespace(
            source_season_year=2026,
            display_name="2026-27 Premier League",
            start_date=datetime(2026, 7, 1).date(),
            end_date=datetime(2027, 6, 30).date(),
            capabilities=capabilities,
        ),
    )
    current = CatalogSnapshot(
        captured_at="2026-07-31T12:00:00+00:00",
        candidates=(
            CatalogCandidate(
                espn_id=700,
                slug="eng.1",
                name="Premier League",
                group="England",
                source_order=0,
                gender=Gender.MALE,
                source_season_year=2026,
                edition_display_name="2026-27 Premier League",
                start_date="2026-07-01",
                end_date="2027-06-30",
                capabilities=capabilities,
            ),
            CatalogCandidate(
                espn_id=999,
                slug="existing.extra",
                name="Existing Extra League",
                group="Other",
                source_order=1,
            ),
        ),
    )
    registry = SimpleNamespace(by_id={700: promoted}, competitions=(promoted,))

    baseline = espn_native_tasks._promoted_registry_baseline(current, registry)

    assert diff_catalogs(baseline, current).changes == ()


@pytest.mark.parametrize(
    ("artifact_dag_id", "artifact_run_id"),
    [("dag_repair_espn", "current-run"), ("dag_ingest_espn", "prior-run")],
)
def test_terminal_rejects_stale_or_prior_plan_artifact(
    monkeypatch, artifact_dag_id, artifact_run_id
):
    from dags.utils import espn_native_tasks

    index = {
        "kind": "espn-plan-index-v1",
        "dag_id": artifact_dag_id,
        "run_id": artifact_run_id,
        "attempt": 1,
        "scope_ids": [],
        "scope_plan_refs": [],
        "mode": "daily",
        "registry_signature": "a" * 64,
    }
    monkeypatch.setattr(espn_native_tasks, "_artifact_root", lambda: "s3://artifacts")
    monkeypatch.setattr(espn_native_tasks, "_read_ref", lambda *_args, **_kwargs: index)
    monkeypatch.setattr(
        espn_native_tasks,
        "_write_payload",
        lambda uri, payload, **_kwargs: {"uri": uri, "sha256": "f" * 64},
    )
    context = {
        "dag": SimpleNamespace(dag_id="dag_ingest_espn"),
        "dag_run": SimpleNamespace(get_task_instances=lambda: ()),
        "run_id": "current-run",
        "logical_date": datetime(2026, 7, 31, tzinfo=timezone.utc),
        "params": {"attempt": 1},
    }

    with pytest.raises(Exception, match="terminal plan identity mismatch"):
        espn_native_tasks.terminal_verdict(
            producer_task_ids=("validate_registry_and_admission",),
            plan_index_ref={"uri": "index", "sha256": "b" * 64},
            **context,
        )
