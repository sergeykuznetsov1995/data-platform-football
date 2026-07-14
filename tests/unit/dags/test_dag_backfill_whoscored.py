"""Contracts for the manual resumable WhoScored backfill DAG."""

from __future__ import annotations

import hashlib
import importlib
import json
import os
import sys
import types
from datetime import datetime, timedelta, timezone
from types import SimpleNamespace

import pytest


@pytest.fixture(autouse=True)
def _clean_operator_registries(monkeypatch):
    from airflow.operators.bash import BashOperator
    from airflow.operators.python import PythonOperator

    BashOperator._instances.clear()
    PythonOperator._instances.clear()
    monkeypatch.setenv("AIRFLOW__CORE__EXECUTOR", "LocalExecutor")
    yield


def _load_module():
    sys.modules.pop("dag_backfill_whoscored", None)
    sys.modules.pop("dags.dag_backfill_whoscored", None)
    return importlib.import_module("dag_backfill_whoscored")


def _timing_provenance(**values):
    started = datetime.now(timezone.utc) - timedelta(hours=1)
    discovery_mode = values.pop(
        "catalog_discovery_mode",
        "full_history" if values.get("full_history_discovery") else "incremental",
    )
    return {
        "catalog_batch_id": "wsc2-generation",
        "catalog_discovery_mode": discovery_mode,
        "backfill_started_at": started.isoformat(),
        "backfill_deadline_at": (started + timedelta(days=30)).isoformat(),
        **values,
    }


def _staged_population_ref():
    return {
        "staged_relation": {
            "population_sha256": "f" * 64,
            "table": "iceberg.bronze.whoscored_backfill_dq_population",
            "snapshot_id": 123,
        }
    }


def _configure_full_history_prepare(monkeypatch, tmp_path, run_discover):
    mod = _load_module()
    from dags.scripts import run_whoscored_scraper, whoscored_ops_store

    catalog_snapshot = object()
    repository = SimpleNamespace(
        ensure_schema=lambda: None,
        load_catalog_generation_snapshot=lambda: (
            {
                "catalog_batch_id": "wsc2-traffic",
                "catalog_discovery_mode": "full_history",
            },
            catalog_snapshot,
        ),
    )
    scope = run_whoscored_scraper.RunnerScope.parse("WS-1=2026")
    runtime_scope = SimpleNamespace(stage_ids=(23752,))
    monkeypatch.setattr(run_whoscored_scraper, "_new_repository", lambda: repository)
    monkeypatch.setattr(run_whoscored_scraper, "_load_runtime", lambda: object)
    monkeypatch.setattr(run_whoscored_scraper, "_run_discover", run_discover)
    monkeypatch.setattr(
        run_whoscored_scraper,
        "_select_catalog_snapshot_scopes",
        lambda snapshot, *_args, **_kwargs: (
            [(scope, runtime_scope)]
            if snapshot is catalog_snapshot
            else pytest.fail("wrong catalog generation")
        ),
    )

    class State:
        def create_plan(self, **_kwargs):
            return {
                "plan_id": "a" * 64,
                "artifact": {"uri": "s3://ops/plan", "sha256": "b" * 64},
            }

    monkeypatch.setattr(
        whoscored_ops_store.WhoScoredBackfillState,
        "from_env",
        classmethod(lambda _cls: State()),
    )
    monkeypatch.setenv("WHOSCORED_OPS_STORE_URI", tmp_path.as_uri())
    monkeypatch.setattr(mod, "RUN_ROOT", str(tmp_path / "runs"))
    return mod


def _backfill_context(*, task_id="prepare_backfill_plan"):
    return {
        "dag": SimpleNamespace(dag_id="dag_backfill_whoscored"),
        "run_id": "manual__discovery-traffic",
        "ti": SimpleNamespace(task_id=task_id, map_index=-1, try_number=1),
        "params": {
            "all_catalog": True,
            "queue_id": "q",
            "scopes": [],
            "require_zero_paid": True,
        },
    }


@pytest.mark.unit
def test_multi_stage_schedule_units_drive_capacity_breach(monkeypatch):
    mod = _load_module()
    monkeypatch.setenv(
        "WHOSCORED_BACKFILL_ASSUMED_REQUEST_UNITS_PER_DAY", "1000"
    )
    now = datetime(2026, 7, 14, tzinfo=timezone.utc)
    started = now - timedelta(days=29, hours=12)
    plan = {
        "provenance": {
            "backfill_started_at": started.isoformat(),
            "backfill_deadline_at": (started + timedelta(days=30)).isoformat(),
        }
    }
    progress = {
        "remaining_request_units": 13 * 70,
        "estimated_completed_request_units": 0,
        "actual_completed_request_units": 0,
        "schedule_stage_cardinality_drifts": 0,
    }

    with pytest.raises(mod.AirflowException, match="30-day backfill SLO failed"):
        mod._backfill_slo_summary(plan, progress, now=now)

    summary = mod._backfill_slo_summary(
        plan,
        {**progress, "remaining_request_units": 70},
        now=now,
    )
    assert summary["capacity_status"] == "sufficient"
    assert summary["request_unit_accounting"] == (
        "actual-completed-plus-exact-match-preview-estimated-remaining-v2"
    )
    assert summary["capacity_assumption"] == (
        "slo-planning-only-not-a-runtime-throttle"
    )


@pytest.mark.unit
def test_all_catalog_scale_is_blocked_by_enforced_source_ceiling(monkeypatch):
    mod = _load_module()
    monkeypatch.delenv(
        "WHOSCORED_BACKFILL_ASSUMED_REQUEST_UNITS_PER_DAY", raising=False
    )
    now = datetime(2026, 7, 14, tzinfo=timezone.utc)
    plan = {
        "provenance": {
            "backfill_started_at": now.isoformat(),
            "backfill_deadline_at": (now + timedelta(days=30)).isoformat(),
        }
    }
    # 1.9m match pages + 611k preview pages + 15,979*70 schedule units
    # + 202k profile pages. These are a sizing model, not a frozen plan.
    remaining = 1_900_000 + 611_000 + 15_979 * 70 + 202_000
    progress = {
        "remaining_request_units": remaining,
        "estimated_completed_request_units": 0,
        "actual_completed_request_units": 0,
        "schedule_stage_cardinality_drifts": 0,
    }

    summary = mod._backfill_slo_summary(
        plan, progress, now=now, enforce_capacity=False
    )

    assert summary["capacity_hard_ceiling_request_units_per_day"] == 86_400
    assert summary["assumed_capacity_request_units_per_day"] == 86_400
    assert summary["required_request_units_per_day"] == pytest.approx(
        remaining / 30
    )
    assert summary["capacity_status"] == "breach"
    assert summary["capacity_blocker"] == (
        "required_request_units_per_day_exceed_assumed_capacity"
    )
    assert summary["observed_projection_status"] == "insufficient_sample"
    with pytest.raises(mod.AirflowException, match="30-day backfill SLO failed"):
        mod._backfill_slo_summary(plan, progress, now=now)


@pytest.mark.unit
def test_capacity_assumption_cannot_exceed_enforced_source_ceiling(monkeypatch):
    mod = _load_module()
    monkeypatch.setenv(
        "WHOSCORED_BACKFILL_ASSUMED_REQUEST_UNITS_PER_DAY", "86401"
    )
    now = datetime(2026, 7, 14, tzinfo=timezone.utc)
    plan = {
        "provenance": {
            "backfill_started_at": now.isoformat(),
            "backfill_deadline_at": (now + timedelta(days=30)).isoformat(),
        }
    }

    with pytest.raises(
        mod.AirflowException,
        match=(
            "WHOSCORED_BACKFILL_ASSUMED_REQUEST_UNITS_PER_DAY must be "
            "in 1000..86400"
        ),
    ):
        mod._backfill_slo_summary(
            plan,
            {"remaining_request_units": 1},
            now=now,
        )


@pytest.mark.unit
def test_observed_throughput_projection_is_advisory_after_sufficient_sample(
    monkeypatch,
):
    mod = _load_module()
    monkeypatch.delenv(
        "WHOSCORED_BACKFILL_ASSUMED_REQUEST_UNITS_PER_DAY", raising=False
    )
    started = datetime(2026, 7, 13, tzinfo=timezone.utc)
    now = started + timedelta(days=1)
    plan = {
        "provenance": {
            "backfill_started_at": started.isoformat(),
            "backfill_deadline_at": (started + timedelta(days=30)).isoformat(),
        }
    }

    summary = mod._backfill_slo_summary(
        plan,
        {
            "remaining_request_units": 50_000,
            "estimated_completed_request_units": 1_000,
            "actual_completed_request_units": 1_000,
            "schedule_stage_cardinality_drifts": 0,
        },
        now=now,
    )

    assert summary["capacity_status"] == "sufficient"
    assert summary["observed_projection_status"] == "lagging"
    assert summary["observed_request_units_per_day"] == 1_000
    assert summary["observed_projected_days_remaining"] == 50
    assert summary["observed_projection_is_advisory"] is True


@pytest.mark.unit
def test_backfill_is_manual_bounded_mapped_and_uses_durable_state():
    mod = _load_module()
    from airflow.operators.bash import BashOperator

    assert mod.dag.schedule is None
    assert mod.dag._dag_kwargs["max_active_runs"] == 1
    assert mod.BACKFILL_CHUNK_SIZE == 25
    task = next(
        item
        for item in BashOperator._instances
        if item.task_id == "run_whoscored_backfill_item"
    )
    assert task.is_mapped is True
    assert task._expand_kwargs["bash_command"].operator.task_id == "build_backfill_work"
    assert task.bash_command is None
    assert mod.MAX_WORK_ITEMS_PER_RUN == 100
    assert mod.PROFILE_CHUNK_SIZE == 200
    assert task._init_kwargs["retries"] == 1
    assert task._init_kwargs["retry_delay"].total_seconds() == 10 * 60
    assert task._init_kwargs["pool"] == mod.BACKFILL_POOL
    assert task._init_kwargs["pool_slots"] == 1
    assert task._init_kwargs["do_xcom_push"] is False
    assert "WHOSCORED_REQUEST_LEDGER_PATH" in task.env
    assert task.env["AIRFLOW_CTX_MAP_INDEX"] == "{{ ti.map_index }}"
    assert "WHOSCORED_PAID_PROXY_URL" not in task.env
    assert mod.dag._dag_kwargs["dagrun_timeout"].total_seconds() == 12 * 3600

    from airflow.operators.python import PythonOperator

    continuation = next(
        item
        for item in PythonOperator._instances
        if item.task_id == "schedule_next_whoscored_backfill_batch"
    )
    assert continuation._init_kwargs["pool"] == mod.DQ_POOL
    assert continuation._init_kwargs["trigger_rule"] == "all_done"
    assert continuation._init_kwargs["retries"] == 2
    historical_dq = next(
        item
        for item in PythonOperator._instances
        if item.task_id == "validate_global_historical_dq"
    )
    assert historical_dq._init_kwargs["pool"] == mod.DIRECT_POOL
    assert historical_dq._init_kwargs["pool_slots"] == 2
    assert continuation._init_kwargs["retry_delay"].total_seconds() == 30 * 60


@pytest.mark.unit
def test_backfill_requires_explicit_selector():
    mod = _load_module()

    with pytest.raises(mod.AirflowException, match="requires explicit scopes"):
        mod.validate_backfill_params(params={"scopes": [], "game_ids": []})


@pytest.mark.unit
def test_frozen_scope_values_relation_is_escaped_and_hard_bounded():
    mod = _load_module()

    sql = mod._scope_values_sql([("WS-O'Brien", "2026")])

    assert "('WS-O''Brien', '2026')" in sql
    assert " OR " not in sql
    with pytest.raises(mod.AirflowException, match="must contain 1..500"):
        mod._scope_values_sql([])
    with pytest.raises(mod.AirflowException, match="must contain 1..500"):
        mod._scope_values_sql(
            [(f"WS-{index}", "2026") for index in range(501)]
        )


@pytest.mark.unit
def test_backfill_rejects_non_local_executor_and_paid_override(monkeypatch):
    mod = _load_module()
    valid = {"scopes": ["WS-252-2=2526"], "game_ids": []}
    monkeypatch.setenv("AIRFLOW__CORE__EXECUTOR", "CeleryExecutor")
    with pytest.raises(mod.AirflowException, match="requires.*LocalExecutor"):
        mod.validate_backfill_params(params=valid)

    monkeypatch.setenv("AIRFLOW__CORE__EXECUTOR", "LocalExecutor")
    with pytest.raises(mod.AirflowException, match="permanently direct-only"):
        mod.validate_backfill_params(params={**valid, "direct_only": False})


@pytest.mark.unit
def test_backfill_params_validate_scope_ids_and_dates():
    mod = _load_module()

    result = mod.validate_backfill_params(
        params={
            "scopes": ["WS-252-2=2526"],
            "game_ids": [30, 10, 30],
            "queue_id": "migration-2026",
            "date_from": "2025-08-01",
            "date_to": "2026-05-31",
        }
    )

    assert result == {
        "scopes": ["WS-252-2=2526"],
        "game_ids": [10, 30],
        "all_catalog": False,
        "queue_id": "migration-2026",
        "plan_id": None,
        "chunk_size": 25,
        "profile_chunk_size": 200,
        "max_work_items": 100,
    }


@pytest.mark.unit
def test_backfill_can_select_every_persisted_catalog_scope():
    mod = _load_module()

    result = mod.validate_backfill_params(
        params={
            "scopes": [],
            "game_ids": [],
            "all_catalog": True,
            "queue_id": "all-adult-men",
        }
    )

    assert result["all_catalog"] is True
    assert result["scopes"] == []


@pytest.mark.unit
def test_resume_requires_exact_queue_and_plan_without_mutable_selectors():
    mod = _load_module()
    plan_id = "a" * 64

    result = mod.validate_backfill_params(
        params={"queue_id": "durable-q", "plan_id": plan_id}
    )

    assert result["queue_id"] == "durable-q"
    assert result["plan_id"] == plan_id
    with pytest.raises(mod.AirflowException, match="not mutable selectors"):
        mod.validate_backfill_params(
            params={
                "queue_id": "durable-q",
                "plan_id": plan_id,
                "scopes": ["WS-1=2026"],
            }
        )


@pytest.mark.unit
def test_explicit_game_ids_require_one_scope():
    mod = _load_module()

    with pytest.raises(mod.AirflowException, match="exactly one explicit scope"):
        mod.validate_backfill_params(
            params={
                "scopes": ["WS-1=2026", "WS-2=2026"],
                "game_ids": [1],
            }
        )


@pytest.mark.unit
def test_prepare_resume_loads_existing_plan_without_catalog_discovery(monkeypatch):
    mod = _load_module()
    from dags.scripts import run_whoscored_scraper, whoscored_ops_store

    plan_id = "a" * 64
    fake_state = SimpleNamespace(
        load_plan=lambda queue, plan: {
            "queue_id": queue,
            "plan_id": plan,
            "scopes": ["WS-1=2026"],
            "provenance": _timing_provenance(catalog_batch_id="wsc2-existing"),
        },
        plan_reference=lambda _queue, _plan: {
            "uri": "s3://ops/plan.json",
            "sha256": "b" * 64,
        },
    )
    monkeypatch.setattr(
        whoscored_ops_store.WhoScoredBackfillState,
        "from_env",
        classmethod(lambda _cls: fake_state),
    )
    monkeypatch.setattr(
        run_whoscored_scraper,
        "_new_repository",
        lambda: (_ for _ in ()).throw(AssertionError("must not discover")),
    )

    result = mod.prepare_backfill_plan(params={"queue_id": "q", "plan_id": plan_id})

    assert result["resumed"] is True
    assert result["plan_id"] == plan_id
    assert result["catalog_generation"] == "wsc2-existing"


@pytest.mark.unit
def test_all_catalog_forces_full_history_and_binds_generation(monkeypatch, tmp_path):
    mod = _load_module()
    from dags.scripts import run_whoscored_scraper, whoscored_ops_store

    catalog_snapshot = object()
    repository = SimpleNamespace(
        ensure_schema=lambda: None,
        load_catalog_generation_snapshot=lambda: (
            {
                "catalog_batch_id": "wsc2-full",
                "catalog_discovery_mode": "full_history",
            },
            catalog_snapshot,
        ),
    )
    calls = []
    scope = run_whoscored_scraper.RunnerScope.parse("WS-1=2026")
    runtime_scope = SimpleNamespace(stage_ids=tuple(range(23752, 23765)))
    monkeypatch.setattr(run_whoscored_scraper, "_new_repository", lambda: repository)
    monkeypatch.setattr(run_whoscored_scraper, "_load_runtime", lambda: object)
    monkeypatch.setattr(
        run_whoscored_scraper,
        "_run_discover",
        lambda *_args, **kwargs: (
            calls.append(kwargs["full_history"]) or SimpleNamespace(errors=[])
        ),
    )
    monkeypatch.setattr(
        run_whoscored_scraper,
        "_select_catalog_snapshot_scopes",
        lambda snapshot, *_args, **_kwargs: (
            [(scope, runtime_scope)]
            if snapshot is catalog_snapshot
            else pytest.fail("wrong catalog generation")
        ),
    )
    captured = {}

    class State:
        def create_plan(self, **kwargs):
            captured.update(kwargs)
            return {
                "plan_id": "a" * 64,
                "artifact": {"uri": "s3://ops/plan", "sha256": "b" * 64},
            }

    monkeypatch.setattr(
        whoscored_ops_store.WhoScoredBackfillState,
        "from_env",
        classmethod(lambda _cls: State()),
    )
    monkeypatch.setenv("WHOSCORED_OPS_STORE_URI", tmp_path.as_uri())
    monkeypatch.setattr(mod, "RUN_ROOT", str(tmp_path / "runs"))

    result = mod.prepare_backfill_plan(
        params={"all_catalog": True, "queue_id": "q", "scopes": []},
        dag=SimpleNamespace(dag_id="dag_backfill_whoscored"),
        run_id="manual__traffic-proof",
        ti=SimpleNamespace(
            task_id="prepare_backfill_plan", map_index=-1, try_number=1
        ),
    )

    assert result["resumed"] is False
    assert calls == [True]
    assert captured["selector"]["full_history_catalog"] is True
    assert captured["provenance"]["catalog_batch_id"] == "wsc2-full"
    assert captured["provenance"]["full_history_discovery"] is True
    assert captured["provenance"]["catalog_eligible_scope_count"] == 1
    assert captured["schedule_stage_ids"] == {
        "WS-1=2026": list(range(23752, 23765))
    }
    assert len(captured["provenance"]["catalog_eligible_scopes_sha256"]) == 64
    assert "backfill_started_at" in captured["provenance"]
    assert "backfill_deadline_at" in captured["provenance"]
    assert result["discovery_traffic_required"] is True
    assert result["discovery_traffic_evidence"]["request_count"] == 0
    assert result["discovery_traffic_evidence"]["paid_proxy_bytes"] == 0


@pytest.mark.unit
def test_discovery_requests_survive_local_loss_and_reach_terminal_traffic_dq(
    monkeypatch, tmp_path
):
    from scrapers.whoscored.transport import JsonlRequestLedger

    def run_discover(*_args, **_kwargs):
        ledger = JsonlRequestLedger(os.environ["WHOSCORED_REQUEST_LEDGER_PATH"])
        identity = {
            "dag_id": os.environ["AIRFLOW_CTX_DAG_ID"],
            "run_id": os.environ["AIRFLOW_CTX_DAG_RUN_ID"],
            "task_id": os.environ["AIRFLOW_CTX_TASK_ID"],
            "map_index": int(os.environ["AIRFLOW_CTX_MAP_INDEX"]),
            "try_number": int(os.environ["AIRFLOW_CTX_TRY_NUMBER"]),
        }
        for index, (request_bytes, response_bytes) in enumerate(((10, 90), (5, 45))):
            ledger.append(
                {
                    "event_version": "whoscored-request-v1",
                    "event_id": f"{index + 1:032x}",
                    **identity,
                    "scope": "catalog",
                    "entity": "discovery",
                    "route": "direct_http",
                    "status": "success",
                    "url": f"https://www.whoscored.com/discovery/{index}",
                    "request_bytes": request_bytes,
                    "response_bytes": response_bytes,
                    "paid_proxy_bytes": 0,
                }
            )
        return SimpleNamespace(errors=[], traffic={"paid_proxy_bytes": 0})

    mod = _configure_full_history_prepare(
        monkeypatch, tmp_path, run_discover
    )
    context = _backfill_context()
    plan_ref = mod.prepare_backfill_plan(**context)
    local_ledger = next((tmp_path / "runs").rglob("requests_*.jsonl"))
    local_ledger.unlink()

    from dags import dag_ingest_whoscored as traffic_mod

    monkeypatch.setattr(traffic_mod, "RUN_ROOT", str(tmp_path / "runs"))
    monkeypatch.setattr(
        traffic_mod, "PAID_LEDGER_PATH", str(tmp_path / "missing-paid.jsonl")
    )
    summary = mod.report_backfill_traffic(
        plan_ref=plan_ref,
        **{**context, "ti": SimpleNamespace(
            task_id="report_whoscored_backfill_traffic",
            map_index=-1,
            try_number=1,
        )},
    )

    assert plan_ref["discovery_traffic_evidence"]["request_count"] == 2
    assert plan_ref["discovery_traffic_evidence"]["wire_bytes"] == 150
    assert summary["request_count"] == 2
    assert summary["wire_bytes"] == 150
    assert summary["durable_request_ledgers"] == 1
    assert summary["paid_proxy_bytes"] == 0


@pytest.mark.unit
def test_discovery_paid_usage_fails_before_plan_and_in_terminal_dq(
    monkeypatch, tmp_path
):
    from scrapers.whoscored.transport import JsonlRequestLedger

    def run_discover(*_args, **_kwargs):
        ledger = JsonlRequestLedger(os.environ["WHOSCORED_REQUEST_LEDGER_PATH"])
        ledger.append(
            {
                "event_version": "whoscored-request-v1",
                "event_id": "f" * 32,
                "dag_id": os.environ["AIRFLOW_CTX_DAG_ID"],
                "run_id": os.environ["AIRFLOW_CTX_DAG_RUN_ID"],
                "task_id": os.environ["AIRFLOW_CTX_TASK_ID"],
                "map_index": int(os.environ["AIRFLOW_CTX_MAP_INDEX"]),
                "try_number": int(os.environ["AIRFLOW_CTX_TRY_NUMBER"]),
                "scope": "catalog",
                "entity": "discovery",
                "route": "paid_lease",
                "status": "accounted",
                "url": "https://www.whoscored.com/discovery/paid",
                "request_bytes": 23,
                "response_bytes": 100,
                "paid_proxy_bytes": 123,
            }
        )
        return SimpleNamespace(errors=[], traffic={"paid_proxy_bytes": 123})

    mod = _configure_full_history_prepare(
        monkeypatch, tmp_path, run_discover
    )
    context = _backfill_context()
    with pytest.raises(mod.AirflowException, match="used paid proxy: 123 bytes"):
        mod.prepare_backfill_plan(**context)

    from dags import dag_ingest_whoscored as traffic_mod

    monkeypatch.setattr(traffic_mod, "RUN_ROOT", str(tmp_path / "runs"))
    monkeypatch.setattr(
        traffic_mod, "PAID_LEDGER_PATH", str(tmp_path / "missing-paid.jsonl")
    )
    with pytest.raises(mod.AirflowException, match="used paid proxy: 123 bytes"):
        mod.report_backfill_traffic(**context)


@pytest.mark.unit
def test_backfill_rejects_ambiguous_catalog_selectors():
    mod = _load_module()

    with pytest.raises(mod.AirflowException, match="mutually exclusive"):
        mod.validate_backfill_params(
            params={
                "scopes": ["WS-252-2=2526"],
                "game_ids": [],
                "all_catalog": True,
                "queue_id": "ambiguous",
            }
        )


@pytest.mark.unit
def test_backfill_batch_validation_allows_durable_incomplete_plan(monkeypatch):
    mod = _load_module()
    from dags.scripts import whoscored_ops_store

    progress = {
        "status": "running",
        "queue_id": "q",
        "plan_id": "a" * 64,
        "next_work_items": 100,
        "remaining_request_units": 100,
    }
    fake_state = SimpleNamespace(
        advance_batch=lambda _queue, _plan, batch_id: progress,
        load_plan=lambda _queue, _plan: {"provenance": _timing_provenance()},
    )
    monkeypatch.setattr(
        whoscored_ops_store.WhoScoredBackfillState,
        "from_env",
        classmethod(lambda _cls: fake_state),
    )

    assert (
        mod.validate_backfill_batch(
            plan_ref={"queue_id": "q", "plan_id": "a" * 64},
            run_id="scheduled__one",
        )["status"]
        == "running"
    )


@pytest.mark.unit
def test_expired_plan_cannot_materialize_source_work(monkeypatch):
    mod = _load_module()
    from dags.scripts import whoscored_ops_store

    expired = datetime.now(timezone.utc) - timedelta(days=1)

    class State:
        def load_plan(self, _queue, _plan):
            return {
                "provenance": {
                    "backfill_started_at": (expired - timedelta(days=29)).isoformat(),
                    "backfill_deadline_at": expired.isoformat(),
                }
            }

        def create_batch(self, *_args, **_kwargs):
            pytest.fail("expired plan must fail before batch/source work")

    monkeypatch.setattr(
        whoscored_ops_store.WhoScoredBackfillState,
        "from_env",
        classmethod(lambda _cls: State()),
    )

    with pytest.raises(mod.AirflowException, match="deadline expired"):
        mod.build_backfill_commands(
            plan_ref={"queue_id": "q", "plan_id": "a" * 64},
            run_id="scheduled__expired",
        )


@pytest.mark.unit
def test_capacity_breach_cannot_materialize_source_work(monkeypatch):
    mod = _load_module()
    from dags.scripts import whoscored_ops_store

    class State:
        def load_plan(self, _queue, _plan):
            return {"provenance": _timing_provenance()}

        def checkpoint_progress(self, _queue, _plan):
            return {
                "remaining_request_units": 3_000_000,
                "estimated_completed_request_units": 0,
                "actual_completed_request_units": 0,
                "schedule_stage_cardinality_drifts": 0,
            }

        def create_batch(self, *_args, **_kwargs):
            pytest.fail("capacity breach must fail before batch/source work")

    monkeypatch.setattr(
        whoscored_ops_store.WhoScoredBackfillState,
        "from_env",
        classmethod(lambda _cls: State()),
    )

    with pytest.raises(mod.AirflowException, match="30-day backfill SLO failed"):
        mod.build_backfill_commands(
            plan_ref={"queue_id": "q", "plan_id": "a" * 64},
            run_id="scheduled__capacity-breach",
        )


@pytest.mark.unit
def test_capacity_breach_stops_automatic_continuation(monkeypatch):
    mod = _load_module()
    from dags.scripts import whoscored_ops_store

    progress = {
        "status": "running",
        "next_work_items": 100,
        "successful_receipts": 7_472,
        "remaining_request_units": 3_000_000,
        "estimated_completed_request_units": 1_118_530,
        "actual_completed_request_units": 1_118_530,
        "schedule_stage_cardinality_drifts": 0,
    }
    state = SimpleNamespace(
        checkpoint_progress=lambda _queue, _plan: dict(progress),
        load_plan=lambda _queue, _plan: {"provenance": _timing_provenance()},
    )
    monkeypatch.setattr(
        whoscored_ops_store.WhoScoredBackfillState,
        "from_env",
        classmethod(lambda _cls: state),
    )

    with pytest.raises(
        mod.AirflowException,
        match="continuation stopped by 30-day capacity preflight",
    ):
        mod.schedule_backfill_continuation(
            plan_ref={"queue_id": "q", "plan_id": "a" * 64},
            run_id="scheduled__capacity-breach",
        )


@pytest.mark.unit
def test_global_historical_dq_is_deferred_until_plan_complete(monkeypatch):
    mod = _load_module()
    from dags.scripts import whoscored_ops_store

    fake_state = SimpleNamespace(
        checkpoint_progress=lambda _queue, _plan: {"status": "running"},
        load_plan=lambda _queue, _plan: {"selector": {"all_catalog": True}},
    )
    monkeypatch.setattr(
        whoscored_ops_store.WhoScoredBackfillState,
        "from_env",
        classmethod(lambda _cls: fake_state),
    )
    monkeypatch.setattr(
        mod,
        "_global_historical_integrity_summary",
        lambda: (_ for _ in ()).throw(AssertionError("DQ must be deferred")),
    )

    result = mod.validate_global_historical_dq(
        plan_ref={"queue_id": "q", "plan_id": "a" * 64}
    )
    assert result["status"] == "deferred"


@pytest.mark.unit
def test_frozen_dq_population_is_exact_content_addressed_receipt_state(
    monkeypatch, tmp_path
):
    mod = _load_module()
    from dags.scripts.whoscored_ops_store import WhoScoredBackfillState

    monkeypatch.setenv("WHOSCORED_OPS_STORE_URI", tmp_path.as_uri())
    state = WhoScoredBackfillState.from_env()
    plan = state.create_plan(
        queue_id="q",
        selector={},
        scopes=["WS-1=2026"],
        schedule_stage_ids={"WS-1=2026": [1]},
    )
    schedule = state.pending_work("q", plan["plan_id"])[0]
    state.append_receipt(
        queue_id="q",
        plan_id=plan["plan_id"],
        work_item=schedule,
        outcome={
            "candidate_game_ids": [10, 20],
            "preview_game_ids": [20],
            "source_stage_ids": [1],
            "source_request_attempts": 70,
            "estimated_request_units": 70,
            "actual_request_units": 70,
        },
    )
    for item in state.pending_work("q", plan["plan_id"]):
        state.append_receipt(
            queue_id="q",
            plan_id=plan["plan_id"],
            work_item=item,
            outcome={"game_ids": item["game_ids"]},
        )
    roster = state.pending_work("q", plan["plan_id"])[0]
    state.append_receipt(
        queue_id="q",
        plan_id=plan["plan_id"],
        work_item=roster,
        outcome={"profile_player_ids": [7, 8]},
    )
    for item in state.pending_work("q", plan["plan_id"]):
        state.append_receipt(
            queue_id="q",
            plan_id=plan["plan_id"],
            work_item=item,
            outcome={
                "player_ids": item["player_ids"],
                "attempted": len(item["player_ids"]),
                "succeeded": len(item["player_ids"]),
            },
        )

    progress = state.checkpoint_progress("q", plan["plan_id"])
    population = mod._frozen_dq_population(state, plan, progress)

    assert progress["status"] == "complete"
    assert population["counts"] == {
        "scopes": 1,
        "matches": 2,
        "previews": 1,
        "players": 2,
        "stages": 1,
    }
    assert [item["game_id"] for item in population["matches"]] == [10, 20]
    assert [item["preview_required"] for item in population["matches"]] == [
        False,
        True,
    ]
    assert population["player_ids"] == [7, 8]
    assert population["scope_stages"] == [
        {
            "scope": "WS-1=2026",
            "league": "WS-1",
            "season": "2026",
            "stage_ids": [1],
        }
    ]
    assert population["population_sha256"] == population["artifact"]["sha256"]
    assert state.store.read_content_addressed_json(
        population["artifact"]["key"],
        expected_sha256=population["population_sha256"],
    )["matches"] == population["matches"]


@pytest.mark.unit
def test_frozen_match_order_handles_live_catalog_prefix_league_ids():
    mod = _load_module()
    rows = [
        {"league": "WS-252-70", "season": "2026", "game_id": 2},
        {"league": "WS-252-7", "season": "2026", "game_id": 1},
        {"league": "WS-252-96", "season": "2026", "game_id": 4},
        {"league": "WS-252-9", "season": "2026", "game_id": 3},
        {"league": "WS-81-673", "season": "2026", "game_id": 6},
        {"league": "WS-81-6", "season": "2026", "game_id": 5},
    ]

    ordered = sorted(rows, key=mod._frozen_match_identity_key)

    assert [row["league"] for row in ordered] == [
        "WS-252-7",
        "WS-252-70",
        "WS-252-9",
        "WS-252-96",
        "WS-81-6",
        "WS-81-673",
    ]


@pytest.mark.unit
def test_frozen_dq_population_rejects_duplicate_valid_work_chunks(
    monkeypatch, tmp_path
):
    mod = _load_module()
    from dags.scripts.whoscored_ops_store import WhoScoredBackfillState

    monkeypatch.setenv("WHOSCORED_OPS_STORE_URI", tmp_path.as_uri())
    state = WhoScoredBackfillState.from_env()
    plan = state.create_plan(
        queue_id="duplicate",
        selector={},
        scopes=["WS-1=2026"],
        schedule_stage_ids={"WS-1=2026": [1]},
    )
    schedule = state.pending_work("duplicate", plan["plan_id"])[0]
    state.append_receipt(
        queue_id="duplicate",
        plan_id=plan["plan_id"],
        work_item=schedule,
        outcome={
            "candidate_game_ids": [10],
            "preview_game_ids": [],
            "source_stage_ids": [1],
            "source_request_attempts": 70,
            "estimated_request_units": 70,
            "actual_request_units": 70,
        },
    )
    expected = state.pending_work("duplicate", plan["plan_id"])[0]
    state.append_receipt(
        queue_id="duplicate",
        plan_id=plan["plan_id"],
        work_item=expected,
        outcome={"game_ids": [10]},
    )
    duplicate = state._match_work("WS-1=2026", [10], [], 1)
    state.append_receipt(
        queue_id="duplicate",
        plan_id=plan["plan_id"],
        work_item=duplicate,
        outcome={"game_ids": [10]},
    )
    roster = state.pending_work("duplicate", plan["plan_id"])[0]
    state.append_receipt(
        queue_id="duplicate",
        plan_id=plan["plan_id"],
        work_item=roster,
        outcome={"profile_player_ids": []},
    )
    progress = state.checkpoint_progress("duplicate", plan["plan_id"])

    assert progress["status"] == "complete"
    with pytest.raises(mod.AirflowException, match="do not exactly match"):
        mod._frozen_dq_population(state, plan, progress)


@pytest.mark.unit
def test_frozen_dq_population_rejects_same_cardinality_stage_identity_drift(
    monkeypatch, tmp_path
):
    mod = _load_module()
    from dags.scripts.whoscored_ops_store import WhoScoredBackfillState

    monkeypatch.setenv("WHOSCORED_OPS_STORE_URI", tmp_path.as_uri())
    state = WhoScoredBackfillState.from_env()
    plan = state.create_plan(
        queue_id="stage-drift",
        selector={},
        scopes=["WS-1=2026"],
        schedule_stage_ids={"WS-1=2026": [1]},
    )
    schedule = state.pending_work("stage-drift", plan["plan_id"])[0]
    state.append_receipt(
        queue_id="stage-drift",
        plan_id=plan["plan_id"],
        work_item=schedule,
        outcome={
            "candidate_game_ids": [],
            "preview_game_ids": [],
            "source_stage_ids": [2],
            "source_request_attempts": 70,
            "estimated_request_units": 70,
            "actual_request_units": 70,
        },
    )
    roster = state.pending_work("stage-drift", plan["plan_id"])[0]
    state.append_receipt(
        queue_id="stage-drift",
        plan_id=plan["plan_id"],
        work_item=roster,
        outcome={"profile_player_ids": []},
    )
    progress = state.checkpoint_progress("stage-drift", plan["plan_id"])

    assert progress["status"] == "complete"
    assert progress["schedule_stage_cardinality_drifts"] == 0
    with pytest.raises(mod.AirflowException, match="stage identity drift"):
        mod._frozen_dq_population(state, plan, progress)


@pytest.mark.unit
def test_final_catalog_proof_rejects_same_count_payload_mutation(monkeypatch):
    mod = _load_module()
    from dags.scripts import run_whoscored_scraper as runner
    from scrapers.whoscored.repository import catalog_payload_sha256

    rows = {
        "competitions": [{"competition_id": "WS-1", "eligibility": "included"}],
        "seasons": [
            {
                "competition_id": "WS-1",
                "season_id": "2026",
                "eligibility": "included",
            }
        ],
        "stages": [{"stage_id": 7, "stage_name": "Original"}],
    }

    class Catalog:
        def to_rows(self):
            return rows

        def eligible_scopes(self, *, active_only=False):
            assert active_only is False
            return [SimpleNamespace(scope=SimpleNamespace(spec="WS-1=2026"))]

    class Repository:
        def load_discovered_catalog(self, *, batch_id):
            assert batch_id == "wsc2-generation"
            return Catalog()

    monkeypatch.setattr(runner, "_new_repository", Repository)
    scope_digest = hashlib.sha256(b"WS-1=2026\n").hexdigest()
    plan = {
        "selector": {"all_catalog": True},
        "scopes": ["WS-1=2026"],
        "provenance": {
            "catalog_batch_id": "wsc2-generation",
            "catalog_payload_sha256": catalog_payload_sha256(rows),
            "catalog_eligible_scope_count": 1,
            "catalog_eligible_scopes_sha256": scope_digest,
        },
    }

    assert mod._catalog_snapshot_scope_summary(plan)["catalog_payload_sha256"]
    rows["stages"][0]["stage_name"] = "Tampered"
    with pytest.raises(mod.AirflowException, match="catalog payload proof failed"):
        mod._catalog_snapshot_scope_summary(plan)


@pytest.mark.unit
def test_frozen_scope_feed_contract_uses_receipt_stage_ids(monkeypatch):
    mod = _load_module()
    from dags import dag_ingest_whoscored as daily
    import utils.silver_tasks as silver_tasks

    expected = daily._expected_feed_state_keys([7])
    payload = json.dumps(
        {"__feeds__": {key: "empty" for key in sorted(expected)}},
        sort_keys=True,
    )

    class Cursor:
        def execute(self, query):
            assert "row_kind='scope'" in query
            assert "FOR VERSION AS OF 123" in query
            assert "VALUES ('WS-" not in query
            assert "JOIN eligible e" in query

        def fetchall(self):
            return [("WS-1", "2026", payload)]

        def close(self):
            return None

    class Connection:
        def cursor(self):
            return Cursor()

        def close(self):
            return None

    monkeypatch.setattr(silver_tasks, "_get_trino_connection", Connection)
    summary = mod._frozen_scope_feed_integrity(
        {
            "scope_stages": [
                {
                    "scope": "WS-1=2026",
                    "league": "WS-1",
                    "season": "2026",
                    "stage_ids": [7],
                }
            ]
        },
        staged_relation=_staged_population_ref()["staged_relation"],
    )

    assert summary["frozen_scope_stage_count"] == 1
    assert summary["expected_feed_state_count"] == 68
    assert summary["actual_feed_state_count"] == 68
    assert summary["missing_feed_state_count"] == 0


@pytest.mark.unit
def test_frozen_scope_feed_contract_is_set_based_and_round_trips_full_catalog(
    monkeypatch,
):
    mod = _load_module()
    from dags import dag_ingest_whoscored as daily
    import utils.silver_tasks as silver_tasks

    queries = []

    class Cursor:
        query = ""

        def execute(self, query):
            self.query = query
            queries.append(query)

        def fetchall(self):
            return [
                (
                    f"WS-{index}",
                    "2026",
                    json.dumps(
                        {
                            "__feeds__": {
                                key: "empty"
                                for key in daily._expected_feed_state_keys(
                                    [int(index) + 1]
                                )
                            }
                        }
                    ),
                )
                for index in range(scope_count)
            ]

        def close(self):
            return None

    class Connection:
        def cursor(self):
            return Cursor()

        def close(self):
            return None

    monkeypatch.setattr(silver_tasks, "_get_trino_connection", Connection)
    scope_count = 1_201
    summary = mod._frozen_scope_feed_integrity(
        {
            "scope_stages": [
                {
                    "scope": f"WS-{index}=2026",
                    "league": f"WS-{index}",
                    "season": "2026",
                    "stage_ids": [index + 1],
                }
                for index in range(scope_count)
            ]
        },
        staged_relation=_staged_population_ref()["staged_relation"],
    )

    assert len(queries) == 1
    assert "VALUES ('WS-" not in queries[0]
    assert "FOR VERSION AS OF 123" in queries[0]
    assert summary["frozen_scope_stage_count"] == scope_count
    assert summary["expected_feed_state_count"] == scope_count * 68
    assert summary["actual_feed_state_count"] == scope_count * 68
    assert summary["missing_scope_feed_manifests"] == 0


@pytest.mark.unit
def test_frozen_scope_feed_contract_fails_on_incomplete_identity_round_trip(
    monkeypatch,
):
    mod = _load_module()
    import utils.silver_tasks as silver_tasks

    class Cursor:
        def execute(self, _query):
            return None

        def fetchall(self):
            return [("WS-1", "2026", None)]

        def close(self):
            return None

    class Connection:
        def cursor(self):
            return Cursor()

        def close(self):
            return None

    monkeypatch.setattr(silver_tasks, "_get_trino_connection", Connection)

    with pytest.raises(mod.AirflowException, match="round-trip every identity"):
        mod._frozen_scope_feed_integrity(
            {
                "scope_stages": [
                    {
                        "league": "WS-1",
                        "season": "2026",
                        "stage_ids": [1],
                    },
                    {
                        "league": "WS-2",
                        "season": "2026",
                        "stage_ids": [2],
                    },
                ]
            },
            staged_relation=_staged_population_ref()["staged_relation"],
        )


@pytest.mark.unit
def test_long_continuation_run_ids_have_distinct_paths():
    mod = _load_module()
    prefix = "manual__" + "q" * 180
    first = mod._work_output_path(
        {"dag_id": "dag_backfill_whoscored", "run_id": prefix + "__one"},
        "work",
    )
    second = mod._work_output_path(
        {"dag_id": "dag_backfill_whoscored", "run_id": prefix + "__two"},
        "work",
    )

    assert first != second
    assert len(first.parent.name) <= 120
    assert len(second.parent.name) <= 120


@pytest.mark.unit
def test_backfill_jinja_writer_and_python_reader_share_run_directory():
    from jinja2 import Environment

    mod = _load_module()
    context = {
        "dag_id": "dag_backfill_whoscored",
        "run_id": "manual__" + "queue:" * 40 + "+00:00",
    }
    environment = Environment()
    environment.filters.update(mod.dag._dag_kwargs["user_defined_filters"])
    rendered = environment.from_string(mod._RUN_DIR_TEMPLATE).render(
        dag=SimpleNamespace(dag_id=context["dag_id"]),
        run_id=context["run_id"],
    )

    assert str(mod._work_output_path(context, "work").parent) == rendered
    assert mod._RUN_DIR_TEMPLATE in mod._TASK_ENV["WHOSCORED_REQUEST_LEDGER_PATH"]
    assert "stable_safe_token" in mod._RUN_DIR_TEMPLATE


@pytest.mark.unit
def test_global_historical_dq_fails_on_any_uncovered_or_parity_gap(monkeypatch):
    mod = _load_module()
    from dags.scripts import whoscored_frozen_dq, whoscored_ops_store

    fake_state = SimpleNamespace(
        checkpoint_progress=lambda _queue, _plan: {"status": "complete"},
        load_plan=lambda _queue, _plan: {
            "selector": {"all_catalog": True},
            "scopes": ["WS-1=2026"],
            "provenance": _timing_provenance(full_history_discovery=True),
        },
    )
    monkeypatch.setattr(
        whoscored_ops_store.WhoScoredBackfillState,
        "from_env",
        classmethod(lambda _cls: fake_state),
    )
    summary = {
        "eligible_scopes": 1,
        "uncovered_eligible_scopes": 1,
        "completed_matches": 100_000,
        "uncovered_completed_matches": 0,
        "parse_failed_matches": 0,
        "retryable_matches": 0,
        "terminal_matches": 0,
        "unproven_not_available_matches": 0,
        "failed_previews": 0,
        "failed_profiles": 0,
        "dataset_count": 25,
        "dataset_parity_mismatches": 0,
        "mismatched_datasets": [],
    }
    monkeypatch.setattr(
        mod,
        "_global_historical_integrity_summary",
        lambda _scopes=None, **_kwargs: summary,
    )
    monkeypatch.setattr(
        mod,
        "_catalog_snapshot_scope_summary",
        lambda _plan: {"plan_scopes": 1},
    )
    monkeypatch.setattr(
        mod,
        "_frozen_dq_population",
        lambda *_args: {
            "counts": {"matches": 100_000, "previews": 0, "players": 0},
            "population_sha256": "f" * 64,
            "artifact": {"uri": "s3://ops/frozen.json"},
        },
    )
    monkeypatch.setattr(
        mod, "_frozen_scope_feed_integrity", lambda _value, **_kwargs: {}
    )
    monkeypatch.setattr(
        whoscored_frozen_dq,
        "stage_frozen_population",
        lambda _value: {"population_sha256": "f" * 64},
    )

    with pytest.raises(mod.AirflowException, match="global historical DQ failed"):
        mod.validate_global_historical_dq(
            plan_ref={"queue_id": "q", "plan_id": "a" * 64}
        )


@pytest.mark.unit
def test_global_historical_summary_covers_25_datasets_and_full_history(monkeypatch):
    mod = _load_module()
    queries = []

    class Cursor:
        query = ""

        def execute(self, query):
            self.query = query
            queries.append(query)

        def fetchall(self):
            if "WITH eligible AS" in self.query:
                return [[0] * 17]
            if "COUNT_IF(m.expected_rows" in self.query:
                return [[0, 0, 0, 0]]
            if "whoscored_player_profile_versions p" in self.query:
                return [[0] * 6]
            return [[0, 0, 0]]

        def close(self):
            return None

    class Connection:
        def cursor(self):
            return Cursor()

        def close(self):
            return None

    import utils.silver_tasks as silver_tasks

    monkeypatch.setattr(silver_tasks, "_get_trino_connection", Connection)
    summary = mod._global_historical_integrity_summary()

    assert summary["dataset_count"] == 25
    assert summary["dataset_parity_mismatches"] == 0
    assert "INTERVAL '7' DAY" not in queries[0]
    assert "availability_version = 'whoscored-match-availability-v2'" in queries[0]
    assert "PARTITION BY m.league, m.season, m.game_id" in queries[0]
    assert "availability_version IS DISTINCT FROM" in queries[0]
    assert "p.state = 'terminal'" in queries[0]
    assert "LEFT JOIN valid_profile" in queries[0]
    assert "JOIN completed c" in queries[0]
    assert "LEFT JOIN valid_preview" in queries[0]
    assert "state='terminal'" in queries[0]
    # One bounded coverage query plus split parity queries avoids Trino's
    # production query-stage ceiling.
    assert len(queries) == 25


@pytest.mark.unit
def test_frozen_historical_summary_skips_mutable_population_scans(monkeypatch):
    mod = _load_module()
    from dags.scripts import whoscored_frozen_dq
    from scrapers.whoscored.repository import (
        MATCH_DATASET_TABLES,
        PREVIEW_DATASET_TABLES,
    )

    queries = []

    class Cursor:
        query = ""

        def execute(self, query):
            self.query = query
            queries.append(query)

        def fetchall(self):
            if "scope_success AS" in self.query:
                return [[1, *([0] * 16)]]
            if "COUNT_IF(m.expected_rows" in self.query:
                return [[0, 0, 0, 0]]
            return [[0, 0, 0]]

        def close(self):
            return None

    class Connection:
        def cursor(self):
            return Cursor()

        def close(self):
            return None

    match_parity = {
        table: {"manifest": 0, "physical": 0, "current": 0}
        for table in (
            *MATCH_DATASET_TABLES.values(),
            *PREVIEW_DATASET_TABLES.values(),
            "whoscored_player_profile_versions",
            "whoscored_player_stage_participations",
        )
    }
    monkeypatch.setattr(
        whoscored_frozen_dq,
        "frozen_historical_integrity",
        lambda *_args, **_kwargs: ({}, match_parity),
    )
    import utils.silver_tasks as silver_tasks

    monkeypatch.setattr(silver_tasks, "_get_trino_connection", Connection)
    summary = mod._global_historical_integrity_summary(
        ["WS-1=2026"],
        catalog_batch_id="wsc2-generation",
        frozen_population=_staged_population_ref(),
    )

    joined = "\n".join(queries)
    assert summary["eligible_scopes"] == 1
    assert summary["dataset_count"] == 25
    assert len(queries) == 14
    assert "whoscored_match_ingest" not in joined
    assert "whoscored_preview_ingest" not in joined
    assert "whoscored_profile_ingest" not in joined


@pytest.mark.unit
def test_full_catalog_frozen_scope_dq_is_set_based_and_aggregates_parity(
    monkeypatch,
):
    mod = _load_module()
    from dags.scripts import whoscored_frozen_dq
    from scrapers.whoscored.repository import (
        MATCH_DATASET_TABLES,
        PREVIEW_DATASET_TABLES,
    )
    import utils.silver_tasks as silver_tasks

    queries = []

    class Cursor:
        query = ""
        parity_gap = False
        parity_compensation = False
        stage_scope_count = 0

        def execute(self, query):
            self.query = query
            queries.append(query)

        def fetchall(self):
            import re

            scope_count = len(
                re.findall(r"\('WS-\d+', '2026'\)", self.query)
            )
            if "row_kind='scope'" in self.query:
                scope_count = self.stage_scope_count
            if "scope_success AS" in self.query:
                return [[scope_count, 0, *([0] * 15)]]
            if "json_extract_scalar" in self.query:
                physical = scope_count
                owner_mismatches = 0
                if self.parity_gap and "whoscored_schedule d" in self.query:
                    physical = max(0, physical - 1)
                    owner_mismatches = 1
                if (
                    self.parity_compensation
                    and "whoscored_schedule d" in self.query
                ):
                    owner_mismatches = 2
                return [[
                    scope_count,
                    physical,
                    scope_count,
                    owner_mismatches,
                ]]
            return [[0, 0, 0]]

        def close(self):
            return None

    class Connection:
        def cursor(self):
            return Cursor()

        def close(self):
            return None

    frozen_entity_parity = {
        table: {"manifest": 0, "physical": 0, "current": 0}
        for table in (
            *MATCH_DATASET_TABLES.values(),
            *PREVIEW_DATASET_TABLES.values(),
            "whoscored_player_profile_versions",
            "whoscored_player_stage_participations",
        )
    }
    monkeypatch.setattr(
        whoscored_frozen_dq,
        "frozen_historical_integrity",
        lambda *_args, **_kwargs: ({}, frozen_entity_parity),
    )
    monkeypatch.setattr(silver_tasks, "_get_trino_connection", Connection)

    scopes = [f"WS-{index}=2026" for index in range(7_472)]
    Cursor.stage_scope_count = len(scopes)
    summary = mod._global_historical_integrity_summary(
        scopes,
        catalog_batch_id="wsc2-generation",
        frozen_population=_staged_population_ref(),
    )

    assert summary["eligible_scopes"] == len(scopes)
    assert summary["dataset_count"] == 25
    assert summary["dataset_parity_mismatches"] == 0
    assert len(queries) == 14
    assert all("VALUES ('WS-" not in query for query in queries)
    assert all("FOR VERSION AS OF 123" in query for query in queries[:11])
    assert all(") OR (" not in query for query in queries)
    assert max(map(len, queries)) < 50_000

    queries.clear()
    Cursor.parity_gap = True
    Cursor.stage_scope_count = 501
    failed = mod._global_historical_integrity_summary(
        scopes[:501],
        catalog_batch_id="wsc2-generation",
        frozen_population=_staged_population_ref(),
    )

    assert failed["dataset_parity_mismatches"] == 1
    assert failed["mismatched_datasets"] == ["whoscored_schedule"]

    queries.clear()
    Cursor.parity_gap = False
    Cursor.parity_compensation = True
    Cursor.stage_scope_count = 501
    compensated = mod._global_historical_integrity_summary(
        scopes[:501],
        catalog_batch_id="wsc2-generation",
        frozen_population=_staged_population_ref(),
    )

    assert compensated["dataset_parity_mismatches"] == 1
    assert compensated["mismatched_datasets"] == ["whoscored_schedule"]


@pytest.mark.unit
def test_scoped_historical_dq_runs_for_explicit_plan(monkeypatch):
    mod = _load_module()
    from dags.scripts import whoscored_frozen_dq, whoscored_ops_store
    import utils.silver_tasks as silver_tasks

    fake_state = SimpleNamespace(
        checkpoint_progress=lambda _queue, _plan: {"status": "complete"},
        load_plan=lambda _queue, _plan: {
            "selector": {"all_catalog": False},
            "scopes": ["WS-1=2026"],
            "provenance": _timing_provenance(),
        },
    )
    monkeypatch.setattr(
        whoscored_ops_store.WhoScoredBackfillState,
        "from_env",
        classmethod(lambda _cls: fake_state),
    )
    captured = []
    population = {
        "counts": {"matches": 0, "previews": 0, "players": 0},
        "population_sha256": "f" * 64,
        "artifact": {"uri": "s3://ops/frozen.json"},
    }
    summary = {
        "dataset_count": 25,
        "dataset_parity_mismatches": 0,
        "mismatched_datasets": [],
    }

    def historical(scopes=None, **kwargs):
        captured.append(
            (scopes, kwargs["catalog_batch_id"], kwargs["frozen_population"])
        )
        return {
            "eligible_scopes": 1,
            "completed_matches": 0,
            "required_previews": 0,
            "frozen_profile_players": 0,
            **summary,
        }

    monkeypatch.setattr(mod, "_global_historical_integrity_summary", historical)
    monkeypatch.setattr(
        mod,
        "_catalog_snapshot_scope_summary",
        lambda _plan: {"plan_scopes": 1},
    )
    monkeypatch.setattr(mod, "_frozen_dq_population", lambda *_args: population)
    monkeypatch.setattr(
        mod, "_frozen_scope_feed_integrity", lambda _value, **_kwargs: {}
    )
    staged_relation = {"population_sha256": "f" * 64, "snapshot_id": 123}
    monkeypatch.setattr(
        whoscored_frozen_dq,
        "stage_frozen_population",
        lambda _value: staged_relation,
    )
    monkeypatch.setattr(
        whoscored_frozen_dq,
        "cleanup_staged_frozen_populations",
        lambda _cur, **_kwargs: 0,
    )

    class Cursor:
        def close(self):
            return None

    class Connection:
        def cursor(self):
            return Cursor()

        def close(self):
            return None

    monkeypatch.setattr(silver_tasks, "_get_trino_connection", Connection)

    result = mod.validate_global_historical_dq(
        plan_ref={"queue_id": "q", "plan_id": "a" * 64}
    )

    assert result["status"] == "success"
    assert captured == [
        (
            ["WS-1=2026"],
            "wsc2-generation",
            {"staged_relation": staged_relation},
        )
    ]
    assert result["expired_frozen_dq_partitions"] == 0


@pytest.mark.unit
def test_continuation_is_deterministic_and_idempotent(monkeypatch):
    mod = _load_module()
    from airflow import exceptions as airflow_exceptions
    from dags.scripts import whoscored_ops_store

    progress = {
        "status": "running",
        "next_work_items": 100,
        "successful_receipts": 100,
        "remaining_request_units": 100,
    }
    fake_state = SimpleNamespace(
        checkpoint_progress=lambda _queue, _plan: dict(progress),
        load_plan=lambda _queue, _plan: {"provenance": _timing_provenance()},
    )
    monkeypatch.setattr(
        whoscored_ops_store.WhoScoredBackfillState,
        "from_env",
        classmethod(lambda _cls: fake_state),
    )

    class AlreadyExists(Exception):
        pass

    monkeypatch.setattr(
        airflow_exceptions, "DagRunAlreadyExists", AlreadyExists, raising=False
    )
    created = {}

    def trigger_dag(**kwargs):
        run_id = kwargs["run_id"]
        if run_id in created:
            raise AlreadyExists(run_id)
        created[run_id] = dict(kwargs["conf"])

    class DagRun:
        @staticmethod
        def find(*, dag_id, run_id):
            assert dag_id == "dag_backfill_whoscored"
            return [SimpleNamespace(conf=created[run_id])]

    api_mod = types.ModuleType("airflow.api")
    common_mod = types.ModuleType("airflow.api.common")
    trigger_mod = types.ModuleType("airflow.api.common.trigger_dag")
    trigger_mod.trigger_dag = trigger_dag
    dagrun_mod = types.ModuleType("airflow.models.dagrun")
    dagrun_mod.DagRun = DagRun
    monkeypatch.setitem(sys.modules, "airflow.api", api_mod)
    monkeypatch.setitem(sys.modules, "airflow.api.common", common_mod)
    monkeypatch.setitem(sys.modules, "airflow.api.common.trigger_dag", trigger_mod)
    monkeypatch.setitem(sys.modules, "airflow.models.dagrun", dagrun_mod)
    ref = {"queue_id": "q", "plan_id": "a" * 64}

    dag_run = SimpleNamespace(run_id="manual__parent", conf={})
    context = {
        "run_id": "manual__parent",
        "dag_run": dag_run,
        "ti": SimpleNamespace(try_number=2),
    }
    first = mod.schedule_backfill_continuation(plan_ref=ref, **context)
    second = mod.schedule_backfill_continuation(plan_ref=ref, **context)

    assert first["continuation"] == "scheduled"
    assert second["continuation"] == "already_scheduled"
    assert first["continuation_run_id"] == second["continuation_run_id"]
    assert list(created.values()) == [
        {
            "queue_id": "q",
            "plan_id": "a" * 64,
            "direct_only": True,
            "require_zero_paid": True,
            "parent_run_id": "manual__parent",
            "start_receipts": 100,
            "no_progress_runs": 0,
        }
    ]


@pytest.mark.unit
def test_backfill_gate_cannot_hide_failed_producer():
    mod = _load_module()
    instances = [
        SimpleNamespace(task_id="run_whoscored_backfill", state="failed"),
        SimpleNamespace(task_id="validate_whoscored_backfill", state="success"),
        SimpleNamespace(task_id="final_success_gate", state="running"),
    ]
    dag_run = SimpleNamespace(get_task_instances=lambda: instances)

    with pytest.raises(mod.AirflowException, match="run_whoscored_backfill=failed"):
        mod.enforce_backfill_gate(
            dag_run=dag_run,
            ti=SimpleNamespace(task_id="final_success_gate"),
        )
