from __future__ import annotations

import importlib
import json
import sys
from datetime import datetime, timezone
from types import SimpleNamespace

import pytest


def _reload_owner(monkeypatch, *, isolated: bool):
    from airflow.operators.python import PythonOperator

    PythonOperator._instances.clear()
    if isolated:
        monkeypatch.setenv("FOTMOB_ISOLATED_STACK", "1")
    else:
        monkeypatch.delenv("FOTMOB_ISOLATED_STACK", raising=False)
    sys.modules.pop("dag_orchestrate_fotmob", None)
    sys.modules.pop("dags.dag_orchestrate_fotmob", None)
    return importlib.import_module("dag_orchestrate_fotmob")


def _task(task_id):
    from airflow.operators.python import PythonOperator

    return next(task for task in PythonOperator._instances if task.task_id == task_id)


def test_shared_default_does_not_materialize_owner(monkeypatch):
    module = _reload_owner(monkeypatch, isolated=False)
    from airflow.operators.python import PythonOperator

    assert module.dag is None
    assert PythonOperator._instances == []


def test_isolated_owner_is_the_one_fair_active_schedule(monkeypatch):
    module = _reload_owner(monkeypatch, isolated=True)

    assert module.dag.dag_id == "dag_orchestrate_fotmob"
    assert module.dag.schedule == "*/5 * * * *"
    assert module.dag._dag_kwargs["max_active_runs"] == 1
    assert module.dag._dag_kwargs["is_paused_upon_creation"] is True
    assert module.dag._dag_kwargs["catchup"] is False
    assert module.dag._dag_kwargs["render_template_as_native_obj"] is True

    assert _task("choose_fotmob_lane").upstream_task_ids == {
        "attest_isolated_runtime"
    }
    assert _task("initialize_fotmob_publication").upstream_task_ids == {
        "choose_fotmob_lane"
    }
    assert _task("initialize_fotmob_publication").python_callable is (
        module.initialize_admitted_publication
    )
    assert _task("initialize_fotmob_publication")._init_kwargs[
        "ignore_downstream_trigger_rules"
    ] is True
    trigger = _task("trigger_fotmob_ingest")
    assert trigger.upstream_task_ids == {"initialize_fotmob_publication"}
    assert trigger._init_kwargs["trigger_dag_id"] == "dag_ingest_fotmob"
    assert trigger._init_kwargs["wait_for_completion"] is True
    conf = trigger._init_kwargs["conf"]
    assert set(conf) == {*module._CONF_KEYS, "fotmob_publication"}
    assert all("choose_fotmob_lane" in conf[key] for key in module._CONF_KEYS)
    assert _task("finalize_fotmob_publication").upstream_task_ids == {
        "trigger_fotmob_ingest"
    }
    assert _task("advance_fotmob_scheduler_state").upstream_task_ids == {
        "finalize_fotmob_publication"
    }


def test_only_automatic_owner_keeps_an_isolated_schedule(monkeypatch):
    monkeypatch.setenv("FOTMOB_ISOLATED_STACK", "1")
    modules = {}
    for name in (
        "dag_orchestrate_fotmob",
        "dag_refresh_fotmob",
        "dag_backfill_fotmob",
        "dag_trigger_fotmob_daily",
    ):
        sys.modules.pop(name, None)
        sys.modules.pop(f"dags.{name}", None)
        modules[name] = importlib.import_module(name)

    assert {
        name: module.dag.schedule for name, module in modules.items()
    } == {
        "dag_orchestrate_fotmob": "*/5 * * * *",
        "dag_refresh_fotmob": None,
        "dag_backfill_fotmob": None,
        "dag_trigger_fotmob_daily": None,
    }


def test_state_advances_only_after_successful_publication(monkeypatch):
    module = _reload_owner(monkeypatch, isolated=True)
    writes = []
    monkeypatch.setattr(
        module.Variable,
        "set",
        lambda key, value, **kwargs: writes.append((key, value, kwargs)),
        raising=False,
    )

    state = module.FotMobSchedulerState.initial()
    module._store_state(state)
    assert writes == [
        (
            module.SCHEDULER_STATE_VARIABLE,
            state.to_dict(),
            {"serialize_json": True},
        )
    ]

    # Failure has no writer path: only the success-only downstream task owns
    # the durable state mutation.
    assert _task("advance_fotmob_scheduler_state")._init_kwargs.get(
        "trigger_rule", "all_success"
    ) == "all_success"
    assert _task("finalize_fotmob_publication")._init_kwargs["trigger_rule"] == (
        "all_done"
    )


def test_first_run_state_default_is_canonical_json(monkeypatch):
    module = _reload_owner(monkeypatch, isolated=False)

    def _realistic_get(key, *, default_var, deserialize_json):
        assert key == module.SCHEDULER_STATE_VARIABLE
        assert deserialize_json is True
        assert isinstance(default_var, str)
        return json.loads(default_var)

    monkeypatch.setattr(module.Variable, "get", _realistic_get)
    assert module._load_state() == module.FotMobSchedulerState.initial()


def test_successful_child_advances_durable_lane_once_across_task_retry(monkeypatch):
    module = _reload_owner(monkeypatch, isolated=False)
    state = module.FotMobSchedulerState.initial()
    storage = {}
    writes = []

    def _get(key, *, default_var, deserialize_json):
        raw = storage.get(key, default_var)
        return json.loads(raw) if isinstance(raw, str) else raw

    def _set(key, value, *, serialize_json):
        assert serialize_json is True
        storage[key] = value
        writes.append(value)

    monkeypatch.setattr(module.Variable, "get", _get)
    monkeypatch.setattr(module.Variable, "set", _set, raising=False)
    decision = {
        "lane": "refresh",
        "selected_date": "2026-08-08",
        "state": state.to_dict(),
        "state_generation": 0,
    }
    ti = SimpleNamespace(xcom_pull=lambda **kwargs: decision)
    now = datetime(2026, 8, 8, 8, 30, tzinfo=timezone.utc)

    first = module.advance_fotmob_scheduler_state(ti=ti, now_utc=now)
    second = module.advance_fotmob_scheduler_state(ti=ti, now_utc=now)

    assert first == second
    assert first["generation"] == 1
    assert first["next_background_lane"] == "backfill"
    assert writes == [first]


def test_background_launch_rechecks_1330_before_initializing(monkeypatch):
    module = _reload_owner(monkeypatch, isolated=False)
    state = module.FotMobSchedulerState.initial()
    selected_at = datetime(2026, 8, 8, 13, 29, tzinfo=timezone.utc)
    decision = {
        "lane": "refresh",
        "state": state.to_dict(),
        "conf": module.build_child_conf(module.FotMobLane.REFRESH, selected_at),
    }
    ti = SimpleNamespace(xcom_pull=lambda **kwargs: decision)
    initialized = []
    monkeypatch.setattr(module, "_load_state", lambda: state)
    monkeypatch.setattr(
        module,
        "initialize_fotmob_publication",
        lambda **context: initialized.append(context) or {"generation_id": "one"},
    )

    assert (
        module.initialize_admitted_publication(
            ti=ti,
            now_utc=datetime(2026, 8, 8, 13, 30, tzinfo=timezone.utc),
            child_running=False,
        )
        is False
    )
    assert initialized == []

    assert module.initialize_admitted_publication(
        ti=ti,
        now_utc=datetime(2026, 8, 8, 13, 29, 30, tzinfo=timezone.utc),
        child_running=False,
    ) == {"generation_id": "one"}
    assert len(initialized) == 1


def test_deployed_runtime_rejects_legacy_owner_admission(monkeypatch, tmp_path):
    module = _reload_owner(monkeypatch, isolated=False)
    report = tmp_path / "deployment.json"
    report.write_text(
        json.dumps(
            {
                "activation_state": "active",
                "unpaused": [
                    "dag_ingest_fotmob",
                    "dag_transform_fotmob_silver",
                    "dag_trigger_fotmob_daily",
                ],
                "paused": [],
            }
        ),
        encoding="utf-8",
    )
    monkeypatch.setenv(module.FOTMOB_DEPLOYMENT_REPORT_PATH_ENV, str(report))
    monkeypatch.setattr(
        module,
        "attest_fotmob_isolated_runtime",
        lambda **context: {"status": "attested"},
    )
    dag_run = SimpleNamespace(dag_id=module.OWNER_DAG_ID, run_type="scheduled")

    with pytest.raises(
        module.AirflowException,
        match="does not name the automatic owner",
    ):
        module._attest_owner_runtime(dag_run=dag_run)

    report.write_text(
        json.dumps(
            {
                "activation_state": "active",
                "unpaused": sorted(module.AUTOMATIC_ADMITTED_DAGS),
                "paused": [],
            }
        ),
        encoding="utf-8",
    )
    assert module._attest_owner_runtime(dag_run=dag_run) == {
        "status": "attested"
    }


def test_non_exact_isolated_value_does_not_materialize_owner(monkeypatch):
    from airflow.operators.python import PythonOperator

    PythonOperator._instances.clear()
    monkeypatch.setenv("FOTMOB_ISOLATED_STACK", "true")
    sys.modules.pop("dag_orchestrate_fotmob", None)
    sys.modules.pop("dags.dag_orchestrate_fotmob", None)
    module = importlib.import_module("dag_orchestrate_fotmob")
    assert module.dag is None
    assert PythonOperator._instances == []
