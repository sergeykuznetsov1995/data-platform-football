from __future__ import annotations

import importlib
import sys
from datetime import timedelta
from types import SimpleNamespace

import pytest


def _load_dag_module():
    from airflow.operators.bash import BashOperator
    from airflow.operators.python import PythonOperator

    BashOperator._instances.clear()
    PythonOperator._instances.clear()
    sys.modules.pop("dags.dag_backfill_sofascore_all_mens", None)
    return importlib.import_module("dags.dag_backfill_sofascore_all_mens")


def _run_scope_operator():
    from airflow.operators.bash import BashOperator

    return next(
        item for item in BashOperator._instances
        if item.task_id == "run_historical_scope"
    )


@pytest.mark.unit
def test_all_mens_backfill_uses_one_bounded_dynamic_operator():
    from airflow.operators.bash import BashOperator
    from airflow.operators.python import PythonOperator

    BashOperator._instances.clear()
    PythonOperator._instances.clear()
    sys.modules.pop("dags.dag_backfill_sofascore_all_mens", None)
    module = importlib.import_module("dags.dag_backfill_sofascore_all_mens")
    dag = module.dag

    assert dag.schedule == "@continuous"
    assert dag._dag_kwargs["max_active_runs"] == 1
    assert dag._dag_kwargs["max_active_tasks"] == 1
    assert len(BashOperator._instances) + len(PythonOperator._instances) < 10
    mapped = next(
        item for item in BashOperator._instances
        if item.task_id == "run_historical_scope"
    )
    assert mapped._expand_kwargs["env"].operator.task_id == "plan_historical_batch"
    assert mapped._init_kwargs["pool"] == "ingest_scraper_pool"
    assert mapped._init_kwargs["priority_weight"] == 1
    assert "run_sofascore_scope_cycle.py" in mapped.bash_command
    assert '--expected-snapshot-id "${SOFASCORE_EXPECTED_SNAPSHOT_ID}"' in (
        mapped.bash_command
    )
    assert '--dag-id "${AIRFLOW_CTX_DAG_ID}"' in mapped.bash_command
    assert '--run-id "${AIRFLOW_CTX_DAG_RUN_ID}"' in mapped.bash_command
    assert '--task-id "${AIRFLOW_CTX_TASK_ID}"' in mapped.bash_command
    assert mapped._init_kwargs["do_xcom_push"] is False
    assert mapped._init_kwargs["max_active_tis_per_dag"] == 1


@pytest.mark.unit
def test_run_historical_scope_gets_one_airflow_retry_after_lease_grace():
    _load_dag_module()
    mapped = _run_scope_operator()

    # One retry with the same run_id reuses the signed plan and replays
    # finished allocations from raw; the delay must outlast the gateway
    # reaper grace (30 s) so a latched lease is reclaimable.
    assert mapped._init_kwargs["retries"] == 1
    assert mapped._init_kwargs["retry_delay"] >= timedelta(seconds=60)


def _planner_kwargs(module, monkeypatch):
    """Drive ``_plan_historical_batch`` to the planner and capture its kwargs."""

    captured = {}

    def _plan(snapshot, **kwargs):
        captured.update(kwargs)
        return []

    monkeypatch.setattr(module, "_production_dag_active", lambda: False)
    monkeypatch.setattr(
        module.state, "read_snapshot", lambda *a, **k: {"campaign_id": "c"}
    )
    monkeypatch.setattr(module.state, "read_completed", lambda *a, **k: set())
    monkeypatch.setattr(module.state, "read_failures", lambda *a, **k: {})
    monkeypatch.setattr(
        module, "load_verified_workload_policy",
        lambda *a, **k: SimpleNamespace(classes={}),
    )
    monkeypatch.setattr(module.state, "plan_historical_batch", _plan)
    assert module._plan_historical_batch(run_id="manual__1") == []
    return captured


@pytest.mark.unit
def test_history_lane_defaults_match_the_single_slot_campaign(monkeypatch):
    for name in (
        "SOFASCORE_HISTORY_BATCH_SIZE",
        "SOFASCORE_HISTORY_POOL",
        "SOFASCORE_HISTORY_MAX_ACTIVE_TASKS",
        "SOFASCORE_HISTORY_FIRST_START_YEAR",
        "SOFASCORE_HISTORY_RATE_LIMIT_PER_MINUTE",
        "SOFASCORE_HISTORY_PROXY_CONTROL_URL",
        "SOFASCORE_HISTORY_MAX_SCOPE_ATTEMPTS",
    ):
        monkeypatch.delenv(name, raising=False)
    module = _load_dag_module()
    mapped = _run_scope_operator()

    assert module.dag._dag_kwargs["max_active_tasks"] == 1
    assert mapped._init_kwargs["pool"] == "ingest_scraper_pool"
    assert mapped._init_kwargs["max_active_tis_per_dag"] == 1
    kwargs = _planner_kwargs(module, monkeypatch)
    assert kwargs["batch_size"] == 1
    assert kwargs["first_start_year"] == 2025
    assert kwargs["task_env"] == {}
    assert kwargs["max_scope_attempts"] == 3
    assert kwargs["failures"] == {}


@pytest.mark.unit
def test_history_lane_knobs_come_from_env(monkeypatch):
    monkeypatch.setenv("SOFASCORE_HISTORY_BATCH_SIZE", "3")
    monkeypatch.setenv("SOFASCORE_HISTORY_POOL", "sofascore_history_pool")
    monkeypatch.setenv("SOFASCORE_HISTORY_MAX_ACTIVE_TASKS", "2")
    monkeypatch.setenv("SOFASCORE_HISTORY_FIRST_START_YEAR", "2024")
    monkeypatch.setenv("SOFASCORE_HISTORY_RATE_LIMIT_PER_MINUTE", "60")
    monkeypatch.setenv(
        "SOFASCORE_HISTORY_PROXY_CONTROL_URL", "http://sofascore-gw-history:8080"
    )
    monkeypatch.setenv("SOFASCORE_HISTORY_MAX_SCOPE_ATTEMPTS", "5")
    module = _load_dag_module()
    mapped = _run_scope_operator()

    assert module.dag._dag_kwargs["max_active_tasks"] == 2
    assert mapped._init_kwargs["pool"] == "sofascore_history_pool"
    assert mapped._init_kwargs["max_active_tis_per_dag"] == 2
    kwargs = _planner_kwargs(module, monkeypatch)
    assert kwargs["batch_size"] == 3
    assert kwargs["first_start_year"] == 2024
    assert kwargs["task_env"] == {
        "SOFASCORE_RATE_LIMIT_PER_MINUTE": "60",
        "SOFASCORE_PROXY_CONTROL_URL": "http://sofascore-gw-history:8080",
    }
    assert kwargs["max_scope_attempts"] == 5


@pytest.mark.unit
@pytest.mark.parametrize(
    ("name", "value"),
    [
        ("SOFASCORE_HISTORY_BATCH_SIZE", "0"),
        ("SOFASCORE_HISTORY_MAX_ACTIVE_TASKS", "two"),
        ("SOFASCORE_HISTORY_FIRST_START_YEAR", "1999"),
        ("SOFASCORE_HISTORY_RATE_LIMIT_PER_MINUTE", "61"),
        ("SOFASCORE_HISTORY_MAX_SCOPE_ATTEMPTS", "0"),
    ],
)
def test_invalid_history_lane_knob_fails_dag_parse(monkeypatch, name, value):
    monkeypatch.setenv(name, value)

    with pytest.raises(ValueError, match=name):
        _load_dag_module()


@pytest.mark.unit
@pytest.mark.parametrize(
    ("production_active", "expected"), [(False, True), (True, False)]
)
def test_history_admission_protects_the_daily_window(
    production_active, expected
):
    module = importlib.import_module("dags.dag_backfill_sofascore_all_mens")
    assert module._history_start_allowed(
        production_active=production_active,
    ) is expected
    assert not hasattr(module, "HISTORY_BLACKOUT_START_HOUR_UTC")


def _capture_env(scope_key):
    return {
        "SOFASCORE_CAMPAIGN_ACTION": "capture",
        "SOFASCORE_EXPECTED_CAMPAIGN_ID": scope_key.split(":")[0],
        "SOFASCORE_SCOPE_KEY": scope_key,
    }


@pytest.mark.unit
def test_finalize_remembers_failed_scopes_by_map_index(monkeypatch):
    module = _load_dag_module()
    planned = [_capture_env("c:8:825"), _capture_env("c:17:1725")]
    states = {0: "failed", 1: "success"}
    marked = []
    monkeypatch.setattr(
        module.state, "mark_failed",
        lambda path, **kw: marked.append((path, kw)),
    )

    def get_task_instance(task_id, map_index=-1):
        assert task_id in ("run_historical_scope", "validate_historical_scope")
        if task_id == "validate_historical_scope":
            # The bash step decides this case; validation followed it.
            return SimpleNamespace(
                state="upstream_failed" if states[map_index] == "failed"
                else "success"
            )
        return SimpleNamespace(state=states[map_index])

    context = {
        "ti": SimpleNamespace(
            xcom_pull=lambda **kw: planned, xcom_push=lambda **kw: None
        ),
        "dag_run": SimpleNamespace(get_task_instance=get_task_instance),
        "run_id": "manual__2",
    }

    assert module._finalize_historical_run(**context)["did_work"] is True
    assert marked == [(
        module.FAILURES_PATH,
        {"campaign_id": "c", "scope_key": "c:8:825", "run_id": "manual__2"},
    )]
    # The memory lives next to the completed-state file.
    assert module.FAILURES_PATH == str(
        module.Path(module.STATE_PATH).with_name("failures.json")
    )


@pytest.mark.unit
def test_finalize_remembers_a_scope_whose_validation_failed(monkeypatch):
    """A paid scope that fails VALIDATION must age towards parking too.

    The bash step succeeds, so ``mark_completed``/``clear_failed`` are never
    reached (validation raises first) — and looking only at the bash task meant
    ``mark_failed`` was never reached either.  The scope stayed ``ready`` at
    ``depth=0``, came back first in the next @continuous run, was paid for in
    full again and failed identically: an unbounded paid loop on one broken
    scope.
    """

    module = _load_dag_module()
    planned = [_capture_env("c:8:825")]
    marked = []
    monkeypatch.setattr(
        module.state, "mark_failed",
        lambda path, **kw: marked.append(kw),
    )

    def get_task_instance(task_id, map_index=-1):
        # Bash green, validation red.
        return SimpleNamespace(
            state="success" if task_id == "run_historical_scope" else "failed"
        )

    context = {
        "ti": SimpleNamespace(
            xcom_pull=lambda **kw: planned, xcom_push=lambda **kw: None
        ),
        "dag_run": SimpleNamespace(get_task_instance=get_task_instance),
        "run_id": "manual__3",
    }

    module._finalize_historical_run(**context)
    assert marked == [
        {"campaign_id": "c", "scope_key": "c:8:825", "run_id": "manual__3"}
    ]


@pytest.mark.unit
def test_validated_scope_clears_its_failure_memory(monkeypatch, tmp_path):
    module = _load_dag_module()
    result = tmp_path / "result.json"
    result.write_text(
        '{"status": "success", "snapshot_id": "s", "campaign_id": "c",'
        ' "tournament_id": 8, "source_season_id": 825}'
    )
    calls = []
    monkeypatch.setattr(
        module.state, "mark_completed",
        lambda path, **kw: calls.append(("completed", kw["scope_key"])),
    )
    monkeypatch.setattr(
        module.state, "clear_failed",
        lambda path, **kw: calls.append(("cleared", kw["scope_key"])),
    )

    module._validate_historical_scope(
        SOFASCORE_CAMPAIGN_ACTION="capture",
        SOFASCORE_SCOPE_RESULT_PATH=str(result),
        SOFASCORE_EXPECTED_SNAPSHOT_ID="s",
        SOFASCORE_SCOPE_KEY="c:8:825",
    )

    assert calls == [("completed", "c:8:825"), ("cleared", "c:8:825")]
