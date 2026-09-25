"""Orchestration contract of ``dag_espn_current`` (#1504).

The DAG file lives in ``deploy/espn/dags/`` (the espn-airflow DAG folder,
#1507), so it is imported by path under the Airflow stubs of this package.
"""

from __future__ import annotations

import importlib.util
from datetime import timedelta
from pathlib import Path
from types import SimpleNamespace

import pytest

DAG_FILE = Path(__file__).resolve().parents[3] / "deploy" / "espn" / "dags" / "dag_espn_current.py"


@pytest.fixture
def dag_module():
    from airflow.operators.python import PythonOperator

    PythonOperator._instances.clear()
    spec = importlib.util.spec_from_file_location("dag_espn_current_under_test", DAG_FILE)
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


def _task(task_id: str):
    from airflow.operators.python import PythonOperator

    return next(task for task in PythonOperator._instances if task.task_id == task_id)


@pytest.mark.unit
def test_four_waves_a_day_paused_on_creation(dag_module) -> None:
    kwargs = dag_module.dag._dag_kwargs
    assert kwargs["dag_id"] == "dag_espn_current"
    assert kwargs["schedule"] == "0 0,6,12,18 * * *"
    assert kwargs["max_active_runs"] == 1
    assert kwargs["catchup"] is False
    assert kwargs["is_paused_upon_creation"] is True
    assert kwargs["dagrun_timeout"] == timedelta(minutes=55)


@pytest.mark.unit
def test_topology_retries_and_one_mapped_task_per_tournament(dag_module) -> None:
    prepare, plan = _task("prepare"), _task("plan_wave")
    run, summary = _task("run_tournament"), _task("wave_summary")

    assert prepare.downstream_task_ids == {"plan_wave"}
    assert plan.downstream_task_ids == {"run_tournament"}
    assert run.downstream_task_ids == {"wave_summary"}
    for task in (prepare, plan, run):
        assert task._init_kwargs["retries"] > 0, task.task_id
    assert run.is_mapped is True
    assert run._expand_kwargs["op_kwargs"].operator is plan
    assert run._init_kwargs["retry_delay"] == timedelta(minutes=3)
    assert run._init_kwargs["max_active_tis_per_dag"] == 4
    assert plan._init_kwargs["pool"] == run._init_kwargs["pool"] == "espn_live"
    assert summary._init_kwargs["trigger_rule"] == "all_done"
    assert summary._init_kwargs["retries"] == 0


class _TI:
    def __init__(self, task_id, state, map_index=-1, outcome=None):
        self.task_id, self.state, self.map_index, self.outcome = task_id, state, map_index, outcome


class _SummaryTI:
    def __init__(self, instances):
        self.instances = instances

    def xcom_pull(self, *, task_ids, map_indexes, key):
        assert (task_ids, key) == ("run_tournament", "outcome")
        return next(i.outcome for i in self.instances if i.map_index == map_indexes)


def _outcome(slug, state="green", error=None):
    return {"slug": slug, "season_year": 2026, "state": state, "matches": 1,
            "dispositions": {}, "first_error": error}


def _context(instances):
    dag_run = SimpleNamespace(get_task_instances=lambda: instances, start_date=None)
    return {"dag_run": dag_run, "ti": _SummaryTI(instances)}


@pytest.mark.unit
def test_wave_summary_tolerates_one_red_tournament_in_five(dag_module) -> None:
    instances = [_TI("plan_wave", "success")] + [
        _TI("run_tournament", "success", i, _outcome(f"x.{i}")) for i in range(4)
    ] + [_TI("run_tournament", "failed", 4, _outcome("x.4", "red", "RuntimeError: boom"))]

    result = dag_module.wave_summary(**_context(instances))

    assert (result["tournaments"], result["red_tournaments"], result["red"]) == (5, 1, False)
    assert "x.4:2026 -> red -> RuntimeError: boom" in result["table"]


@pytest.mark.unit
def test_wave_summary_is_red_when_the_plan_failed(dag_module) -> None:
    from airflow.exceptions import AirflowException

    instances = [_TI("plan_wave", "failed"), _TI("run_tournament", "upstream_failed", -1)]

    with pytest.raises(AirflowException, match="plan_wave: failed"):
        dag_module.wave_summary(**_context(instances))
