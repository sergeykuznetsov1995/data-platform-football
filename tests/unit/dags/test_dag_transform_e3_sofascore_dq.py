"""Production DAG wiring for fail-closed SofaScore committed-state DQ."""

from __future__ import annotations

import importlib
import sys

import pytest


pytestmark = pytest.mark.unit


def _reload_e3():
    from airflow.operators.python import PythonOperator

    PythonOperator._instances.clear()
    sys.modules.pop("dag_transform_e3", None)
    sys.modules.pop("dags.dag_transform_e3", None)
    return importlib.import_module("dag_transform_e3")


@pytest.fixture
def e3():
    return _reload_e3()


def _task(task_id):
    from airflow.operators.python import PythonOperator

    return next(task for task in PythonOperator._instances if task.task_id == task_id)


def test_committed_dq_is_between_sofascore_gold_and_general_e3_validation(e3):
    task = _task("validate_sofascore_committed_state")

    assert task.python_callable is e3._validate_sofascore_committed_state
    assert "gold_e3.fct_sofascore_team_match_post_shot_xg" in task.upstream_task_ids
    assert task.downstream_task_ids == {"validate_e3"}
    assert (
        "validate_sofascore_committed_state" in _task("validate_e3").upstream_task_ids
    )


def test_committed_dq_callable_returns_partition_summary(e3, monkeypatch):
    import utils.sofascore_dq as dq

    expected = {
        "status": "success",
        "partitions": 2,
        "checks": 50,
        "results": [],
    }
    monkeypatch.setattr(
        dq,
        "run_active_registry_committed_dq",
        lambda: expected,
    )

    assert e3._validate_sofascore_committed_state() == expected


@pytest.mark.parametrize("error", ["dq", "trino"])
def test_committed_dq_callable_fails_closed(e3, monkeypatch, error):
    from airflow.exceptions import AirflowException

    import utils.sofascore_dq as dq

    def fail():
        if error == "dq":
            raise dq.SofaScoreDQViolation("rating below 95%")
        raise RuntimeError("table cannot be planned")

    monkeypatch.setattr(dq, "run_active_registry_committed_dq", fail)

    with pytest.raises(AirflowException, match="SofaScore committed-state DQ"):
        e3._validate_sofascore_committed_state()
