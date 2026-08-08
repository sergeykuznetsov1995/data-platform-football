"""Topology and callable contracts for the trigger-only ESPN Silver DAG."""

from __future__ import annotations

import importlib
import sys

import pytest

pytestmark = pytest.mark.unit


def _reload():
    from airflow.operators.python import PythonOperator

    PythonOperator._instances.clear()
    sys.modules.pop("dag_transform_espn_silver", None)
    sys.modules.pop("dags.dag_transform_espn_silver", None)
    return importlib.import_module("dag_transform_espn_silver")


def _tasks():
    from airflow.operators.python import PythonOperator

    return {task.task_id: task for task in PythonOperator._instances}


def test_trigger_only_dag_materializes_all_six_before_validation():
    module = _reload()
    tasks = _tasks()

    assert module.dag.schedule is None
    assert module.dag._dag_kwargs["catchup"] is False
    assert module.dag._dag_kwargs["max_active_runs"] == 1
    assert module.dag._dag_kwargs["max_active_tasks"] == 1
    assert module.SILVER_TRANSFORMS == [
        ("match", "dags/sql/silver/espn_match.sql", "espn_match"),
        ("team_match", "dags/sql/silver/espn_team_match.sql", "espn_team_match"),
        ("player_match_aggregate", "dags/sql/silver/espn_player_match_aggregate.sql", "espn_player_match_aggregate"),
        ("match_events", "dags/sql/silver/espn_match_events.sql", "espn_match_events"),
        ("substitutions", "dags/sql/silver/espn_substitutions.sql", "espn_substitutions"),
        ("venue", "dags/sql/silver/espn_venue.sql", "espn_venue"),
    ]
    transform_ids = {f"silver_transforms.{name}" for name, _, _ in module.SILVER_TRANSFORMS}
    assert transform_ids == set(tasks) - {"validate_silver", "validate_silver_quality"}
    assert tasks["validate_silver"].upstream_task_ids == transform_ids
    assert tasks["validate_silver"].downstream_task_ids == {"validate_silver_quality"}
    assert tasks["validate_silver_quality"].upstream_task_ids == {"validate_silver"}
    assert tasks["validate_silver_quality"].python_callable is module._validate_silver_quality


def test_no_ingest_or_publication_coupling_and_quality_merges_custom_results(monkeypatch):
    module = _reload()

    from utils.data_quality import CheckResult, RunReport

    report = RunReport(results=[])
    monkeypatch.setattr("utils.data_quality.run_checks", lambda checks, raise_on_error: report)
    monkeypatch.setattr(
        "utils.espn_silver_dq.run_espn_silver_custom_checks",
        lambda: [CheckResult("soft", "custom", "WARNING", False)],
    )
    assert module._validate_silver_quality()["warnings"] == ["soft"]
