"""Fail-closed topology contracts for the FBref publication chain."""

from __future__ import annotations

import importlib
import sys

import pytest


def _reset_operators() -> None:
    from airflow.operators.bash import BashOperator
    from airflow.operators.python import PythonOperator

    BashOperator._instances.clear()
    PythonOperator._instances.clear()


def _reload_silver():
    _reset_operators()
    sys.modules.pop("dag_transform_fbref_silver", None)
    sys.modules.pop("dags.dag_transform_fbref_silver", None)
    return importlib.import_module("dag_transform_fbref_silver")


def _task(task_id: str):
    from airflow.operators.python import PythonOperator

    return next(task for task in PythonOperator._instances if task.task_id == task_id)


def test_silver_validation_is_fail_closed_and_terminal():
    _reload_silver()
    preflight = _task("validate_publication_scope")
    identity = _task("ensure_source_identity_columns")
    validate_rows = _task("validate_silver")
    validate_quality = _task("validate_silver_quality")

    # Both DQ gates keep the default fail-closed all_success rule. Quality is
    # the FBref-only terminal verdict; xref belongs to master publication.
    assert validate_rows._init_kwargs.get("trigger_rule", "all_success") == "all_success"
    assert validate_quality._init_kwargs.get("trigger_rule", "all_success") == "all_success"
    assert validate_rows.downstream_task_ids == {"validate_silver_quality"}
    assert validate_quality.downstream_task_ids == set()
    assert preflight.upstream_task_ids == set()
    assert preflight.downstream_task_ids == {"ensure_source_identity_columns"}
    assert identity.upstream_task_ids == {"validate_publication_scope"}
    assert preflight.python_callable.__name__ == (
        "_validate_fbref_publication_preflight"
    )

    from airflow.operators.python import PythonOperator

    assert not any(
        task._init_kwargs.get("trigger_dag_id") == "dag_transform_xref"
        for task in PythonOperator._instances
    )


def test_silver_does_not_race_master_by_triggering_gold():
    _reload_silver()
    from airflow.operators.python import PythonOperator

    gold_triggers = [
        task
        for task in PythonOperator._instances
        if task._init_kwargs.get("trigger_dag_id") == "dag_transform_fbref_gold"
    ]
    assert gold_triggers == []


def test_silver_preflight_validates_control_scope_before_writes(monkeypatch):
    module = _reload_silver()
    from utils import fbref_pipeline_tasks, silver_tasks

    control_run_id = "11111111-1111-4111-8111-111111111111"

    def resolve(_context):
        return control_run_id

    def validate(**kwargs):
        return {"status": "ready", **kwargs}

    monkeypatch.setattr(
        silver_tasks, "fbref_control_run_id_from_context", resolve
    )
    monkeypatch.setattr(
        fbref_pipeline_tasks, "validate_fbref_publication_scope", validate
    )

    assert module._validate_fbref_publication_preflight() == {
        "status": "ready",
        "control_run_id": control_run_id,
    }


def test_row_count_failure_raises_before_quality_or_gold(monkeypatch):
    module = _reload_silver()
    from airflow.exceptions import AirflowException
    from utils import silver_tasks

    monkeypatch.setattr(
        silver_tasks,
        "validate_silver_tables",
        lambda **_kwargs: {
            "status": "warning",
            "details": {},
            "total_rows": 0,
            "warnings": ["fbref_match_enriched: 0 < 50"],
        },
    )

    with pytest.raises(AirflowException, match="Silver validation FAILED"):
        module._validate_silver()
