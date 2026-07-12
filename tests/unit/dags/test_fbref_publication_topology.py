"""Fail-closed topology contracts for the FBref publication chain."""

from __future__ import annotations

import importlib
from pathlib import Path
import sys

import pytest


def _reset_operators() -> None:
    from airflow.operators.bash import BashOperator
    from airflow.operators.python import PythonOperator

    BashOperator._instances.clear()
    PythonOperator._instances.clear()


def _reload_ingest():
    _reset_operators()

    from utils import medallion_config

    medallion_config.CONFIG_DIR = (
        Path(__file__).resolve().parents[3] / "configs" / "medallion"
    )
    medallion_config.reset_cache()
    sys.modules.pop("dag_ingest_fbref", None)
    sys.modules.pop("dags.dag_ingest_fbref", None)
    return importlib.import_module("dag_ingest_fbref")


def _reload_silver():
    _reset_operators()
    sys.modules.pop("dag_transform_fbref_silver", None)
    sys.modules.pop("dags.dag_transform_fbref_silver", None)
    return importlib.import_module("dag_transform_fbref_silver")


def _task(task_id: str):
    from airflow.operators.python import PythonOperator

    return next(task for task in PythonOperator._instances if task.task_id == task_id)


def test_ingest_has_fail_closed_terminal_gate_without_nested_silver_trigger():
    _reload_ingest()
    terminal = _task("ingest_complete")
    traffic_report = _task("report_proxy_traffic")
    from airflow.operators.python import PythonOperator

    assert terminal.upstream_task_ids == {
        "validate_all_data",
        "report_proxy_traffic",
    }
    assert terminal._init_kwargs["trigger_rule"] == "all_success"
    assert traffic_report._init_kwargs["trigger_rule"] == "all_done"
    silver_triggers = [
        task
        for task in PythonOperator._instances
        if task._init_kwargs.get("trigger_dag_id") == "dag_transform_fbref_silver"
    ]
    assert silver_triggers == []


def test_silver_validation_is_fail_closed_and_terminal():
    _reload_silver()
    validate_rows = _task("validate_silver")
    validate_quality = _task("validate_silver_quality")

    assert validate_rows._init_kwargs["trigger_rule"] == "all_success"
    assert validate_quality._init_kwargs["trigger_rule"] == "all_success"
    assert validate_rows.downstream_task_ids == {"validate_silver_quality"}
    assert validate_quality.downstream_task_ids == set()


def test_silver_does_not_race_master_by_triggering_gold():
    _reload_silver()
    from airflow.operators.python import PythonOperator

    gold_triggers = [
        task
        for task in PythonOperator._instances
        if task._init_kwargs.get("trigger_dag_id") == "dag_transform_fbref_gold"
    ]
    assert gold_triggers == []


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
