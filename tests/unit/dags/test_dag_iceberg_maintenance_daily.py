from __future__ import annotations

import importlib
import sys
from datetime import datetime, timezone

import pytest


def _load_module():
    sys.modules.pop("dag_iceberg_maintenance_daily", None)
    return importlib.import_module("dag_iceberg_maintenance_daily")


def test_daily_maintenance_is_split_serial_and_fail_closed() -> None:
    from airflow.operators.python import PythonOperator

    PythonOperator._instances.clear()
    mod = _load_module()
    tasks = {task.task_id: task for task in PythonOperator._instances}

    assert mod.dag._dag_kwargs["max_active_runs"] == 1
    assert mod.dag._dag_kwargs["max_active_tasks"] == 1
    assert set(tasks) == {
        "janitor_fbref_generic_stages",
        "cleanup_whoscored_dq_stage_partitions",
        "maintain_whoscored_bronze",
        "maintain_other_high_churn_bronze",
    }
    assert tasks["maintain_whoscored_bronze"].upstream_task_ids == {
        "cleanup_whoscored_dq_stage_partitions"
    }
    assert tasks["janitor_fbref_generic_stages"].downstream_task_ids == {
        "cleanup_whoscored_dq_stage_partitions",
        "maintain_other_high_churn_bronze",
    }
    assert tasks["cleanup_whoscored_dq_stage_partitions"]._init_kwargs[
        "trigger_rule"
    ] == "all_done"
    assert tasks["maintain_other_high_churn_bronze"]._init_kwargs[
        "trigger_rule"
    ] == "all_done"

    with pytest.raises(Exception, match="WhoScored had 1 table failure"):
        mod._fail_on_partial_maintenance(
            {"failures": [("iceberg.bronze.whoscored_events", "boom")]},
            group="WhoScored",
        )


def test_daily_maintenance_uses_distinct_effective_retention(monkeypatch) -> None:
    import utils.maintenance_tasks as maintenance

    mod = _load_module()
    calls: list[dict] = []

    def _fake(**kwargs):
        calls.append(kwargs)
        return {"failures": []}

    monkeypatch.setattr(maintenance, "maintain_iceberg_tables", _fake)

    logical_date = datetime(2026, 7, 14, tzinfo=timezone.utc)
    mod._maintain_whoscored(logical_date=logical_date)
    mod._maintain_other_high_churn(logical_date=logical_date)

    assert calls[0]["retention_threshold"] == "14d"
    assert calls[0]["table_filter"] == maintenance.WHOSCORED_HIGH_CHURN
    assert calls[0]["compact_live_files"] is True
    assert calls[0]["compaction_rotation"] == logical_date.date().toordinal()
    assert calls[1]["retention_threshold"] == "3d"
    assert calls[1]["table_filter"] == maintenance.NON_WHOSCORED_HIGH_CHURN
    assert calls[1]["compact_live_files"] is False
    assert "compaction_rotation" not in calls[1]
    assert sum(call["compact_live_files"] is True for call in calls) == 1


def test_daily_maintenance_runs_logical_dq_retention(monkeypatch) -> None:
    import utils.maintenance_tasks as maintenance

    mod = _load_module()
    expected = {"status": "success", "partitions_deleted": 3}
    monkeypatch.setattr(
        maintenance,
        "cleanup_whoscored_dq_stage_partitions",
        lambda: expected,
    )

    assert mod._cleanup_whoscored_dq_stage() is expected
