from __future__ import annotations

import importlib
import sys
from datetime import datetime, timedelta, timezone

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
    assert mod.dag._dag_kwargs["schedule"] == "0 2 * * *"
    assert tasks["janitor_fbref_generic_stages"]._init_kwargs[
        "execution_timeout"
    ] == timedelta(minutes=120)
    assert mod.dag._dag_kwargs["default_args"]["execution_timeout"] == timedelta(
        minutes=30
    )
    assert all(
        "execution_timeout" not in task._init_kwargs
        for task_id, task in tasks.items()
        if task_id not in {"janitor_fbref_generic_stages", "compact_bronze"}
    )
    assert set(tasks) == {
        "compact_bronze",
        "janitor_fbref_generic_stages",
        "cleanup_whoscored_dq_stage_partitions",
        "maintain_whoscored_bronze",
        "maintain_other_high_churn_bronze",
        "check_maintenance_run",
    }
    assert tasks["maintain_whoscored_bronze"].upstream_task_ids == {
        "cleanup_whoscored_dq_stage_partitions"
    }
    assert tasks["janitor_fbref_generic_stages"].downstream_task_ids == {
        "cleanup_whoscored_dq_stage_partitions",
        "maintain_other_high_churn_bronze",
        "check_maintenance_run",
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
    assert calls[0]["compact_live_files"] is False
    assert "compaction_rotation" not in calls[0]
    assert calls[1]["retention_threshold"] == "3d"
    assert calls[1]["table_filter"] == maintenance.NON_WHOSCORED_HIGH_CHURN
    assert calls[1]["compact_live_files"] is False
    assert "compaction_rotation" not in calls[1]
    assert sum(call["compact_live_files"] is True for call in calls) == 0


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


def test_daily_fbref_callable_uses_bounded_wait(monkeypatch) -> None:
    import utils.fbref_maintenance as maintenance

    mod = _load_module()
    expected = {"mode": "audit", "control_run_id": "maintenance"}
    monkeypatch.setattr(
        maintenance, "maintain_fbref_stages_with_lock_wait", lambda: expected
    )
    assert mod._maintain_fbref_stages() is expected


def test_nightly_compaction_is_first_and_uses_fbref_pool():
    from airflow.operators.python import PythonOperator
    PythonOperator._instances.clear()
    _load_module()
    tasks = {task.task_id: task for task in PythonOperator._instances}
    task = tasks["compact_bronze"]
    assert task._init_kwargs["pool"] == "fbref_scraper_pool"
    assert task._init_kwargs["mode"] == "reschedule"
    assert task.downstream_task_ids == {"janitor_fbref_generic_stages", "check_maintenance_run"}
    assert task._init_kwargs["execution_timeout"] == timedelta(hours=3)


def test_compaction_failure_cannot_be_hidden_by_successful_cleanup():
    from types import SimpleNamespace
    from airflow.operators.python import PythonOperator

    PythonOperator._instances.clear()
    mod = _load_module()
    tasks = {task.task_id: task for task in PythonOperator._instances}
    final = tasks["check_maintenance_run"]
    assert final._init_kwargs["trigger_rule"] == "all_done"
    assert final.upstream_task_ids == set(tasks) - {final.task_id}
    assert not final.downstream_task_ids
    assert tasks["cleanup_whoscored_dq_stage_partitions"]._init_kwargs[
        "trigger_rule"
    ] == "all_done"
    instances = [
        SimpleNamespace(task_id=task_id, state=(
            "failed" if task_id == "compact_bronze" else
            "upstream_failed" if task_id == "janitor_fbref_generic_stages" else "success"
        ))
        for task_id in tasks if task_id != final.task_id
    ]
    context = dict(task=final, dag_run=SimpleNamespace(get_task_instances=lambda: instances))
    with pytest.raises(Exception, match="compact_bronze"):
        mod._check_maintenance_run(**context)
    for ti in instances:
        ti.state = "success"
    mod._check_maintenance_run(**context)
