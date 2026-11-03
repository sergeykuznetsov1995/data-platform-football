from __future__ import annotations

import importlib
import sys
from datetime import datetime, timezone

from airflow.operators.python import PythonOperator


def _load_module():
    PythonOperator._instances.clear()
    sys.modules.pop("dag_iceberg_maintenance", None)
    return importlib.import_module("dag_iceberg_maintenance")


def test_weekly_maintenance_leaves_compaction_to_nightly_pass(monkeypatch) -> None:
    import utils.maintenance_tasks as maintenance

    mod = _load_module()
    calls: list[dict] = []

    def _fake(**kwargs):
        calls.append(kwargs)
        return {"failures": []}

    monkeypatch.setattr(maintenance, "maintain_iceberg_tables", _fake)
    logical_date = datetime(2026, 7, 12, tzinfo=timezone.utc)

    mod._maintain(logical_date=logical_date)

    assert calls == [{"compact_live_files": True, "compaction_schemas": ("silver", "gold"), "compaction_rotation": logical_date.date().toordinal() // 7}]


def test_weekly_has_single_sweep_task_with_long_timeout() -> None:
    from datetime import timedelta

    mod = _load_module()
    assert mod.dag.schedule == "0 5 * * 0"
    tasks = {task.task_id: task for task in PythonOperator._instances}
    # The ops manifest moved to the daily maintain_ops task (no duplicate).
    assert set(tasks) == {"maintain_iceberg_tables"}
    assert tasks["maintain_iceberg_tables"]._init_kwargs["execution_timeout"] == timedelta(hours=3)
