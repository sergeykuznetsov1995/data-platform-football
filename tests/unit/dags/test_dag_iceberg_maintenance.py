from __future__ import annotations

import importlib
import sys
from datetime import datetime, timezone

from airflow.operators.python import PythonOperator


def _load_module():
    PythonOperator._instances.clear()
    sys.modules.pop("dag_iceberg_maintenance", None)
    return importlib.import_module("dag_iceberg_maintenance")


def test_weekly_maintenance_activates_bounded_compaction(monkeypatch) -> None:
    import utils.maintenance_tasks as maintenance

    mod = _load_module()
    calls: list[dict] = []

    def _fake(**kwargs):
        calls.append(kwargs)
        return {"failures": []}

    monkeypatch.setattr(maintenance, "maintain_iceberg_tables", _fake)
    logical_date = datetime(2026, 7, 12, tzinfo=timezone.utc)

    mod._maintain(logical_date=logical_date)

    assert calls == [
        {
            "compact_live_files": True,
            "compaction_rotation": logical_date.date().toordinal() // 7,
        }
    ]


def test_manifest_lifecycle_is_an_independent_weekly_task(monkeypatch) -> None:
    import utils.maintenance_tasks as maintenance

    mod = _load_module()
    expected = {"table": "iceberg.ops.sofascore_capture_manifest"}
    calls = []

    def _fake():
        calls.append(True)
        return expected

    monkeypatch.setattr(maintenance, "maintain_sofascore_capture_manifest", _fake)

    assert mod._maintain_sofascore_capture_manifest() is expected
    assert calls == [True]
    assert mod.dag.schedule == "0 5 * * 0"

    tasks = {task.task_id: task for task in PythonOperator._instances}
    assert set(tasks) == {
        "maintain_iceberg_tables",
        "maintain_sofascore_capture_manifest",
    }
    assert tasks["maintain_sofascore_capture_manifest"].upstream_task_ids == set()
    assert tasks["maintain_sofascore_capture_manifest"].downstream_task_ids == set()
    assert tasks["maintain_iceberg_tables"].upstream_task_ids == set()
    assert tasks["maintain_iceberg_tables"].downstream_task_ids == set()
