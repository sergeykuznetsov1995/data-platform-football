from __future__ import annotations

import importlib
import sys
from datetime import datetime, timezone


def _load_module():
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
