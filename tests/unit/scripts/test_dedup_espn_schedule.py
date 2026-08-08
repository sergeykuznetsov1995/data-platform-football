"""Safety boundary for the one-shot legacy ESPN schedule deduplicator."""

from __future__ import annotations

import importlib.util
import sys
from pathlib import Path

import pytest


REPO_ROOT = Path(__file__).resolve().parents[3]
SCRIPT_PATH = REPO_ROOT / "scripts" / "dedup_espn_schedule.py"


def _load_module():
    spec = importlib.util.spec_from_file_location("dedup_espn_schedule", SCRIPT_PATH)
    assert spec is not None and spec.loader is not None
    module = importlib.util.module_from_spec(spec)
    sys.modules[spec.name] = module
    spec.loader.exec_module(module)
    return module


class FakeCursor:
    def __init__(
        self,
        *,
        relation_type: str,
        total: int = 1,
        unique: int = 1,
    ):
        self.relation_type = relation_type
        self.total = total
        self.unique = unique
        self.statements: list[str] = []
        self._last = ""

    def execute(self, sql):
        self._last = sql
        self.statements.append(sql)

    def fetchall(self):
        lowered = self._last.lower()
        if "information_schema.tables" in lowered:
            return [(self.relation_type,)]
        if "count(distinct game_id)" in lowered:
            return [(self.total, self.unique)]
        return []


class FakeConnection:
    def __init__(self, cursor: FakeCursor):
        self._cursor = cursor

    def cursor(self):
        return self._cursor


def _mutating_sql(cursor: FakeCursor) -> list[str]:
    prefixes = ("alter ", "create ", "delete ", "drop ", "insert ", "update ")
    return [
        sql
        for sql in cursor.statements
        if sql.lstrip().lower().startswith(prefixes)
    ]


def test_compact6_refuses_before_opening_trino(monkeypatch):
    module = _load_module()
    monkeypatch.setenv("ESPN_BRONZE_LAYOUT_MODE", "compact6")
    monkeypatch.setattr(
        module,
        "get_conn",
        lambda: pytest.fail("compact6 must refuse before opening Trino"),
    )

    with pytest.raises(RuntimeError, match="legacy14"):
        module.main()


@pytest.mark.parametrize("layout_mode", [None, "future7"])
def test_missing_or_unknown_layout_refuses_before_opening_trino(
    monkeypatch, layout_mode
):
    from scrapers.espn.layout import LayoutError

    module = _load_module()
    if layout_mode is None:
        monkeypatch.delenv("ESPN_BRONZE_LAYOUT_MODE", raising=False)
    else:
        monkeypatch.setenv("ESPN_BRONZE_LAYOUT_MODE", layout_mode)
    monkeypatch.setattr(
        module,
        "get_conn",
        lambda: pytest.fail("invalid layout must refuse before opening Trino"),
    )

    with pytest.raises(LayoutError):
        module.main()


def test_legacy14_refuses_a_view_before_any_mutation(monkeypatch):
    module = _load_module()
    monkeypatch.setenv("ESPN_BRONZE_LAYOUT_MODE", "legacy14")
    cursor = FakeCursor(relation_type="VIEW")
    monkeypatch.setattr(module, "get_conn", lambda: FakeConnection(cursor))

    with pytest.raises(RuntimeError, match="BASE TABLE"):
        module.main()

    assert _mutating_sql(cursor) == []


def test_verified_legacy14_base_table_can_reach_the_clean_noop(monkeypatch):
    module = _load_module()
    monkeypatch.setenv("ESPN_BRONZE_LAYOUT_MODE", "legacy14")
    cursor = FakeCursor(relation_type="BASE TABLE", total=3, unique=3)
    monkeypatch.setattr(module, "get_conn", lambda: FakeConnection(cursor))

    module.main()

    assert "information_schema.tables" in cursor.statements[0].lower()
    assert _mutating_sql(cursor) == []
