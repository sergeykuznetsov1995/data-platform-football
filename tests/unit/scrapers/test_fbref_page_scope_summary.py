"""Offline guards for the resumable ownership migration and admin entrypoint."""

import ast
import json
from pathlib import Path

import pytest

from scrapers.fbref.control import ControlStore
from scrapers.fbref.control.migrations import MIGRATIONS
from scrapers.fbref.control.store import _ScopeBackfillCursor, _FRONTIER_SCOPE_CTE_TEMPLATE


def test_1805_summary_migration_is_schema_only_and_invalidations_lock_conflicts():
    migration = next(m for m in MIGRATIONS if m.version == 13)
    assert migration.version == 13
    ddl = "\n".join(migration.statements)
    assert "SELECT DISTINCT" not in ddl.split("CREATE FUNCTION", 1)[0]
    assert "FROM fbref_control.frontier_provenance" not in ddl
    invalidation = ddl.split("CREATE FUNCTION fbref_control.invalidate_registry_scope_summary()", 1)[1]
    assert "ON CONFLICT (source, competition_id) DO UPDATE" in invalidation
    assert "ON CONFLICT DO NOTHING" not in invalidation
    # Merely updating last_seen_at must not rescan the entire stored ownership.
    assert "IS NOT DISTINCT FROM" in invalidation
    assert "current_scope_lifecycle" in invalidation and "current_scope_reason" in invalidation
    assert "OLD.is_current" in invalidation and "OLD.season_id" in invalidation


def test_1805_backfill_limits_fail_before_opening_a_connection():
    def connect(_dsn):
        pytest.fail("invalid batch size reached PostgreSQL")
    store = ControlStore("postgresql://test.invalid/scope", connection_factory=connect)
    for size in (0, -1, 1001):
        with pytest.raises(ValueError, match="batch_size"):
            store.backfill_page_scopes(batch_size=size)


def test_registry_refresh_skips_locked_invalidations_and_deletes_only_captured_rows():
    captured = [{"source": "fbref", "competition_id": "9"}]
    deleted = []
    class Cursor:
        def execute(self, sql, params=None):
            self.rows = []
            if "SELECT source, competition_id" in sql:
                assert "FOR UPDATE SKIP LOCKED" in sql
                # Another registry transaction owns competition 10's queue row.
                self.rows = captured
            elif "DELETE FROM fbref_control.page_scope_registry_dirty" in sql:
                deleted.extend(json.loads(params[0]))

        def fetchall(self):
            return self.rows

    store = ControlStore("postgresql://test.invalid/scope")
    assert store._refresh_registry_scopes(Cursor()) == 0
    assert deleted == captured


def test_1805_backfill_deadline_applies_to_the_entire_batch(monkeypatch):
    calls = []
    class Cursor:
        def execute(self, sql, params=None):
            calls.append((sql, params))
    clock = iter((0, 10, 24, 26))
    monkeypatch.setattr("scrapers.fbref.control.store.monotonic", lambda: next(clock))
    cursor = _ScopeBackfillCursor(Cursor())
    cursor.execute("SELECT 1")
    cursor.execute("SELECT 2")
    with pytest.raises(TimeoutError, match="time budget"):
        cursor.execute("SELECT 3")
    assert [params[0] for sql, params in calls if "set_config" in sql] == ["15000ms", "1000ms"]
    assert [sql for sql, _ in calls if "set_config" not in sql] == ["SELECT 1", "SELECT 2"]


def test_1805_control_cli_runs_one_bounded_batch(monkeypatch, capsys):
    from scrapers.fbref.control import __main__ as cli
    calls = []
    class Store:
        def backfill_page_scopes(self, **kwargs):
            calls.append(kwargs)
            return {"processed": 2, "after_target_id": "last", "done": False}
    monkeypatch.setattr(cli.ControlStore, "from_env", lambda: Store())
    monkeypatch.setattr("sys.argv", ["fbref-control", "backfill-page-scopes", "--batch-size", "2"])
    assert cli.main() == 0
    assert calls == [{"batch_size": 2}]
    assert json.loads(capsys.readouterr().out)["done"] is False


def test_1805_collector_matches_frozen_v13_ownership_semantics():
    from scrapers.fbref.control.migrations import SCOPE_CTE_V13
    assert _FRONTIER_SCOPE_CTE_TEMPLATE == SCOPE_CTE_V13, "ownership changes need a new summary migration"


def test_1805_control_imports_fit_existing_runtime_contract():
    # FotMob/WhoScored deliveries already ship these control modules. Adding an
    # import of a new module must not silently extend their delivery contract.
    shipped = {"__init__", "migrations", "models", "replay_effects", "store"}
    root = Path(__file__).resolve().parents[3] / "scrapers/fbref/control"
    for module in sorted(shipped):
        tree = ast.parse((root / f"{module}.py").read_text())
        dependencies = {
            node.module.rsplit(".", 1)[-1]
            for node in ast.walk(tree)
            if isinstance(node, ast.ImportFrom)
            and node.module
            and node.module.startswith("scrapers.fbref.control.")
        }
        assert dependencies <= shipped, f"{module} adds runtime dependencies: {dependencies - shipped}"
