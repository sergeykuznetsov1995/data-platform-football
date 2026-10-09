"""Cold imports and runtime plans must not load the shared scraper stack."""

import importlib
import os
from pathlib import Path
import subprocess
import sys

import pytest


_PROBE = r'''
import importlib
import importlib.abc
from datetime import datetime, timezone
from pathlib import Path
import runpy
import socket
import sys

root = Path.cwd()
sys.path.insert(0, str(root / "dags"))
runpy.run_path(str(root / "tests/unit/dags/conftest.py"))

def forbidden_network(*args, **kwargs):
    raise AssertionError("planning test attempted a real network request")

socket.socket.connect = forbidden_network

def forbidden_module(name):
    return any(name == prefix or name.startswith(prefix + ".")
               for prefix in ("scrapers.base", "scrapers.whoscored", "whoscored"))

class ImportGuard(importlib.abc.MetaPathFinder):
    def find_spec(self, fullname, path=None, target=None):
        if forbidden_module(fullname):
            raise AssertionError("plan imported " + fullname)

sys.meta_path.insert(0, ImportGuard())
case = sys.argv[1]
if case == "catalog":
    importlib.import_module("scrapers.understat.catalog")
else:
    name = "dag_ingest_understat" if case.endswith("current") else "dag_backfill_understat"
    module = importlib.import_module(name)
    if case == "plan_current":
        import scrapers.understat as understat
        from scrapers.understat.catalog import LEAGUES, UnderstatScope, season_slug

        class Client:
            def __init__(self, **kwargs): pass
            def close(self): pass

        class Catalog:
            def __init__(self, client, **kwargs): pass
            def rolling_scopes(self, **kwargs):
                assert kwargs == {"window": 2, "probe_next": True}
                return [UnderstatScope(item.league, item.source_league,
                            item.source_league_id, season_slug(year), year,
                            year < 2026)
                        for item in LEAGUES for year in (2025, 2026)]

        understat.UnderstatClient = Client
        understat.UnderstatCatalog = Catalog
        plan = module.plan_current_scopes(
            logical_date=datetime(2026, 10, 5, tzinfo=timezone.utc))
        assert len(plan) == 12
        assert {row["UNDERSTAT_LEAGUE"] for row in plan} == {item.league for item in LEAGUES}
    elif case == "plan_history":
        import trino.dbapi
        statements = []
        closed = []

        class Cursor:
            def execute(self, sql, *params):
                statements.append((sql, params))
            def fetchall(self): return []
            def close(self): closed.append("cursor")

        class Connection:
            def cursor(self): return Cursor()
            def close(self): closed.append("connection")

        trino.dbapi.connect = lambda **kwargs: Connection()
        assert module.plan_history_scope() == []
        assert len(statements) == 1
        assert statements[0][0].startswith("WITH keys AS")
        assert statements[0][1] == ()
        assert closed == ["cursor", "connection"]

assert not any(forbidden_module(name) for name in sys.modules)
assert "scrapers.understat.scraper" not in sys.modules
'''


@pytest.mark.parametrize("case", [
    "catalog", "import_current", "import_history", "plan_current", "plan_history",
])
def test_understat_plans_are_isolated_in_fresh_process(case):
    root = Path(__file__).resolve().parents[3]
    result = subprocess.run(
        [sys.executable, "-B", "-c", _PROBE, case],
        cwd=root,
        env={**os.environ, "PYTHONDONTWRITEBYTECODE": "1"},
        capture_output=True,
        text=True,
        timeout=30,
    )
    assert result.returncode == 0, result.stdout + result.stderr


@pytest.mark.parametrize("module_name, planner", [
    ("dag_ingest_understat", "plan_current_scopes"),
    ("dag_backfill_understat", "plan_history_scope"),
])
@pytest.mark.parametrize("configured", [[], ["RUS-Premier League"], ["UNKNOWN-League"]])
def test_config_mismatch_fails_before_external_work(monkeypatch, module_name, planner, configured):
    from airflow.exceptions import AirflowException
    import scrapers.understat as understat
    from scrapers.understat.manifest import UnderstatManifestRepository

    module = importlib.import_module(module_name)
    monkeypatch.setattr(module, "UNDERSTAT_LEAGUES", configured, raising=False)

    def no_external_work(*args, **kwargs):
        raise AssertionError("configuration mismatch must fail before external work")

    monkeypatch.setattr(understat, "UnderstatClient", no_external_work)
    monkeypatch.setattr(UnderstatManifestRepository, "from_env", no_external_work)
    with pytest.raises(AirflowException, match="UNDERSTAT_LEAGUES.*missing=.*extra="):
        getattr(module, planner)()
