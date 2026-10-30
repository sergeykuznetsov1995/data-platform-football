"""Shared tooling must load without the retired WhoScored startup ceremony."""

import asyncio
import builtins
import importlib.util
import sys
from pathlib import Path

import pytest

ROOT = Path(__file__).resolve().parents[3]
FILTERS = ("scripts/proxy_filter/filter_proxy.py", "scripts/fbref_proxy/filter_proxy.py")


def _load(relative, monkeypatch):
    def forbidden(*args, **kwargs):
        raise AssertionError("WhoScored runtime was accessed")

    real_open = builtins.open
    real_import = builtins.__import__

    def guarded_open(file, *args, **kwargs):
        if "whoscored_runtime_startup" in str(file):
            forbidden()
        return real_open(file, *args, **kwargs)

    def guarded_import(name, *args, **kwargs):
        if name.startswith(("scrapers.whoscored.runtime_contract", "scrapers.whoscored.proxy_campaign")):
            forbidden()
        return real_import(name, *args, **kwargs)

    monkeypatch.delattr(sys, "_whoscored_runtime_startup_root", raising=False)
    monkeypatch.setattr(sys, "_load_whoscored_runtime_contract", forbidden, raising=False)
    monkeypatch.setattr(builtins, "open", guarded_open)
    monkeypatch.setattr(builtins, "__import__", guarded_import)
    spec = importlib.util.spec_from_file_location("independent_shared_tool", ROOT / relative)
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


@pytest.mark.parametrize("relative", FILTERS)
def test_filter_import_without_whoscored_anchor(relative, monkeypatch):
    module = _load(relative, monkeypatch)
    assert module.SOURCE_MODE == "shared-no-whoscored"


@pytest.mark.parametrize("relative", FILTERS)
def test_retired_source_mode_rejected_before_listener(relative, monkeypatch):
    module = _load(relative, monkeypatch)
    monkeypatch.setattr(sys, "argv", [relative, "--source-mode", "whoscored-only"])
    monkeypatch.setattr(module.asyncio, "start_server", lambda *a, **kw: pytest.fail("listener opened"))
    with pytest.raises(SystemExit) as error:
        asyncio.run(module.main())
    assert error.value.code == 2


@pytest.mark.parametrize("relative", FILTERS)
@pytest.mark.parametrize("metadata", [
    {"source": "whoscored", "dag_id": "dag_ingest_whoscored"},
    {"dag_id": "dag_backfill_whoscored"},
    {"source": "whoscored", "dag_id": "dag_ingest_fbref"},
])
def test_whoscored_lease_rejected_before_upstream(relative, metadata, monkeypatch):
    module = _load(relative, monkeypatch)

    class Manager:
        def get_proxy(self):
            pytest.fail("upstream selected")

    with pytest.raises(ValueError, match="WhoScored leases are not supported"):
        module._create_lease(Manager(), max_bytes=100, ttl_seconds=30, metadata=metadata)


def test_audit_import_uses_pure_inventory(monkeypatch):
    module = _load("scripts/audit_bronze_columns.py", monkeypatch)
    inventory = module.WHOSCORED_CONTRACT
    inventory.validate_definition()
    assert len(inventory.BUSINESS_TABLES) == 27
    assert len(inventory.MANIFEST_TABLES) == 5
    assert set(inventory.BATCH_COLUMN_BY_TABLE) == set(inventory.BUSINESS_TABLES)
    assert all(inventory.BUSINESS_REQUIRED_COLUMNS[table] for table in inventory.BUSINESS_TABLES)


def test_maintenance_without_direct_runtime_gate(monkeypatch):
    from utils import maintenance_tasks

    real_import = builtins.__import__

    def guarded_import(name, *args, **kwargs):
        if name == "scrapers.whoscored.runtime_contract":
            pytest.fail("maintenance imported the WhoScored runtime gate")
        return real_import(name, *args, **kwargs)

    class Connection:
        closed = False

        def close(self):
            self.closed = True

    connection = Connection()
    monkeypatch.setattr(builtins, "__import__", guarded_import)
    monkeypatch.setattr(maintenance_tasks, "_connect", lambda: connection)
    monkeypatch.setattr(maintenance_tasks, "_list_tables", lambda *args: [])
    maintenance_tasks.maintain_iceberg_tables(compact_live_files=False)
    assert connection.closed
