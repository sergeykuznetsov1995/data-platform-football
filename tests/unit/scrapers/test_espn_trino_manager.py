"""ESPN Trino manager connects without dynamic filtering (#1557).

A dynamic filter built from an ``IS NOT DISTINCT FROM`` join column drops the
target rows whose value is NULL, so the tombstone MERGE of
``single_statement_replace`` did not delete any row with a NULL column.
"""

from __future__ import annotations

from types import SimpleNamespace

import pytest

import scrapers.base.trino_manager as base
from scrapers.whoscored.trino_budget import WRITE_TIMEOUT
from scrapers.espn.trino_manager import (
    REQUEST_TIMEOUT,
    EspnIcebergWriter,
    EspnTrinoTableManager,
)


def _fake_trino(monkeypatch, calls: list[dict]) -> None:
    monkeypatch.setattr(
        base,
        "trino",
        SimpleNamespace(
            dbapi=SimpleNamespace(connect=lambda **kwargs: calls.append(kwargs)),
            auth=SimpleNamespace(BasicAuthentication=lambda *args: args),
        ),
        raising=False,
    )


@pytest.mark.unit
def test_connection_pins_identity_and_bounds_query_lifetime(monkeypatch):
    calls: list[dict] = []
    _fake_trino(monkeypatch, calls)
    monkeypatch.setenv("TRINO_PASSWORD", "synthetic-generic")
    monkeypatch.setenv("TRINO_USER", "airflow")
    monkeypatch.setenv("TRINO_SRC_ESPN_PASSWORD", "synthetic-dedicated")
    monkeypatch.delenv("TRINO_PORT", raising=False)
    manager = EspnTrinoTableManager(host="trino", user="airflow")
    manager._create_connection()

    assert calls[-1]["session_properties"] == {
        "enable_dynamic_filtering": "false",
        "query_max_run_time": f"{WRITE_TIMEOUT}s",
    }
    assert calls[-1]["host"] == "trino"
    assert calls[-1]["port"] == 8443
    assert calls[-1]["user"] == "src_espn"
    assert calls[-1]["schema"] == "bronze"
    assert calls[-1]["http_scheme"] == "https"
    assert calls[-1]["auth"] == ("src_espn", "synthetic-dedicated")
    assert calls[-1]["verify"] is False
    assert calls[-1]["max_attempts"] == 1
    # Literal replay INSERTs must outrun the driver's 30s default (#2108).
    assert calls[-1]["request_timeout"] == REQUEST_TIMEOUT == 300


@pytest.mark.unit
def test_generic_credentials_cannot_open_connection(monkeypatch):
    calls = []
    _fake_trino(monkeypatch, calls)
    monkeypatch.setenv("TRINO_PASSWORD", "synthetic-generic")
    monkeypatch.delenv("TRINO_SRC_ESPN_PASSWORD", raising=False)
    monkeypatch.setattr(base.TrinoTableManager, "_trino_unreachable", False)
    monkeypatch.setattr(base.time, "sleep", lambda _: pytest.fail("retried missing credential"))
    manager = EspnTrinoTableManager()
    with pytest.raises(base.TrinoError, match="TRINO_SRC_ESPN_PASSWORD"):
        _ = manager.connection
    assert calls == []
    manager._conn = object()
    assert manager.connection is manager._conn


@pytest.mark.unit
def test_espn_writer_reuses_source_manager():
    writer = EspnIcebergWriter(trino_host="example", trino_port=9443)
    manager = writer._get_trino_manager()
    assert writer._get_trino_manager() is manager
    assert isinstance(manager, EspnTrinoTableManager)
    assert (manager.host, manager.port) == ("example", 9443)


@pytest.mark.unit
def test_legacy_scraper_uses_espn_writer_by_default():
    from scrapers.espn.scraper import ESPNScraper

    assert isinstance(ESPNScraper()._iceberg_writer._get_trino_manager(), EspnTrinoTableManager)


@pytest.mark.unit
def test_base_manager_is_left_untouched(monkeypatch):
    calls: list[dict] = []
    _fake_trino(monkeypatch, calls)
    monkeypatch.delenv("TRINO_PASSWORD", raising=False)

    base.TrinoTableManager(host="trino")._create_connection()

    assert "session_properties" not in calls[-1]

