"""ESPN Trino manager connects without dynamic filtering (#1557).

A dynamic filter built from an ``IS NOT DISTINCT FROM`` join column drops the
target rows whose value is NULL, so the tombstone MERGE of
``single_statement_replace`` did not delete any row with a NULL column.
"""

from __future__ import annotations

from types import SimpleNamespace

import pytest

import scrapers.base.trino_manager as base
from scrapers.espn.trino_manager import EspnTrinoTableManager


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
@pytest.mark.parametrize("password", [None, "secret"])
def test_connection_disables_dynamic_filtering(monkeypatch, password):
    calls: list[dict] = []
    _fake_trino(monkeypatch, calls)
    if password:
        monkeypatch.setenv("TRINO_PASSWORD", password)
    else:
        monkeypatch.delenv("TRINO_PASSWORD", raising=False)

    manager = EspnTrinoTableManager(host="trino")
    manager._create_connection()

    assert calls[-1]["session_properties"] == {
        "enable_dynamic_filtering": "false"
    }
    assert calls[-1]["host"] == "trino"
    assert calls[-1]["port"] == manager.port
    if password:
        assert calls[-1]["http_scheme"] == "https"
        assert calls[-1]["auth"] == ("airflow", password)
        assert calls[-1]["verify"] is False
    else:
        assert "http_scheme" not in calls[-1]


@pytest.mark.unit
def test_base_manager_is_left_untouched(monkeypatch):
    calls: list[dict] = []
    _fake_trino(monkeypatch, calls)
    monkeypatch.delenv("TRINO_PASSWORD", raising=False)

    base.TrinoTableManager(host="trino")._create_connection()

    assert "session_properties" not in calls[-1]

