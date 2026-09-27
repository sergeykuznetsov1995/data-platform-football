"""TrinoTableManager connections run without dynamic filtering (#1557).

A dynamic filter built from an ``IS NOT DISTINCT FROM`` join column drops the
target rows whose value is NULL, so the tombstone MERGE of
``single_statement_replace`` did not delete any row with a NULL column.
"""

from __future__ import annotations

from types import SimpleNamespace

import pytest

import scrapers.base.trino_manager as base
from scrapers.base.trino_manager import TrinoTableManager


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

    TrinoTableManager(host="trino")._create_connection()

    assert calls[-1]["session_properties"] == {
        "enable_dynamic_filtering": "false"
    }
    assert ("http_scheme" in calls[-1]) is bool(password)
