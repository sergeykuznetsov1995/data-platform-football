"""Тесты инструментирования Trino-клиента FotMob (#1284).

Покрывают три вещи: подпись запросов волны в очереди Trino (``source=``),
слоу-лог запросов дольше порога и врезку писателя в раннер.
"""

from __future__ import annotations

import importlib
import logging
from types import SimpleNamespace

import pytest

from scrapers.base.trino_manager import TrinoTableManager
from scrapers.fotmob.trino_instrumentation import (
    FOTMOB_SLOW_SQL_SECONDS,
    FotMobIcebergWriter,
    FotMobTrinoTableManager,
    fotmob_query_source,
)


def _fake_trino(monkeypatch, calls):
    """Подменить клиент Trino в базовом модуле и собирать kwargs connect()."""

    from scrapers.base import trino_manager as base

    def _connect(**kwargs):
        calls.append(kwargs)
        return SimpleNamespace(cursor=lambda: None)

    monkeypatch.setattr(
        base,
        "trino",
        SimpleNamespace(
            dbapi=SimpleNamespace(connect=_connect),
            auth=SimpleNamespace(
                BasicAuthentication=lambda user, password: ("basic", user, password)
            ),
        ),
        raising=False,
    )
    return calls


@pytest.mark.unit
class TestSource:
    def test_source_is_current_or_history_by_run_id(self):
        assert fotmob_query_source("abc-123") == "fotmob:current:abc-123"
        assert (
            fotmob_query_source("fotmob-hist-2019-uefa")
            == "fotmob:history:fotmob-hist-2019-uefa"
        )

    def test_connection_carries_source_and_session_properties(self, monkeypatch):
        calls: list[dict] = []
        _fake_trino(monkeypatch, calls)
        monkeypatch.delenv("TRINO_PASSWORD", raising=False)

        manager = FotMobTrinoTableManager(
            source="fotmob:maintenance",
            session_properties={"query_max_execution_time": "180s"},
            host="trino",
            port=8080,
        )
        manager._create_connection()

        assert calls[-1]["source"] == "fotmob:maintenance"
        assert calls[-1]["session_properties"] == {
            "query_max_execution_time": "180s"
        }

    def test_connection_without_password_has_no_session_properties_key(
        self, monkeypatch
    ):
        calls: list[dict] = []
        _fake_trino(monkeypatch, calls)
        monkeypatch.delenv("TRINO_PASSWORD", raising=False)

        manager = FotMobTrinoTableManager(source="fotmob:current:x", port=8080)
        manager._create_connection()

        assert calls[-1]["source"] == "fotmob:current:x"
        assert "session_properties" not in calls[-1]
        assert "http_scheme" not in calls[-1]

    def test_connection_with_password_keeps_the_https_branch(self, monkeypatch):
        calls: list[dict] = []
        _fake_trino(monkeypatch, calls)
        monkeypatch.setenv("TRINO_PASSWORD", "secret")

        manager = FotMobTrinoTableManager(source="fotmob:current:x", port=8443)
        manager._create_connection()

        assert calls[-1]["http_scheme"] == "https"
        assert calls[-1]["auth"] == ("basic", "airflow", "secret")
        assert calls[-1]["verify"] is False
        assert calls[-1]["source"] == "fotmob:current:x"


@pytest.mark.unit
class TestSlowLog:
    @staticmethod
    def _manager(monkeypatch, elapsed, result="rows"):
        monkeypatch.delenv("TRINO_PASSWORD", raising=False)
        monkeypatch.setattr(
            TrinoTableManager,
            "_execute",
            lambda self, sql, fetch=False, params=None: result,
        )
        mod = importlib.import_module("scrapers.fotmob.trino_instrumentation")
        ticks = iter([0.0, float(elapsed)])
        monkeypatch.setattr(mod, "_now_seconds", lambda: next(ticks))
        return FotMobTrinoTableManager(source="fotmob:current:x", port=8080)

    def test_slow_query_logs_one_normalized_warning(self, monkeypatch, caplog):
        manager = self._manager(monkeypatch, FOTMOB_SLOW_SQL_SECONDS + 1.5)
        sql = (
            "SELECT   *\n  FROM iceberg.bronze.fotmob_ingest_manifest\n"
            "WHERE target_type IN ('a', 'b', 'c') AND x = 1 " + "-- padding " * 200
        )

        with caplog.at_level(logging.WARNING):
            assert manager._execute(sql, fetch=True) == "rows"

        warnings = [
            record
            for record in caplog.records
            if record.levelno == logging.WARNING
            and record.getMessage().startswith("FotMob slow SQL")
        ]
        assert len(warnings) == 1
        message = warnings[0].getMessage()
        assert "IN (…3…)" in message
        assert "\n" not in message
        assert len(message.split("sql=", 1)[1]) <= 400

    def test_fast_query_logs_nothing(self, monkeypatch, caplog):
        manager = self._manager(monkeypatch, FOTMOB_SLOW_SQL_SECONDS - 0.5)

        with caplog.at_level(logging.WARNING):
            manager._execute("SELECT 1")

        assert [
            record
            for record in caplog.records
            if record.getMessage().startswith("FotMob slow SQL")
        ] == []

    def test_failed_slow_query_is_logged_too(self, monkeypatch, caplog):
        monkeypatch.delenv("TRINO_PASSWORD", raising=False)

        def _boom(self, sql, fetch=False, params=None):
            raise RuntimeError("trino down")

        monkeypatch.setattr(TrinoTableManager, "_execute", _boom)
        mod = importlib.import_module("scrapers.fotmob.trino_instrumentation")
        ticks = iter([0.0, FOTMOB_SLOW_SQL_SECONDS + 0.1])
        monkeypatch.setattr(mod, "_now_seconds", lambda: next(ticks))
        manager = FotMobTrinoTableManager(source="fotmob:current:x", port=8080)

        with caplog.at_level(logging.WARNING):
            with pytest.raises(RuntimeError):
                manager._execute("SELECT 1")

        assert any(
            record.getMessage().startswith("FotMob slow SQL")
            for record in caplog.records
        )


@pytest.mark.unit
class TestWriter:
    def test_writer_builds_instrumented_manager_once(self, monkeypatch):
        monkeypatch.delenv("TRINO_PASSWORD", raising=False)
        writer = FotMobIcebergWriter(run_id="fotmob-hist-2019-uefa")

        manager = writer._get_trino_manager()

        assert isinstance(manager, FotMobTrinoTableManager)
        assert manager._source == "fotmob:history:fotmob-hist-2019-uefa"
        assert writer._get_trino_manager() is manager

    def test_writer_keeps_the_default_catalog(self, monkeypatch):
        monkeypatch.delenv("TRINO_PASSWORD", raising=False)
        writer = FotMobIcebergWriter(run_id="run-1")

        assert writer.catalog == "iceberg"
        assert writer._get_trino_manager().catalog == "iceberg"


@pytest.mark.unit
class TestRunnerWiring:
    def test_native_service_writer_is_instrumented_with_the_run_id(
        self, monkeypatch
    ):
        """Раннер строит репозиторий с нашим писателем и run_id из параметра."""

        from scrapers.fotmob import raw_store as raw_store_mod
        from scrapers.fotmob import repository as repository_mod
        from scrapers.fotmob import service as service_mod
        from scrapers.fotmob import transport as transport_mod

        mod = importlib.import_module("dags.scripts.run_fotmob_scraper")
        monkeypatch.setenv(mod.FOTMOB_SHARED_RPM_ENV, "0")
        monkeypatch.delenv("TRINO_PASSWORD", raising=False)

        captured: dict = {}

        class _Repository:
            def __init__(self, **kwargs):
                captured.update(kwargs)

        monkeypatch.setattr(repository_mod, "FotMobRepository", _Repository)
        monkeypatch.setattr(
            transport_mod, "FotMobTransport", lambda **kwargs: SimpleNamespace(**kwargs)
        )
        monkeypatch.setattr(
            service_mod,
            "FotMobIngestService",
            lambda **kwargs: SimpleNamespace(**kwargs),
        )
        monkeypatch.setattr(
            raw_store_mod.FotMobRawStore,
            "from_uri",
            classmethod(lambda cls, uri: SimpleNamespace(uri=uri)),
        )

        args = SimpleNamespace(
            raw_store_uri="file:///tmp/fotmob-raw",
            requests_per_minute=30,
            workers=2,
            max_attempts=3,
            commit_batch_size=50,
            max_buffered_rows=1000,
            max_requests=100,
            max_direct_mib=1,
            max_proxy_mib=1,
            mode="refresh",
            run_id=None,
        )

        service, raw_store = mod._build_native_service(args, "run-from-parameter")

        writer = captured["writer"]
        assert isinstance(writer, FotMobIcebergWriter)
        assert writer.run_id == "run-from-parameter"
        assert captured["write_guard"] is mod._writer_lock
