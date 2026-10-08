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

class _Authority:
    def __init__(self):
        self.lost = False
        self.checks = 0
        self.watched = 0

    def check(self):
        from scrapers.fotmob.writer_lock import WriterLockLost
        self.checks += 1
        if self.lost:
            raise WriterLockLost("test authority lost")

    def watch(self, cancel):
        from contextlib import contextmanager
        @contextmanager
        def guard():
            self.watched += 1
            self.check()
            try:
                yield
            finally:
                self.check()
        return guard()


class _SQLCursor:
    def __init__(self, calls, failure=None):
        self.calls = calls
        self.failure = failure

    def execute(self, sql, *params):
        self.calls.append(sql)
        if self.failure:
            self.failure(sql)

    def fetchall(self):
        return [[1]]

    def close(self):
        pass

    def cancel(self):
        self.calls.append("CANCEL")


def _guarded_manager(monkeypatch, failure=None):
    monkeypatch.delenv("TRINO_PASSWORD", raising=False)
    manager = FotMobTrinoTableManager(source="fotmob:test")
    calls = []
    def connection():
        return SimpleNamespace(cursor=lambda: _SQLCursor(calls, failure), close=lambda: None)
    manager._conn = connection()
    monkeypatch.setattr(manager, "_create_connection", connection)
    lease = _Authority()
    manager.set_write_authority(lease)
    return manager, lease, calls


def test_lost_authority_prevents_all_subsequent_sql(monkeypatch):
    from scrapers.fotmob.writer_lock import WriterLockLost
    manager, lease, calls = _guarded_manager(monkeypatch)
    lease.lost = True
    for sql in ("INSERT INTO iceberg.bronze.t VALUES (1)", "SELECT 1"):
        with pytest.raises(WriterLockLost):
            manager._execute(sql)
    assert calls == []


def test_connection_retry_does_not_issue_probe_after_authority_loss(monkeypatch):
    from scrapers.fotmob.writer_lock import WriterLockLost
    manager, lease, calls = _guarded_manager(monkeypatch)
    manager._conn = None
    def connect():
        lease.lost = True
        raise OSError("Connection refused")
    monkeypatch.setattr(manager, "_create_connection", connect)
    monkeypatch.setattr("scrapers.fotmob.trino_instrumentation.time.sleep", lambda _: None)
    with pytest.raises(WriterLockLost):
        manager._execute("INSERT INTO iceberg.bronze.t VALUES (1)")
    assert calls == []


def test_query_retry_does_not_continue_after_authority_loss(monkeypatch):
    from scrapers.fotmob.writer_lock import WriterLockLost
    manager, lease, calls = _guarded_manager(monkeypatch)
    def fail(sql):
        lease.lost = True
        raise OSError("Connection reset")
    manager._conn = SimpleNamespace(cursor=lambda: _SQLCursor(calls, fail))
    with pytest.raises(WriterLockLost):
        manager._execute("INSERT INTO iceberg.bronze.t VALUES (1)")
    assert len(calls) == 1
    assert lease.watched == 1


def test_handled_interrupt_drops_stage_only_before_target_mutation(monkeypatch):
    import pandas as pd
    for target_started in (False, True):
        def fail(sql):
            if (target_started and sql.startswith("DELETE FROM")) or (
                not target_started and sql.startswith("INSERT INTO")
            ):
                raise KeyboardInterrupt()
        manager, lease, calls = _guarded_manager(monkeypatch, fail)
        monkeypatch.setattr(manager, "get_table_columns", lambda *args: {"id": "BIGINT"})
        with pytest.raises(KeyboardInterrupt):
            manager.insert_dataframe_atomic("bronze", "fotmob_t", pd.DataFrame({"id": [1]}),
                                            delete_filter="id=1", staging_id="interrupt")
        drops = [sql for sql in calls if sql.startswith("DROP TABLE")]
        # The first DROP clears the retry-stable stage before CREATE.
        assert len(drops) == (1 if target_started else 2)


def test_lost_lease_preserves_stage_and_never_runs_cleanup_sql(monkeypatch):
    import pandas as pd
    from scrapers.fotmob.writer_lock import WriterLockLost
    manager, lease, calls = _guarded_manager(monkeypatch)
    def fail(sql):
        if sql.startswith("INSERT INTO"):
            lease.lost = True
            raise OSError("Connection reset")
    manager._conn = SimpleNamespace(cursor=lambda: _SQLCursor(calls, fail))
    monkeypatch.setattr(manager, "get_table_columns", lambda *args: {"id": "BIGINT"})
    with pytest.raises(WriterLockLost):
        manager.insert_dataframe_atomic("bronze", "fotmob_t", pd.DataFrame({"id": [1]}),
                                        delete_filter="id=1", staging_id="lost")
    assert sum(sql.startswith("DROP TABLE") for sql in calls) == 1
    assert calls[-1].startswith("INSERT INTO")


def test_manager_loss_cannot_be_reset_by_unbinding_guard(monkeypatch):
    from scrapers.fotmob.writer_lock import WriterLockLost
    manager, lease, calls = _guarded_manager(monkeypatch)
    lease.lost = True
    with pytest.raises(WriterLockLost):
        manager._execute("SELECT 1")
    manager.set_write_authority(None)
    with pytest.raises(WriterLockLost):
        manager._execute("SELECT 1")
    assert calls == []


def test_only_planning_reads_allowed_between_production_guards(monkeypatch):
    from scrapers.fotmob.writer_lock import WriterLockBusy
    manager, lease, calls = _guarded_manager(monkeypatch)
    manager.set_write_authority(None)
    assert manager._execute("SELECT 1", fetch=True) == [[1]]
    with pytest.raises(WriterLockBusy):
        manager._execute("-- guarded mutation\nDROP TABLE iceberg.bronze.fotmob_t")
    assert calls == ["SELECT 1"]


def test_actual_heartbeat_cancels_cursor_and_latches_manager_loss(monkeypatch):
    import threading
    from scrapers.fotmob import writer_lock as locks
    monkeypatch.delenv("TRINO_PASSWORD", raising=False)
    connection = SimpleNamespace(owns=True)
    monkeypatch.setattr(locks, "_query", lambda conn, *args: (conn.owns,))
    lease = locks.WriterLease(connection, 12, heartbeat_seconds=.01)
    canceled = threading.Event()
    calls = []
    class Cursor(_SQLCursor):
        def execute(self, sql):
            calls.append(sql)
            connection.owns = False
            assert canceled.wait(1), "active SQL did not receive cancellation"
        def cancel(self):
            canceled.set()
    manager = FotMobTrinoTableManager(source="fotmob:test")
    manager._conn = SimpleNamespace(cursor=lambda: Cursor(calls))
    manager.set_write_authority(lease)
    with pytest.raises(locks.WriterLockLost):
        manager._execute("INSERT INTO iceberg.bronze.fotmob_t VALUES (1)")
    assert canceled.is_set()
    manager.set_write_authority(None)
    with pytest.raises(locks.WriterLockLost):
        manager._execute("SELECT 1")
    assert len(calls) == 1


def test_explain_analyze_mutation_is_not_a_planning_read(monkeypatch):
    from scrapers.fotmob.writer_lock import WriterLockBusy
    manager, lease, calls = _guarded_manager(monkeypatch)
    manager.set_write_authority(None)
    with pytest.raises(WriterLockBusy):
        manager._execute("EXPLAIN ANALYZE INSERT INTO iceberg.bronze.fotmob_t VALUES (1)")
    assert calls == []


def test_signal_cancel_preserves_interruption_and_resets_connection(monkeypatch):
    class Terminated(BaseException):
        pass
    manager, lease, calls = _guarded_manager(monkeypatch)
    def terminate(sql):
        assert manager.cancel_active_query() is True
        raise Terminated("TERM")
    manager._conn = SimpleNamespace(cursor=lambda: _SQLCursor(calls, terminate), close=lambda: None)
    with pytest.raises(Terminated, match="TERM"):
        manager._execute("INSERT INTO iceberg.bronze.fotmob_t VALUES (1)")
    assert manager._conn is None
    assert manager._write_failure is None
    assert manager.cancel_active_query() is False
    assert calls[-1] == "CANCEL"
    manager._conn = SimpleNamespace(cursor=lambda: _SQLCursor(calls))
    manager._execute("INSERT INTO iceberg.bronze.fotmob_manifest VALUES (1)")
    assert len(calls) == 3


def test_commit_conflict_retry_checks_authority_again(monkeypatch):
    manager, lease, calls = _guarded_manager(monkeypatch)
    from scrapers.fotmob.writer_lock import WriterLockLost
    def conflict(sql):
        raise RuntimeError("ICEBERG_COMMIT_ERROR: Found conflicting files")
    manager._conn = SimpleNamespace(cursor=lambda: _SQLCursor(calls, conflict))
    monkeypatch.setattr("scrapers.base.trino_manager.time.sleep", lambda _: setattr(lease, "lost", True))
    with pytest.raises(WriterLockLost):
        manager._execute_committing("MERGE INTO iceberg.bronze.fotmob_t USING x ON true WHEN MATCHED THEN DELETE")
    assert len(calls) == 1


def test_mutation_response_loss_is_not_blindly_replayed(monkeypatch):
    from scrapers.base.trino_manager import TrinoError
    manager, lease, calls = _guarded_manager(monkeypatch)
    def lost_response(sql):
        raise OSError("Connection reset after dispatch")
    manager._conn = SimpleNamespace(cursor=lambda: _SQLCursor(calls, lost_response))
    with pytest.raises(TrinoError, match="Connection reset"):
        manager._execute("INSERT INTO iceberg.bronze.fotmob_t VALUES (1)")
    assert calls == ["INSERT INTO iceberg.bronze.fotmob_t VALUES (1)"]


def test_fotmob_http_transport_has_no_hidden_statement_retries(monkeypatch):
    calls = []
    _fake_trino(monkeypatch, calls)
    manager = FotMobTrinoTableManager(source="fotmob:test")
    manager._create_connection()
    assert calls[-1]["max_attempts"] == 1
    assert calls[-1]["request_timeout"] == (3, 5)


@pytest.mark.parametrize('failure_check', [1, 2])
def test_repository_boundary_loss_irrevocably_revokes_manager(monkeypatch, failure_check):
    from contextlib import nullcontext
    from types import SimpleNamespace
    from scrapers.fotmob.repository import FotMobRepository
    from scrapers.fotmob.writer_lock import WriterLockLost
    manager, _, calls = _guarded_manager(monkeypatch)
    class BoundaryLease:
        checks = 0
        def check(self):
            self.checks += 1
            if self.checks >= failure_check:
                raise WriterLockLost('boundary ownership lost')
    lease = BoundaryLease()
    repository = FotMobRepository(
        writer=SimpleNamespace(_get_trino_manager=lambda: manager),
        write_guard=lambda: nullcontext(lease),
    )
    with pytest.raises(WriterLockLost):
        with repository._guarded_write():
            pass
    with pytest.raises(WriterLockLost):
        manager._execute('SELECT 1', fetch=True)
    assert calls == []
