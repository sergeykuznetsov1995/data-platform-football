"""Тесты ночной компакции мелких файлов bronze FotMob (#1284).

Компакция ходит в Trino только через инжектируемый менеджер, в метабазу — через
инжектируемый пробник волны, а замок писателя берёт инжектируемым
contextmanager'ом: здесь ни одного живого соединения.
"""

from __future__ import annotations

import importlib
import json
import logging
import subprocess
import sys
from contextlib import contextmanager
from datetime import datetime, timedelta, timezone
from pathlib import Path
from types import SimpleNamespace

import pytest

from scrapers.fotmob import maintenance
from scrapers.fotmob.repository import TABLE_PARTITIONS


REPO_ROOT = Path(__file__).resolve().parents[3]
WRAPPER = REPO_ROOT / "deploy" / "fotmob" / "compaction.sh"

MANIFEST = "fotmob_ingest_manifest"


def _at(hour: int, minute: int) -> datetime:
    return datetime(2026, 9, 18, hour, minute, tzinfo=timezone.utc)


class _Clock:
    """Часы прогона: каждый вызов сдвигает время на ``step``."""

    def __init__(self, start: datetime, step: timedelta = timedelta(0)) -> None:
        self.value = start
        self.step = step
        self.calls = 0

    def __call__(self) -> datetime:
        current = self.value
        self.calls += 1
        self.value = current + self.step
        return current


class _FakeManager:
    """Фейковый Trino: держит содержимое ``$files`` и склеивает по ALTER."""

    def __init__(self, files: dict[str, list[tuple[str, str, int]]]) -> None:
        # table -> list of (partition, path, size)
        self.files = {table: list(rows) for table, rows in files.items()}
        self.queries: list[str] = []
        self.events: list[str] = []
        self.fail_on_alter = False
        self.merged = 0
        self._last = None

    @property
    def connection(self):
        """Соединение компакции: курсор ходит в ту же фейковую таблицу."""

        self.events.append("connection")
        manager = self

        class _Cursor:
            def execute(self, sql: str) -> None:
                manager.events.append("execute")
                manager._last = manager.execute_query(sql)

            def fetchall(self):
                return manager._last

            def close(self) -> None:
                manager.events.append("cursor_closed")

        return SimpleNamespace(cursor=_Cursor)

    def _table_of(self, sql: str) -> str:
        for table in self.files:
            if table in sql:
                return table
        raise AssertionError(f"неизвестная таблица в SQL: {sql[:120]}")

    def execute_query(self, sql: str):
        self.queries.append(sql)
        table = self._table_of(sql)
        if sql.lstrip().upper().startswith("ALTER"):
            if self.fail_on_alter:
                raise RuntimeError("Trino query exceeded maximum execution time")
            paths = {
                literal.strip().strip("'")
                for literal in sql.split(" IN (", 1)[1].rsplit(")", 1)[0].split(",")
            }
            rows = self.files[table]
            merged_size = sum(size for _p, path, size in rows if path in paths)
            partition = next(part for part, path, _s in rows if path in paths)
            self.files[table] = [row for row in rows if row[1] not in paths]
            self.merged += 1
            self.files[table].append(
                (partition, f"merged-{self.merged}.parquet", merged_size)
            )
            return [(len(paths), 1)]
        if "count(*)" in sql:
            rows = self.files[table]
            return [(len(rows), sum(size for _p, _path, size in rows))]
        return [
            (partition, path, size)
            for partition, path, size in self.files[table]
            if size < maintenance.SMALL_FILE_MAX_BYTES
        ]


def _files(partition: str, count: int, *, size: int = 10_000, prefix: str = "f"):
    return [
        (partition, f"s3://bucket/{prefix}-{partition}-{index:05d}.parquet", size)
        for index in range(count)
    ]


def _run(manager, *, now, tables=(MANIFEST,), **kwargs):
    lock_entries = kwargs.pop("lock_entries", [])
    pauses = kwargs.pop("pauses", [])
    notifications = kwargs.pop("notifications", [])

    @contextmanager
    def _lock():
        lock_entries.append(1)
        if hasattr(manager, "events"):
            manager.events.append("lock")
        yield True

    defaults = dict(
        tables=tables,
        now=now,
        manager_factory=lambda: manager,
        wave_probe=lambda: (False, None),
        writer_lock=kwargs.pop("writer_lock", _lock),
        sleep=lambda seconds: pauses.append(seconds),
        notify=lambda message: notifications.append(message),
    )
    defaults.update(kwargs)
    return maintenance.run(**defaults)


@pytest.mark.unit
class TestWindow:
    @pytest.mark.parametrize("moment", [_at(23, 29), _at(23, 50), _at(2, 0)])
    def test_outside_the_window_nothing_is_queried(self, moment):
        manager = _FakeManager({MANIFEST: _files("match", 10)})

        result = _run(manager, now=_Clock(moment))

        assert result["stopped"] == "out_of_window"
        assert maintenance.exit_code(result) == 0
        assert manager.queries == []

    def test_inside_the_window_the_run_works(self):
        manager = _FakeManager({MANIFEST: _files("match", 10)})

        result = _run(manager, now=_Clock(_at(23, 31)))

        assert result["stopped"] == "complete"
        assert result["chunks"] == 1
        assert manager.queries


@pytest.mark.unit
class TestWaveBarrier:
    def test_running_wave_is_polled_until_the_window_ends(self):
        manager = _FakeManager({MANIFEST: _files("match", 10)})
        pauses: list[float] = []

        result = _run(
            manager,
            now=_Clock(_at(23, 31), timedelta(minutes=5)),
            wave_probe=lambda: (True, None),
            pauses=pauses,
        )

        assert result["stopped"] == "wave_running"
        assert maintenance.exit_code(result) == 0
        assert manager.queries == []
        assert pauses and set(pauses) == {maintenance.WAVE_POLL_SECONDS}

    def test_recent_wave_end_defers_the_run(self):
        manager = _FakeManager({MANIFEST: _files("match", 10)})

        result = _run(
            manager,
            now=_Clock(_at(23, 40)),
            wave_probe=lambda: (False, _at(23, 20)),
        )

        assert result["stopped"] == "delivery_tail"
        assert maintenance.exit_code(result) == 0
        assert manager.queries == []

    def test_old_wave_end_does_not_defer_the_run(self):
        manager = _FakeManager({MANIFEST: _files("match", 4)})

        result = _run(
            manager,
            now=_Clock(_at(23, 40)),
            wave_probe=lambda: (False, _at(22, 55)),
        )

        assert result["stopped"] == "complete"


@pytest.mark.unit
class TestChunkPlanning:
    def test_chunks_respect_every_bound_and_never_mix_partitions(self):
        candidates = _files("player", 1_000, size=1024, prefix="p" * 200) + _files(
            "match", 1_500, size=1024, prefix="m" * 200
        )

        chunks = maintenance.plan_chunks(MANIFEST, candidates)

        assert [chunk["partition"] for chunk in chunks][0] == "match"
        for chunk in chunks:
            assert 2 <= chunk["files"] <= maintenance.MAX_FILES_PER_CHUNK
            assert chunk["bytes"] <= maintenance.MAX_INPUT_BYTES_PER_CHUNK
            assert chunk["sql_bytes"] <= maintenance.MAX_SQL_BYTES
            assert len(chunk["paths"]) == chunk["files"]
            assert len({path.split("-")[1] for path in chunk["paths"]}) == 1
        assert sum(chunk["files"] for chunk in chunks) == 2_500

    def test_input_bytes_bound_splits_a_partition(self):
        candidates = _files("match", 20, size=8 * 1024 * 1024 - 1)

        chunks = maintenance.plan_chunks(MANIFEST, candidates)

        assert len(chunks) > 1
        for chunk in chunks:
            assert chunk["bytes"] <= maintenance.MAX_INPUT_BYTES_PER_CHUNK

    def test_smallest_files_go_first(self):
        candidates = [
            ("match", "s3://bucket/big.parquet", 7_000_000),
            ("match", "s3://bucket/small.parquet", 1_000),
            ("match", "s3://bucket/mid.parquet", 500_000),
        ]

        chunks = maintenance.plan_chunks(MANIFEST, candidates)

        assert chunks[0]["paths"][0] == "s3://bucket/small.parquet"

    def test_large_files_and_lonely_partitions_are_skipped(self):
        candidates = [
            ("match", "s3://bucket/huge-1.parquet", maintenance.SMALL_FILE_MAX_BYTES),
            ("match", "s3://bucket/huge-2.parquet", maintenance.SMALL_FILE_MAX_BYTES + 1),
            ("player", "s3://bucket/lonely.parquet", 1_000),
        ]

        assert maintenance.plan_chunks(MANIFEST, candidates) == []

    def test_discovery_sql_filters_data_files_below_the_small_threshold(self):
        sql = maintenance.candidates_sql(MANIFEST)

        assert f'iceberg.bronze."{MANIFEST}$files"' in sql
        assert "content = 0" in sql
        assert f"file_size_in_bytes < {maintenance.SMALL_FILE_MAX_BYTES}" in sql
        assert "CAST(partition AS json)" in sql


@pytest.mark.unit
class TestChunkSql:
    def test_sql_is_an_exact_path_optimize(self):
        paths = ["s3://bucket/a.parquet", "s3://bucket/o'brien.parquet"]

        sql = maintenance.chunk_sql(MANIFEST, paths)

        assert sql.startswith(f'ALTER TABLE iceberg.bronze."{MANIFEST}" EXECUTE optimize(')
        assert (
            f"file_size_threshold => '{maintenance.FILE_SIZE_THRESHOLD}'" in sql
        )
        assert (
            sql.endswith(
                'WHERE "$path" IN (\'s3://bucket/a.parquet\','
                "'s3://bucket/o''brien.parquet')"
            )
        )

    def test_invalid_path_is_refused(self):
        with pytest.raises(RuntimeError):
            maintenance.chunk_sql(MANIFEST, ["s3://bucket/a.parquet", "bad\npath"])


@pytest.mark.unit
class TestDeadline:
    def test_no_chunk_starts_after_the_cutoff(self):
        manager = _FakeManager({MANIFEST: _files("match", 2_500, size=1024)})

        result = _run(manager, now=_Clock(_at(23, 41), timedelta(minutes=1)))

        assert result["stopped"] == "deadline"
        assert result["chunks"] == 1
        assert manager.merged == 1

    def test_lock_taken_after_the_cutoff_starts_no_chunk(self):
        """Замок ждётся до 600 с — время проверяется ещё раз уже под замком."""

        manager = _FakeManager({MANIFEST: _files("match", 10)})
        clock = _Clock(_at(23, 43))

        @contextmanager
        def _slow_lock():
            # Ожидание замка съело остаток окна.
            clock.value = _at(23, 46)
            yield True

        result = _run(manager, now=clock, writer_lock=_slow_lock)

        assert result["stopped"] == "deadline"
        assert result["chunks"] == 0
        assert manager.merged == 0
        assert not any(
            query.lstrip().upper().startswith("ALTER") for query in manager.queries
        )


@pytest.mark.unit
class TestWriterLock:
    def test_every_chunk_runs_under_the_lock_with_a_pause_between_chunks(self):
        manager = _FakeManager({MANIFEST: _files("match", 2_500, size=1024)})
        entries: list[int] = []
        pauses: list[float] = []

        result = _run(
            manager,
            now=_Clock(_at(23, 31)),
            lock_entries=entries,
            pauses=pauses,
        )

        assert result["chunks"] == 3
        assert len(entries) == result["chunks"]
        assert pauses == [maintenance.CHUNK_PAUSE_SECONDS] * result["chunks"]

    def test_connection_is_opened_before_the_lock_and_one_query_runs_under_it(self):
        """Под замком — ровно один запрос: переподключения базового клиента вне замка."""

        manager = _FakeManager({MANIFEST: _files("match", 10)})

        result = _run(manager, now=_Clock(_at(23, 31)))

        assert result["chunks"] == 1
        assert manager.events == ["connection", "lock", "execute", "cursor_closed"]

    def test_busy_lock_stops_the_run_green(self, caplog):
        from dags.scripts.run_fotmob_scraper import WriterLockBusy

        manager = _FakeManager({MANIFEST: _files("match", 10)})

        @contextmanager
        def _busy():
            raise WriterLockBusy("занято")
            yield  # pragma: no cover

        with caplog.at_level(logging.WARNING):
            result = _run(manager, now=_Clock(_at(23, 31)), writer_lock=_busy)

        assert result["stopped"] == "lock_busy"
        assert maintenance.exit_code(result) == 0
        assert result["chunks"] == 0
        assert any(
            record.levelno == logging.WARNING and "lock" in record.getMessage().lower()
            for record in caplog.records
        )


@pytest.mark.unit
class TestFailure:
    def test_trino_error_is_red_and_alerts_once(self):
        manager = _FakeManager({MANIFEST: _files("match", 2_500, size=1024)})
        notifications: list[str] = []

        def _factory():
            return manager

        original = manager.execute_query
        state = {"alters": 0}

        def _execute(sql: str):
            if sql.lstrip().upper().startswith("ALTER"):
                state["alters"] += 1
                if state["alters"] == 2:
                    raise RuntimeError("Trino query exceeded maximum execution time")
            return original(sql)

        manager.execute_query = _execute

        result = _run(
            manager,
            now=_Clock(_at(23, 31)),
            manager_factory=_factory,
            notifications=notifications,
        )

        assert result["stopped"] == "error"
        assert maintenance.exit_code(result) == 1
        assert result["chunks"] == 1
        assert len(notifications) == 1
        assert MANIFEST in notifications[0]

    def test_broken_alert_does_not_swallow_the_failed_run(self):
        manager = _FakeManager({MANIFEST: _files("match", 10)})

        def _boom_alert(_message):
            raise ModuleNotFoundError("utils.alerts")

        def _failing_query(sql: str):
            if sql.lstrip().upper().startswith("ALTER"):
                raise RuntimeError("Trino query exceeded maximum execution time")
            return original(sql)

        original = manager.execute_query
        manager.execute_query = _failing_query

        result = _run(manager, now=_Clock(_at(23, 31)), notify=_boom_alert)

        assert result["stopped"] == "error"
        assert maintenance.exit_code(result) == 1

    def test_alert_works_on_the_bare_cli_sys_path(self, monkeypatch):
        """CLI стартует с PYTHONPATH=<корень>, а utils.alerts лежит в dags/utils."""

        dags_dir = str(REPO_ROOT / "dags")
        monkeypatch.setattr(
            sys, "path", [entry for entry in sys.path if entry != dags_dir]
        )
        for name in [
            name
            for name in list(sys.modules)
            if name == "utils" or name.startswith("utils.")
        ]:
            monkeypatch.delitem(sys.modules, name)
        monkeypatch.delenv("TELEGRAM_BOT_TOKEN", raising=False)
        monkeypatch.delenv("TELEGRAM_CHAT_ID", raising=False)

        # Без правки пути CLI получил бы здесь ModuleNotFoundError.
        with pytest.raises(ModuleNotFoundError):
            importlib.import_module("utils.alerts")

        maintenance.notify("compaction failed")

        assert dags_dir in sys.path
        assert "utils.alerts" in sys.modules


@pytest.mark.unit
class TestMetricsAndOutput:
    def test_metrics_and_final_json(self, capsys, caplog):
        manager = _FakeManager({MANIFEST: _files("match", 100, size=1024)})

        with caplog.at_level(logging.INFO):
            result = _run(manager, now=_Clock(_at(23, 31)))
            maintenance.report(result)

        assert result["files_before"] == 100
        assert result["files_after"] == 1
        assert result["bytes_before"] == 100 * 1024
        assert result["bytes_after"] == 100 * 1024
        assert result["tables"][0]["table"] == MANIFEST
        assert result["tables"][0]["chunks"] == 1

        printed = json.loads(capsys.readouterr().out.strip().splitlines()[-1])
        assert printed["stopped"] == "complete"
        assert printed["files_before"] == 100
        assert printed["files_after"] == 1
        assert any(
            record.getMessage().startswith("FotMob compaction done:")
            for record in caplog.records
        )
        assert any(
            record.getMessage().startswith("FotMob compaction: table=")
            for record in caplog.records
        )

    def test_unmeasured_table_is_not_reported_as_complete(self):
        manager = _FakeManager({MANIFEST: _files("match", 10)})
        original = manager.execute_query
        state = {"metrics": 0}

        def _execute(sql: str):
            if "count(*)" in sql:
                state["metrics"] += 1
                if state["metrics"] == 2:
                    raise RuntimeError("Trino is gone")
            return original(sql)

        manager.execute_query = _execute

        result = _run(manager, now=_Clock(_at(23, 31)))

        assert result["stopped"] == "metrics_unavailable"
        assert maintenance.exit_code(result) == 0
        assert result["metrics_incomplete"] == [MANIFEST]
        assert result["files_before"] == result["files_after"] == 0

    def test_complete_when_nothing_is_small_enough(self):
        manager = _FakeManager(
            {MANIFEST: _files("match", 3, size=maintenance.SMALL_FILE_MAX_BYTES + 5)}
        )

        result = _run(manager, now=_Clock(_at(23, 31)))

        assert result["stopped"] == "complete"
        assert result["chunks"] == 0
        assert result["files_before"] == result["files_after"] == 3


@pytest.mark.unit
class TestCompactionConnection:
    def test_manager_is_signed_and_capped_by_the_server(self, monkeypatch):
        from scrapers.base import trino_manager as base
        from scrapers.fotmob.trino_instrumentation import FotMobTrinoTableManager

        calls: list[dict] = []
        monkeypatch.delenv("TRINO_PASSWORD", raising=False)
        monkeypatch.setattr(
            base,
            "trino",
            SimpleNamespace(
                dbapi=SimpleNamespace(connect=lambda **kwargs: calls.append(kwargs)),
                auth=SimpleNamespace(BasicAuthentication=lambda *a: None),
            ),
            raising=False,
        )

        manager = maintenance.build_compaction_manager()
        manager._create_connection()

        assert isinstance(manager, FotMobTrinoTableManager)
        assert calls[-1]["source"] == "fotmob:maintenance"
        assert calls[-1]["session_properties"] == {
            "query_max_run_time": maintenance.QUERY_MAX_RUN_TIME,
            "query_max_execution_time": maintenance.QUERY_MAX_EXECUTION_TIME,
        }


@pytest.mark.unit
class TestTableList:
    def test_tables_are_known_and_ordered_by_measured_cost(self):
        assert set(maintenance.FOTMOB_COMPACTION_TABLES) <= set(TABLE_PARTITIONS)
        assert maintenance.FOTMOB_COMPACTION_TABLES[:3] == (
            "fotmob_ingest_manifest",
            "fotmob_field_inventory",
            "fotmob_squad_snapshots",
        )


@pytest.mark.unit
class TestWrapper:
    def test_wrapper_is_valid_bash(self):
        assert subprocess.run(["bash", "-n", str(WRAPPER)], check=False).returncode == 0

    def test_wrapper_argv_has_no_runner_mines(self):
        text = WRAPPER.read_text(encoding="utf-8")

        assert "dags/scripts/run_fotmob_scrape" not in text
        assert "run-id fotmob-hist-" not in text
        assert "python -m scrapers.fotmob.maintenance" in text
