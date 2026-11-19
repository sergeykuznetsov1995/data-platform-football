"""Deterministic per-table pacing, abandoned reservations and shared snapshots."""

import multiprocessing
import os
from unittest.mock import patch

import pytest

from scrapers.whoscored import trino
from scrapers.whoscored.trino_budget import INTERVAL, WRITE_TIMEOUT, TrinoBudget


class Clock:
    def __init__(self, now=0):
        self.now = now

    def __call__(self):
        return self.now

    def sleep(self, seconds):
        assert seconds > 0
        self.now += seconds


def test_cross_lane_pacing_at_minute_boundary_and_after_slow_commit(tmp_path):
    clock = Clock(59.9)
    current = TrinoBudget(tmp_path, clock=clock, sleep=clock.sleep)
    history = TrinoBudget(tmp_path, clock=clock, sleep=clock.sleep)
    with current.statement(["commit:a"]):
        clock.now = 60
    clock.now = 60.1
    with history.statement(["commit:b", "poll:a"]):
        assert clock.now == 60.1
    with history.statement(["commit:a"]):
        assert clock.now == 120
        clock.now += 25
    with current.statement(["poll:a", "commit:a", "commit:a"]):
        assert clock.now == 205


def _abandon(root):
    with TrinoBudget(root, clock=Clock(10)).statement(["poll:m", "commit:a"]):
        os._exit(0)


def test_killed_writer_preserves_pacing_across_process_restart(tmp_path):
    process = multiprocessing.get_context("spawn").Process(
        target=_abandon, args=(tmp_path,)
    )
    process.start()
    process.join(20)
    assert process.exitcode == 0
    clock = Clock(11)
    budget = TrinoBudget(tmp_path, clock=clock, sleep=clock.sleep)
    with budget.statement(["commit:b"]):
        assert clock.now == 11
    with budget.statement(["poll:m"]):
        assert clock.now == 11 + INTERVAL
    with budget.statement(["commit:a"]):
        assert clock.now == 10 + WRITE_TIMEOUT + INTERVAL


def test_unknown_commit_is_not_replayed_and_definite_rejection_can_retry(tmp_path):
    clock = Clock()
    budget = TrinoBudget(tmp_path, clock=clock, sleep=clock.sleep)
    with pytest.raises(OSError):
        with budget.statement(["commit:a", "poll:m"]):
            raise OSError("outcome unknown")
    with budget.statement(["poll:m"]):
        assert clock.now == 60
    with pytest.raises(ValueError):
        with budget.statement(
            ["commit:b"], rejected=lambda exc: isinstance(exc, ValueError)
        ):
            raise ValueError("definitely rejected")
    with budget.statement(["commit:b"]):
        assert clock.now == 120
    with budget.statement(["commit:a"]):
        assert clock.now >= WRITE_TIMEOUT + INTERVAL


def _competing_lane(root, start, first_inside, release, result):
    clock = Clock(59.9)
    budget = TrinoBudget(root, clock=clock, sleep=clock.sleep)
    start.wait(20)
    with budget.statement(["poll:manifest"]):
        result.send(clock.now)
        first_inside.set()
        assert release.wait(20)


def test_contending_processes_cannot_poll_same_manifest_together(tmp_path):
    ctx = multiprocessing.get_context("spawn")
    start, inside, release = (ctx.Event() for _ in range(3))
    readers, processes = [], []
    for _ in range(2):
        reader, writer = ctx.Pipe(duplex=False)
        process = ctx.Process(
            target=_competing_lane, args=(tmp_path, start, inside, release, writer)
        )
        process.start()
        readers.append(reader)
        processes.append(process)
        writer.close()
    try:
        start.set()
        assert inside.wait(20)
        release.set()
        for process in processes:
            process.join(20)
            assert process.exitcode == 0
        assert sorted(reader.recv() for reader in readers) == pytest.approx(
            [59.9, 119.9]
        )
    finally:
        release.set()
        for process in processes:
            if process.is_alive():
                process.terminate()
                process.join(20)
        for reader in readers:
            reader.close()


def test_snapshot_expires_and_refresh_failure_is_not_published(tmp_path):
    clock = Clock(10)
    one = TrinoBudget(tmp_path, clock=clock, sleep=clock.sleep)
    two = TrinoBudget(tmp_path, clock=clock, sleep=clock.sleep)
    assert one.snapshot("inventory", lambda: [1]) == [1]
    clock.now = 69.99
    assert two.snapshot("inventory", lambda: pytest.fail("cached")) == [1]
    clock.now = 70
    with pytest.raises(OSError):
        two.snapshot("inventory", lambda: (_ for _ in ()).throw(OSError("read failed")))
    assert one.snapshot("inventory", lambda: [2]) == [2]


@pytest.mark.parametrize(
    ("sql", "keys"),
    [
        (
            "SELECT * FROM iceberg.bronze.whoscored_match_ingest_latest",
            {"poll:iceberg.bronze.whoscored_match_ingest_manifest"},
        ),
        (
            "SELECT * FROM bronze.whoscored_schedule_current s JOIN bronze.whoscored_preview_ingest_manifest p ON true",
            {
                "poll:iceberg.bronze.whoscored_scope_ingest_manifest",
                "poll:iceberg.bronze.whoscored_preview_ingest_manifest",
            },
        ),
        (
            'INSERT INTO "iceberg"."bronze"."whoscored_matches" VALUES (1)',
            {"commit:iceberg.bronze.whoscored_matches"},
        ),
        (
            "DELETE FROM bronze.whoscored_scope_ingest_manifest WHERE false",
            {
                "commit:iceberg.bronze.whoscored_scope_ingest_manifest",
                "poll:iceberg.bronze.whoscored_scope_ingest_manifest",
            },
        ),
        (
            "SELECT 'from iceberg.bronze.whoscored_match_ingest_manifest' -- from bronze.whoscored_scope_ingest_manifest",
            set(),
        ),
        (
            "CREATE OR REPLACE VIEW bronze.v AS SELECT * FROM bronze.whoscored_match_ingest_manifest",
            set(),
        ),
    ],
)
def test_physical_slots_for_repository_sql(sql, keys):
    assert trino.statement_keys(sql, "iceberg") == keys


def test_sql_and_direct_iceberg_share_the_same_table_slot(tmp_path, monkeypatch):
    clock = Clock(59.9)
    budget = TrinoBudget(tmp_path, clock=clock, sleep=clock.sleep)
    monkeypatch.setattr(trino, "shared_budget", lambda: budget)
    monkeypatch.setenv("TRINO_SRC_WHOSCORED_PASSWORD", "source-fixture")
    manager = trino.WhoScoredTrinoManager()
    writer = trino.WhoScoredWriter()
    with (
        patch("trino.dbapi.connect"),
        patch.object(trino.IcebergWriter, "_append_dataframe_pyiceberg") as append,
    ):
        append.side_effect = lambda *a, **k: clock.now
        manager._execute("INSERT INTO iceberg.bronze.whoscored_events VALUES (1)")
        clock.now = 60.1
        observed = writer._append_dataframe_pyiceberg(
            None, database="bronze", table="whoscored_events"
        )
    assert observed == pytest.approx(119.9)


def test_source_password_required_before_connecting(monkeypatch):
    monkeypatch.setenv("TRINO_USER", "airflow")
    monkeypatch.setenv("TRINO_PASSWORD", "shared-fixture")
    monkeypatch.delenv("TRINO_SRC_WHOSCORED_PASSWORD", raising=False)
    with patch("trino.dbapi.connect") as connect:
        with pytest.raises(ValueError, match="TRINO_SRC_WHOSCORED_PASSWORD"):
            trino.WhoScoredWriter()._get_trino_manager()
        connect.assert_not_called()


def test_reconnected_poll_waits_and_unknown_write_is_submitted_once(
    tmp_path, monkeypatch
):
    from scrapers.base.trino_manager import TrinoAmbiguousWriteError

    clock = Clock(59.9)
    budget = TrinoBudget(tmp_path, clock=clock, sleep=clock.sleep)
    monkeypatch.setattr(trino, "shared_budget", lambda: budget)
    monkeypatch.setenv("TRINO_SRC_WHOSCORED_PASSWORD", "source-fixture")
    manager = trino.WhoScoredTrinoManager()
    attempts = []

    def execute(sql):
        if sql == "SELECT 1":
            return
        attempts.append((sql, clock.now))
        if len(attempts) in (1, 3):
            raise OSError("Connection reset")

    with patch("trino.dbapi.connect") as connect:
        connect.return_value.cursor.return_value.execute.side_effect = execute
        poll = "SELECT * FROM bronze.whoscored_scope_ingest_manifest"
        manager.execute_query(poll)
        update = "INSERT INTO bronze.whoscored_matches VALUES (1)"
        with pytest.raises(TrinoAmbiguousWriteError):
            manager._execute(update)
    assert attempts == [(poll, 59.9), (poll, 119.9), (update, 119.9)]
    assert all(
        call.kwargs["user"] == "src_whoscored" for call in connect.call_args_list
    )


def test_driver_does_not_retry_the_initial_post_of_an_ambiguous_insert(
    tmp_path, monkeypatch
):
    import requests
    from scrapers.base.trino_manager import TrinoAmbiguousWriteError

    monkeypatch.setenv("WHOSCORED_LOCK_DIR", str(tmp_path))
    monkeypatch.setenv("TRINO_SRC_WHOSCORED_PASSWORD", "source-fixture")
    manager = trino.WhoScoredTrinoManager()
    manager._conn = manager._create_connection()
    with patch.object(
        requests.Session,
        "post",
        side_effect=requests.ConnectionError("Connection reset"),
    ) as post:
        # Build the real driver request after replacing its POST transport.
        with pytest.raises(TrinoAmbiguousWriteError):
            manager._execute("INSERT INTO bronze.whoscored_matches VALUES (1)")
    assert post.call_count == 1
