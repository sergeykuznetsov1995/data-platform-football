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
        assert clock.now == 11 + INTERVAL + WRITE_TIMEOUT + INTERVAL


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


@pytest.mark.parametrize("create", ["CREATE TABLE", "CREATE TABLE IF NOT EXISTS"])
def test_empty_create_paces_the_first_insert_across_lanes(
    tmp_path, monkeypatch, create
):
    clock = Clock(59.9)
    monkeypatch.setenv("TRINO_SRC_WHOSCORED_PASSWORD", "source-fixture")
    current, history = [
        trino.WhoScoredTrinoManager(
            budget=TrinoBudget(tmp_path, clock=clock, sleep=clock.sleep)
        )
        for _ in range(2)
    ]
    submitted = []

    def execute(manager, sql, **kwargs):
        submitted.append(clock())
        if len(submitted) == 1:
            clock.now += 0.2

    monkeypatch.setattr(trino.TrinoTableManager, "_execute", execute)
    current._execute(f'{create} "iceberg"."bronze"."whoscored_events" (id BIGINT)')
    history._execute("INSERT INTO bronze.whoscored_lineups VALUES (1)")
    history._execute("INSERT INTO bronze.whoscored_events VALUES (1)")

    assert submitted == pytest.approx([59.9, 60.1, 120.1])


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


@pytest.mark.parametrize(
    ("dag", "source"),
    [
        ("dag_ingest_whoscored", "whoscored-current"),
        ("dag_backfill_whoscored", "whoscored-history"),
        (None, "whoscored-service"),
    ],
)
def test_query_origin_is_independent_of_the_login(monkeypatch, dag, source):
    monkeypatch.setenv("TRINO_USER", "airflow")
    monkeypatch.setenv("TRINO_SRC_WHOSCORED_PASSWORD", "source-fixture")
    if dag:
        monkeypatch.setenv("AIRFLOW_CTX_DAG_ID", dag)
    else:
        monkeypatch.delenv("AIRFLOW_CTX_DAG_ID", raising=False)
    with trino.WhoScoredTrinoManager()._create_connection() as connection:
        assert (connection.user, connection.source) == ("src_whoscored", source)


def test_injected_manager_supplies_direct_writer_budget(tmp_path, monkeypatch):
    from scrapers.whoscored.repository import WhoScoredRepository

    clock = Clock(10)
    monkeypatch.setenv("TRINO_SRC_WHOSCORED_PASSWORD", "source-fixture")
    monkeypatch.setattr(
        trino,
        "shared_budget",
        lambda: TrinoBudget(tmp_path / "default", clock=clock, sleep=clock.sleep),
    )
    manager = trino.WhoScoredTrinoManager()
    manager.budget = TrinoBudget(
        tmp_path / "configured", clock=clock, sleep=clock.sleep
    )
    repo = WhoScoredRepository(trino=manager)
    with (
        patch("trino.dbapi.connect"),
        patch.object(trino.IcebergWriter, "_append_dataframe_pyiceberg") as append,
    ):
        append.side_effect = lambda *a, **k: clock.now
        manager._execute("INSERT INTO bronze.whoscored_events VALUES (1)")
        observed = repo.writer._append_dataframe_pyiceberg(
            None, database="bronze", table="whoscored_events"
        )
    assert observed == 70


def test_injected_query_client_shares_inventory_across_repositories(
    tmp_path, monkeypatch
):
    from unittest.mock import Mock
    from scrapers.whoscored.repository import WhoScoredRepository

    monkeypatch.setenv("WHOSCORED_LOCK_DIR", str(tmp_path))
    client = Mock()
    client.execute_query.return_value = []
    for _ in range(2):
        assert WhoScoredRepository(trino=client).history_coverage() == {}
    client.execute_query.assert_called_once()


def test_initial_connection_failure_does_not_fence_unsubmitted_write(
    tmp_path, monkeypatch
):
    from scrapers.base.trino_manager import TrinoError, TrinoTableManager

    clock = Clock(5)
    budget = TrinoBudget(tmp_path, clock=clock, sleep=clock.sleep)
    monkeypatch.setenv("TRINO_SRC_WHOSCORED_PASSWORD", "source-fixture")
    monkeypatch.setattr(TrinoTableManager, "_trino_unreachable", False)
    manager = trino.WhoScoredTrinoManager(budget=budget)
    monkeypatch.setattr(manager, "_CONNECT_RETRIES", 1)
    with patch("trino.dbapi.connect") as connect:
        cursor = connect.return_value.cursor.return_value
        cursor.execute.side_effect = OSError(
            "connection failed in the initial health check"
        )
        with pytest.raises(TrinoError):
            manager._execute("INSERT INTO bronze.whoscored_events VALUES (1)")
        assert [call.args[0] for call in cursor.execute.call_args_list] == ["SELECT 1"]
        cursor.execute.side_effect = None
        manager._reset_connection()
        manager._execute("INSERT INTO bronze.whoscored_events VALUES (2)")
    assert clock.now <= 5 + INTERVAL


def test_lost_response_fences_even_a_late_submission(tmp_path):
    clock = Clock(0)
    budget = TrinoBudget(tmp_path, clock=clock, sleep=clock.sleep)
    with pytest.raises(OSError):
        with budget.statement(["commit:a"]):
            # The connection/statement setup consumed time before the POST.
            clock.now = 180
            raise OSError("no response to the write")
    with budget.statement(["commit:b"]):
        assert clock.now == 180
    with budget.statement(["commit:a"]):
        assert clock.now >= 180 + WRITE_TIMEOUT + INTERVAL


def test_uncertain_commit_survives_waiter_restart_and_worker_reboot(
    tmp_path, monkeypatch
):
    clock = Clock(10)
    boot = tmp_path / "boot"
    boot.write_text("one")
    import scrapers.whoscored.trino_budget as pacing

    monkeypatch.setattr(pacing, "_BOOT", boot)
    budget = TrinoBudget(tmp_path / "slots", clock=clock, sleep=clock.sleep)
    with pytest.raises(OSError):
        with budget.statement(["commit:a"]):
            raise OSError("unknown outcome")

    # First contender observes the ongoing cooldown, then is interrupted.
    def stop(seconds):
        raise KeyboardInterrupt

    waiting = TrinoBudget(budget.root, clock=clock, sleep=stop)
    with pytest.raises(KeyboardInterrupt):
        with waiting.statement(["commit:a"]):
            pytest.fail("table still fenced")
    boot.write_text("two")
    clock.now = 1
    with budget.statement(["commit:a"]):
        assert clock.now >= 1 + WRITE_TIMEOUT + INTERVAL


def test_session_cannot_outlive_the_uncertain_commit_fence(monkeypatch):
    monkeypatch.setenv("TRINO_SRC_WHOSCORED_PASSWORD", "source-fixture")
    with pytest.raises(ValueError, match="query_max_run"):
        trino.WhoScoredTrinoManager(session_properties={"query_max_run_time": "2h"})
    with pytest.raises(ValueError, match="query_max_run"):
        trino.WhoScoredTrinoManager(query_max_run_seconds=WRITE_TIMEOUT + 1)
    with patch("trino.dbapi.connect") as connect:
        trino.WhoScoredTrinoManager(query_max_run_seconds=50)._create_connection()
    assert connect.call_args.kwargs["session_properties"]["query_max_run_time"] == "50s"
