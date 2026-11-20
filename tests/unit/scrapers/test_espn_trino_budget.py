"""Exercise shared ESPN pacing at the real manager and journal entry points."""

import multiprocessing
import os
from collections import deque
from types import SimpleNamespace
from unittest.mock import Mock

import pytest
from requests import ReadTimeout
from trino.client import TrinoQuery
from trino.exceptions import Http502Error, Http503Error, Http504Error, TrinoConnectionError

from scrapers.base import trino_manager as base
from scrapers.espn import trino_budget
from scrapers.espn.journal import JOURNAL_TABLE, flush_journal
from scrapers.espn.trino_budget import BudgetConnection, statement_keys
from scrapers.espn.trino_manager import EspnTrinoTableManager
from scrapers.whoscored.trino_budget import INTERVAL, WRITE_TIMEOUT, TrinoBudget

pytestmark = pytest.mark.unit


class Clock:
    def __init__(self):
        self.now = 0
        self.sleeps = []

    def __call__(self):
        return self.now

    def sleep(self, seconds):
        self.sleeps.append(seconds)
        self.now += seconds


class Driver:
    """A separate connection per connect(), with one scripted final page per query."""

    def __init__(self, clock):
        self.clock = clock
        self.submissions = []
        self.pages = deque()
        self.connections = []
        self.closed_cursors = []

    def connect(self, **settings):
        connection = SimpleNamespace(cursor=lambda: Cursor(self), close=lambda: None)
        self.connections.append(connection)
        return connection


class Cursor:
    arraysize = 1

    def __init__(self, driver):
        self.driver = driver
        self.probe = False
        self.drained = False

    def execute(self, sql, params=None):
        self.probe = sql == "SELECT 1"
        self.drained = False
        if not self.probe:
            self.driver.submissions.append((self.driver.clock(), sql, params))

    def fetchall(self):
        assert not self.drained, "driver fetched twice"
        self.drained = True
        if self.probe:
            return [(1,)]
        # Reading two result pages consumes five seconds of virtual server time.
        self.driver.clock.now += 2
        rows = [(1,), (2,)]
        self.driver.clock.now += 3
        if self.driver.pages:
            self.driver.pages.popleft()()
        return rows + [(3,)]

    def close(self):
        self.driver.closed_cursors.append(self)


@pytest.fixture
def pair(monkeypatch, tmp_path):
    clock = Clock()
    driver = Driver(clock)
    monkeypatch.setenv("TRINO_SRC_ESPN_PASSWORD", "synthetic-dedicated")
    monkeypatch.setattr(base.TrinoTableManager, "_trino_unreachable", False)
    monkeypatch.setattr(base.trino.dbapi, "connect", driver.connect)
    managers = [
        EspnTrinoTableManager(budget=TrinoBudget(tmp_path, clock=clock, sleep=clock.sleep))
        for _ in range(2)
    ]
    return managers[0], managers[1], driver, clock


def test_default_budget_uses_espn_state_volume(tmp_path, monkeypatch):
    monkeypatch.setattr(trino_budget, "default_state_path", lambda: tmp_path / "gate.json")
    first, second = trino_budget.default_budget(), trino_budget.default_budget()
    assert first is not second
    assert first.root == second.root == tmp_path / "trino-budget"


def test_two_managers_pace_raw_journal_writes_and_reads(pair):
    first, second, driver, clock = pair
    for manager in (first, second):
        assert flush_journal(manager.connection, [{}, {}]) == 2
    sql = f"SELECT * FROM {JOURNAL_TABLE}"
    assert second._execute(sql, fetch=True) == [(1,), (2,), (3,)]
    first._execute("SELECT * FROM bronze.espn_other", fetch=True)
    cursor = first.connection.cursor()
    try:
        assert cursor.execute(sql) is cursor
        assert cursor.fetchone() == (1,)
        assert list(cursor) == [(2,), (3,)]
        assert cursor.fetchall() == []
    finally:
        cursor.close()

    assert len(driver.connections) == 2
    assert len(driver.closed_cursors) == 7  # five statements plus two probes
    assert [t for t, _, _ in driver.submissions] == [
        0, 5 + INTERVAL, 10 + INTERVAL, 15 + INTERVAL, 15 + 2 * INTERVAL
    ]
    assert clock.sleeps == [INTERVAL, INTERVAL - 5]


def test_repeated_stage_insert_paces_only_that_stage(pair):
    first, second, driver, _ = pair
    sql = "INSERT INTO bench.legacy__stg_one VALUES (?)"
    first._execute(sql, params=(1,))
    second._execute("INSERT INTO bench.legacy__stg_two VALUES (?)", params=(2,))
    second._execute(sql, params=(3,))
    assert [(t, params) for t, _, params in driver.submissions] == [
        (0, (1,)), (5, (2,)), (5 + INTERVAL, (3,))
    ]


def test_use_resolves_bare_names_for_other_cursors(pair):
    first, second, driver, _ = pair
    first._execute('USE "ICEBERG"."Ops"')
    first._execute("SELECT * FROM legacy", fetch=True)
    second._execute("SELECT * FROM ops.legacy", fetch=True)
    assert [t for t, _, _ in driver.submissions] == [0, 5, 10 + INTERVAL]


def test_fetchmany_iteration_and_cursor_reuse(pair):
    first, _, _, _ = pair
    with first.connection.cursor() as cursor:
        cursor.arraysize = 2
        cursor.execute("SELECT * FROM bronze.a")
        assert cursor.fetchmany() == [(1,), (2,)]
        assert cursor.fetchall() == [(3,)]
        assert cursor.fetchone() is None
        cursor.execute("SELECT 1")
        assert cursor.fetchall() == [(1,)]
        cursor.execute("SELECT * FROM ops.b")
        assert list(cursor) == [(1,), (2,), (3,)]


class Conflict(Exception):
    error_name = "ICEBERG_COMMIT_ERROR"


def fail_with(error):
    def fail():
        raise error
    return fail


@pytest.mark.parametrize(
    ("error", "expected_wait"),
    [
        (RuntimeError("Connection reset by peer"), INTERVAL + WRITE_TIMEOUT),
        (Conflict("Failed to commit the transaction"), INTERVAL + WRITE_TIMEOUT),
        (Conflict("Found conflicting files"), INTERVAL),
    ],
)
def test_final_page_failure_cools_commit_and_preserves_original_error(pair, error, expected_wait):
    first, second, driver, clock = pair
    sql = "INSERT INTO bronze.espn_game VALUES (1)"
    driver.pages.append(fail_with(error))
    cursor = first.connection.cursor()
    try:
        with pytest.raises(type(error)) as caught:
            cursor.execute(sql)
        assert caught.value is error
        assert cursor.fetchall() == []
    finally:
        cursor.close()
    second._execute(sql)
    assert clock.sleeps == [expected_wait]
    assert [t for t, _, _ in driver.submissions] == [0, 5 + expected_wait]


def test_manager_with_select_retries_after_final_page_scan_cooldown(pair):
    first, _, driver, _ = pair
    sql = "WITH games AS (SELECT * FROM bronze.espn_game) SELECT * FROM games"
    driver.pages.append(fail_with(RuntimeError("Connection reset by peer")))
    first.execute_query(sql)
    assert [(t, query) for t, query, _ in driver.submissions] == [
        (0, sql), (5 + INTERVAL, sql)
    ]


def test_manager_insert_with_lost_final_page_never_replays(pair):
    first, second, driver, _ = pair
    sql = "INSERT INTO bronze.espn_game VALUES (1)"
    driver.pages.append(fail_with(RuntimeError("Connection reset by peer")))
    with pytest.raises(base.TrinoAmbiguousWriteError):
        first._execute(sql)
    assert len(driver.submissions) == len(driver.connections) == 1
    second._execute(sql)
    assert [t for t, _, _ in driver.submissions] == [0, 5 + INTERVAL + WRITE_TIMEOUT]


def test_merge_final_response_timeout_surfaces_driver_ambiguity(pair):
    first, _, driver, _ = pair
    sql = "MERGE INTO bronze.a t USING bronze.b s ON t.id=s.id WHEN MATCHED THEN DELETE"
    request = Mock()
    request.get.side_effect = ReadTimeout("response deadline exceeded")
    driver.pages.append(TrinoQuery(request, sql).fetch)

    with pytest.raises(base.TrinoAmbiguousWriteError) as caught:
        first._execute_committing(sql)

    request.get.assert_called_once()
    cause = caught.value.__cause__
    assert isinstance(cause, TrinoConnectionError)
    assert "failed to fetch: response deadline exceeded" in str(cause)
    assert [query for _, query, _ in driver.submissions] == [sql]


@pytest.mark.parametrize("error_type", [Http502Error, Http503Error, Http504Error])
def test_mutation_gateway_failure_is_ambiguous(pair, error_type):
    first, _, driver, _ = pair
    sql = "INSERT INTO bronze.espn_game VALUES (1)"
    error = error_type("gateway unavailable")
    driver.pages.append(fail_with(error))

    with pytest.raises(base.TrinoAmbiguousWriteError) as caught:
        first._execute(sql)

    assert caught.value.__cause__ is error
    assert [query for _, query, _ in driver.submissions] == [sql]


@pytest.mark.parametrize(("sql", "error"), [
    ("SELECT * FROM bronze.a", TrinoConnectionError("response deadline exceeded")),
    ("DELETE FROM bronze.a", ValueError("response deadline exceeded")),
])
def test_read_or_nontransport_failure_keeps_ordinary_error(pair, sql, error):
    first, _, driver, _ = pair
    driver.pages.append(fail_with(error))

    with pytest.raises(base.TrinoError) as caught:
        first._execute(sql)

    assert type(caught.value) is base.TrinoError
    assert caught.value.__cause__ is error
    assert [query for _, query, _ in driver.submissions] == [sql]


def test_mutation_probe_failure_is_ordinary_before_submission(pair, monkeypatch):
    first, _, driver, _ = pair
    monkeypatch.setattr(first, "_CONNECT_RETRIES", 1)
    monkeypatch.setattr(Cursor, "fetchall", Mock(side_effect=TrinoConnectionError("probe failed")))

    with pytest.raises(base.TrinoError, match="probe failed") as caught:
        first._execute("INSERT INTO bronze.a VALUES (1)")

    assert type(caught.value) is base.TrinoError
    assert driver.submissions == []


def test_manager_committing_retries_only_definite_rejection(pair, monkeypatch):
    first, _, driver, clock = pair
    sql = "MERGE INTO bronze.a t USING bronze.b s ON t.id=s.id WHEN MATCHED THEN DELETE"
    monkeypatch.setattr(base.time, "sleep", lambda _: None)
    driver.pages.extend([
        fail_with(Conflict("Found conflicting files")),
        fail_with(Conflict("Failed to commit the transaction")),
    ])
    with pytest.raises(base.TrinoError, match="Failed to commit the transaction"):
        first._execute_committing(sql)
    assert clock.sleeps == [INTERVAL]
    assert [t for t, _, _ in driver.submissions] == [0, 5 + INTERVAL]


@pytest.mark.parametrize("sql", [
    "SELECT 1", "SHOW TABLES FROM bronze", "DESCRIBE iceberg.ops.espn_journal",
    "CREATE SCHEMA IF NOT EXISTS bronze",
    "SET SESSION task_concurrency = 1", "SELECT * FROM iceberg.information_schema.tables",
])
def test_unmetered(sql):
    assert statement_keys(sql) == set()


def test_quoted_join_nested_cte_self_scan_and_literal():
    sql = """
        INSERT INTO "Iceberg"."Bronze"."Legacy" (msg)
        WITH latest AS (
            SELECT a.msg FROM "ICEBERG".bronze."LEGACY" a
            JOIN (SELECT id FROM ops.journal) j ON a.id=j.id
        ), subset(id) AS (SELECT id FROM bench.temporary)
        SELECT 'FROM ops.fake '' JOIN bronze.other' FROM latest
        JOIN subset USING (id) /* JOIN ops.comment */
        WHERE latest.id IS NOT DISTINCT FROM subset.id -- FROM bronze.ignore
    """
    assert statement_keys(sql) == {
        "commit:iceberg.bronze.legacy", "scan:iceberg.bronze.legacy",
        "scan:iceberg.ops.journal", "scan:iceberg.bench.temporary",
    }


@pytest.mark.parametrize(("sql", "keys"), [
    ("CREATE TABLE IF NOT EXISTS ops.journal (msg VARCHAR) WITH (format = 'PARQUET')",
     {"commit:iceberg.ops.journal"}),
    ("MERGE INTO a t USING bench.a__stg_1 s ON t.id=s.id WHEN MATCHED THEN DELETE",
     {"commit:iceberg.bronze.a", "scan:iceberg.bronze.a", "scan:iceberg.bench.a__stg_1"}),
    ("UPDATE bronze.a SET x=(SELECT max(x) FROM ops.b)",
     {"commit:iceberg.bronze.a", "scan:iceberg.bronze.a", "scan:iceberg.ops.b"}),
    ("DELETE FROM iceberg.bronze.a WHERE id IN (SELECT id FROM a)",
     {"commit:iceberg.bronze.a", "scan:iceberg.bronze.a"}),
    ('CREATE TABLE bench.a__stg_1 AS SELECT *, CAST(NULL AS VARCHAR) AS "__dpf_replace_op" '
     'FROM bronze.a WHERE FALSE', {"commit:iceberg.bench.a__stg_1", "scan:iceberg.bronze.a"}),
    ("CREATE TABLE bench.a__stg_1 AS SELECT * FROM bronze.a WHERE false",
     {"commit:iceberg.bench.a__stg_1", "scan:iceberg.bronze.a"}),
    ("CREATE TABLE bench.a__stg_1 AS SELECT * FROM (SELECT * FROM ops.b) src",
     {"commit:iceberg.bench.a__stg_1", "scan:iceberg.ops.b"}),
    ("CREATE TABLE bench.a AS SELECT * FROM a UNION ALL SELECT * FROM b WHERE FALSE",
     {"commit:iceberg.bench.a", "scan:iceberg.bronze.a", "scan:iceberg.bronze.b"}),
    ("WITH a AS (SELECT * FROM a) SELECT * FROM a, ops.b",
     {"scan:iceberg.bronze.a", "scan:iceberg.ops.b"}),
])
def test_physical_keys(sql, keys):
    assert statement_keys(sql) == keys


@pytest.mark.parametrize("sql", [
    "CALL iceberg.system.add_files('ops.a')", "ALTER TABLE ops.a EXECUTE optimize",
    "SELECT * FROM TABLE(system.query(query => 'SELECT * FROM bronze.a'))",
    "RESET SESSION query_max_run_time", "SELECT 1; DELETE FROM ops.journal",
])
def test_unknown_data_statements_never_reach_driver(pair, sql):
    first, _, driver, _ = pair
    cursor = first.connection.cursor()
    with pytest.raises(ValueError):
        cursor.execute(sql)
    assert driver.submissions == []
    cursor.close()


def _die_during_final_page(root, sql):
    clock = Clock()
    driver = Driver(clock)
    driver.pages.append(lambda: os._exit(17))
    budget = TrinoBudget(root, clock=clock, sleep=clock.sleep)
    BudgetConnection(driver.connect(), budget, "iceberg").cursor().execute(sql)


def test_crashed_writer_fences_next_process(tmp_path):
    sql = "INSERT INTO ops.journal VALUES (1)"
    child = multiprocessing.get_context("spawn").Process(
        target=_die_during_final_page, args=(tmp_path, sql)
    )
    child.start()
    try:
        child.join(30)
        assert child.exitcode == 17
    finally:
        if child.is_alive():
            child.terminate()
            child.join()
    clock = Clock()
    clock.now = 20
    driver = Driver(clock)
    budget = TrinoBudget(tmp_path, clock=clock, sleep=clock.sleep)
    BudgetConnection(driver.connect(), budget, "iceberg").cursor().execute(sql)
    assert clock.sleeps == [INTERVAL + WRITE_TIMEOUT]
