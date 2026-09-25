"""#1360: refresh-lane pool wait metric for the host morning report.

The module is imported by the host morning report straight from the release
tree, so it must not need Airflow.
"""

from __future__ import annotations

import importlib.util
import subprocess
import sys
from datetime import datetime, timedelta, timezone
from pathlib import Path

import pytest

MODULE_PATH = (
    Path(__file__).resolve().parents[3] / "dags" / "utils" / "sofascore_pool_wait.py"
)
T0, T1 = "2026-09-24T00:00:00Z", "2026-09-25T00:00:00Z"


@pytest.fixture
def pool_wait():
    spec = importlib.util.spec_from_file_location("sofascore_pool_wait", MODULE_PATH)
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


def test_module_needs_only_the_standard_library():
    code = (
        "import importlib.util, sys\n"
        f"s = importlib.util.spec_from_file_location('m', {str(MODULE_PATH)!r})\n"
        "m = importlib.util.module_from_spec(s); s.loader.exec_module(m)\n"
        "assert 'airflow' not in sys.modules\n"
    )
    subprocess.run([sys.executable, "-I", "-c", code], check=True)


def test_pool_wait_sql_never_reads_queued_dttm(pool_wait):
    # queued_dttm is stamped when the pool slot is granted (Airflow 2.11), so
    # queued_dttm -> start_date is always ~0 and cannot show a wait.
    assert "queued_dttm" not in pool_wait.pool_wait_sql(T0, T1)
    assert pool_wait.WAIT_THRESHOLD_S == 60


# --- the SQL itself, executed on DuckDB over metadata-shaped tables ---------

R = "dag_refresh_sofascore_all_mens"
H = "dag_backfill_sofascore_all_mens"
D = "dag_ingest_sofascore"


@pytest.fixture
def metadb():
    duckdb = pytest.importorskip("duckdb")
    con = duckdb.connect()
    con.execute(
        "CREATE TABLE dag_run(dag_id VARCHAR, run_id VARCHAR, "
        "start_date TIMESTAMPTZ, end_date TIMESTAMPTZ)"
    )
    for table in ("task_instance", "task_instance_history"):
        con.execute(
            f"CREATE TABLE {table}(dag_id VARCHAR, run_id VARCHAR, task_id VARCHAR, "
            "map_index INT, try_number INT, state VARCHAR, pool VARCHAR, "
            "start_date TIMESTAMPTZ, end_date TIMESTAMPTZ)"
        )
    return con


def _run(con, dag_id, run_id, start, end=None):
    con.execute("INSERT INTO dag_run VALUES (?, ?, ?, ?)", [dag_id, run_id, start, end])


def _ti(con, dag_id, run_id, task_id, start, end, *, try_number=1, state="success",
        pool="p", table="task_instance"):
    con.execute(
        f"INSERT INTO {table} VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)",
        [dag_id, run_id, task_id, -1, try_number, state, pool, start, end],
    )


def _wait(pool_wait, con, t0=T0, t1=T1):
    row = con.execute(pool_wait.pool_wait_sql(t0, t1)).fetchone()
    return (row[0], row[1], float(row[2]), float(row[3]))


def test_run_behind_the_daily_is_waiting_and_hand_overs_are_not(pool_wait, metadb):
    # 24.09 live shape: the 15:30 run's first task started 9.3 min late.
    _run(metadb, R, "r1530", "2026-09-24 15:30:00+00")
    _ti(metadb, R, "r1530", "schedules", "2026-09-24 15:39:18+00", "2026-09-24 16:11:40+00")
    _ti(metadb, R, "r1530", "plan", "2026-09-24 16:11:42+00", "2026-09-24 16:12:00+00")
    # 00:30 run: every hand-over within 60 s, the last one exactly 60 s.
    _run(metadb, R, "r0030", "2026-09-24 00:30:00+00")
    _ti(metadb, R, "r0030", "schedules", "2026-09-24 00:30:24+00", "2026-09-24 01:00:00+00")
    _ti(metadb, R, "r0030", "plan", "2026-09-24 01:01:00+00", "2026-09-24 01:02:00+00")
    assert _wait(pool_wait, metadb) == (2, 1, 558.0, 558.0)


def test_sixty_one_seconds_is_waiting(pool_wait, metadb):
    _run(metadb, R, "r", "2026-09-24 08:30:00+00")
    _ti(metadb, R, "r", "schedules", "2026-09-24 08:31:01+00", "2026-09-24 09:00:00+00")
    assert _wait(pool_wait, metadb) == (1, 1, 61.0, 61.0)


def test_a_retried_task_keeps_its_first_try_wait(pool_wait, metadb):
    _run(metadb, R, "r", "2026-09-24 15:30:00+00")
    # First try waited 10 min for the slot, failed; the retry started after the
    # 2 min retry delay — that delay is not a pool wait.
    _ti(metadb, R, "r", "scope", "2026-09-24 15:40:00+00", "2026-09-24 16:00:00+00",
        state="failed", table="task_instance_history")
    _ti(metadb, R, "r", "scope", "2026-09-24 16:02:00+00", "2026-09-24 16:30:00+00",
        try_number=2)
    # The next task is measured from the retry's end, not the first try's.
    _ti(metadb, R, "r", "validate", "2026-09-24 16:30:30+00", "2026-09-24 16:31:00+00")
    assert _wait(pool_wait, metadb) == (1, 1, 600.0, 600.0)


def test_a_retry_that_waits_for_the_slot_after_its_delay_is_waiting(pool_wait, metadb):
    # Astra r3: the first try started at once and failed at 16:00; after the
    # 2 min retry delay the retry waited for the busy pool until 16:20.
    _run(metadb, R, "r", "2026-09-24 15:30:00+00")
    _ti(metadb, R, "r", "scope", "2026-09-24 15:30:20+00", "2026-09-24 16:00:00+00",
        state="failed", table="task_instance_history")
    _ti(metadb, R, "r", "scope", "2026-09-24 16:20:00+00", "2026-09-24 16:40:00+00",
        try_number=2)
    assert _wait(pool_wait, metadb) == (1, 1, 1080.0, 1080.0)


def test_a_task_still_waiting_for_the_slot_is_not_a_false_zero(pool_wait, metadb):
    now = datetime.now(timezone.utc).replace(microsecond=0)
    t0 = (now - timedelta(hours=3)).strftime("%Y-%m-%dT%H:%M:%SZ")
    t1 = (now + timedelta(hours=1)).strftime("%Y-%m-%dT%H:%M:%SZ")
    # A finished run that did not wait, and one whose first task sits in
    # ``scheduled`` for two hours with nothing of its run running.
    _run(metadb, R, "done", now - timedelta(hours=2, minutes=30))
    _ti(metadb, R, "done", "schedules", now - timedelta(hours=2, minutes=30),
        now - timedelta(hours=2, minutes=20))
    _run(metadb, R, "stuck", now - timedelta(hours=2))
    _ti(metadb, R, "stuck", "schedules", None, None, state="scheduled")
    runs, waiting, wait_sum, _ = _wait(pool_wait, metadb, t0, t1)
    assert (runs, waiting) == (2, 1)
    assert wait_sum >= 2 * 3600 - 5


def test_a_task_queued_behind_its_own_running_run_is_not_waiting(pool_wait, metadb):
    now = datetime.now(timezone.utc).replace(microsecond=0)
    t0 = (now - timedelta(hours=3)).strftime("%Y-%m-%dT%H:%M:%SZ")
    t1 = (now + timedelta(hours=1)).strftime("%Y-%m-%dT%H:%M:%SZ")
    _run(metadb, R, "r", now - timedelta(hours=2))
    _ti(metadb, R, "r", "scope0", now - timedelta(hours=2), None, state="running")
    _ti(metadb, R, "r", "scope1", None, None, state="scheduled")
    assert _wait(pool_wait, metadb, t0, t1)[:2] == (1, 0)


def test_runs_belong_to_the_day_they_started(pool_wait, metadb):
    # Started before midnight: not this day, even with tasks after midnight.
    _run(metadb, R, "late", "2026-09-23 23:50:00+00")
    _ti(metadb, R, "late", "plan", "2026-09-24 00:20:00+00", "2026-09-24 00:30:00+00")
    # Started in the day: its tasks after the next midnight still count.
    _run(metadb, R, "eve", "2026-09-24 23:50:00+00")
    _ti(metadb, R, "eve", "schedules", "2026-09-25 00:05:00+00", "2026-09-25 00:10:00+00")
    assert _wait(pool_wait, metadb) == (1, 1, 900.0, 900.0)


def test_lane_overlap_counts_history_inside_daily_and_daily_in_lane_pools(
    pool_wait, metadb
):
    _run(metadb, D, "daily", "2026-09-24 14:00:00+00", "2026-09-24 18:00:00+00")
    # History scope attempts: one inside the daily run (a first try kept in
    # the history table), one after it, one on another task id.
    _ti(metadb, H, "h1", "run_historical_scope", "2026-09-24 14:10:00+00",
        "2026-09-24 15:00:00+00", state="failed", table="task_instance_history")
    _ti(metadb, H, "h2", "run_historical_scope", "2026-09-24 19:00:00+00",
        "2026-09-24 20:00:00+00")
    _ti(metadb, H, "h1", "plan_historical_batch", "2026-09-24 14:05:00+00",
        "2026-09-24 14:06:00+00")
    # Daily attempts: one in the refresh pool, one in the history pool started
    # before the window and still holding it inside, one in its own pool.
    _ti(metadb, D, "daily", "t1", "2026-09-24 14:00:00+00", "2026-09-24 14:30:00+00",
        pool="sofascore_players_pool")
    _ti(metadb, D, "old", "t2", "2026-09-23 23:00:00+00", "2026-09-24 01:00:00+00",
        pool="sofascore_history_pool")
    _ti(metadb, D, "daily", "t3", "2026-09-24 14:30:00+00", "2026-09-24 15:00:00+00",
        pool="ingest_scraper_pool")
    row = metadb.execute(pool_wait.lane_overlap_sql(T0, T1)).fetchone()
    assert tuple(row) == (1, 1, 1)


@pytest.mark.parametrize("bad", ["2026-09-24", "2026-09-24T00:00:00Z'; DROP", ""])
def test_sql_rejects_non_literal_bounds(pool_wait, bad):
    with pytest.raises(ValueError):
        pool_wait.pool_wait_sql(bad, T1)
    with pytest.raises(ValueError):
        pool_wait.lane_overlap_sql(T0, bad)


def test_parse_rows_reads_psql_tuples(pool_wait):
    wait = pool_wait.parse_rows("3|1|556|555.6\n", "12|0|0\n")
    assert wait == pool_wait.PoolWait(3, 1, 556.0, 555.6, 12, 0, 0)


def test_parse_rows_refuses_a_malformed_row(pool_wait):
    with pytest.raises(ValueError):
        pool_wait.parse_rows("", "0|0|0")
    with pytest.raises(ValueError):
        pool_wait.parse_rows("3|1|556", "0|0|0")
    with pytest.raises(ValueError):
        pool_wait.parse_rows("3|1|556|555.6", "0|0")


def test_format_line_green_when_nothing_waited(pool_wait):
    line = pool_wait.format_line(pool_wait.PoolWait(3, 0, 0.0, 24.0, 12, 0, 0))
    assert line == (
        "ожидание пула актуалкой: 0 мин (0/3 прогонов ждали, макс 0,4 мин) ✅ · "
        "история во время дейли: 12 скоупов · дейли в пулах актуалки/истории: 0/0"
    )


def test_format_line_red_when_a_run_waited(pool_wait):
    # 24.09 live: the 15:30 run waited 9.3 min behind the daily ingest.
    line = pool_wait.format_line(pool_wait.PoolWait(3, 1, 556.0, 555.6, 0, 0, 0))
    assert line.startswith(
        "ожидание пула актуалкой: 9,3 мин (1/3 прогонов ждали, макс 9,3 мин) ⛔"
    )


@pytest.mark.parametrize(("refresh", "history"), [(1, 0), (0, 1)])
def test_format_line_red_when_daily_takes_a_lane_pool(pool_wait, refresh, history):
    line = pool_wait.format_line(pool_wait.PoolWait(3, 0, 0.0, 10.0, 5, refresh, history))
    assert "⛔" in line and line.endswith(f"дейли в пулах актуалки/истории: {refresh}/{history}")


def test_format_line_warns_without_runs(pool_wait):
    line = pool_wait.format_line(pool_wait.PoolWait(0, 0, 0.0, 0.0, 0, 0, 0))
    assert line.startswith("ожидание пула актуалкой: ⚠️ нет прогонов")
