"""#1360: refresh-lane pool wait metric for the host morning report.

The module is imported by the host morning report straight from the release
tree, so it must not need Airflow.
"""

from __future__ import annotations

import importlib.util
import subprocess
import sys
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


def test_pool_wait_sql_measures_first_try_gaps_of_the_refresh_dag(pool_wait):
    sql = pool_wait.pool_wait_sql(T0, T1)
    assert "t.dag_id = 'dag_refresh_sofascore_all_mens'" in sql
    assert "t.try_number = 1" in sql
    assert "t.state IN ('success', 'failed')" in sql
    assert f"t.start_date >= '{T0}'::timestamptz" in sql
    assert f"t.start_date < '{T1}'::timestamptz" in sql
    # The gap is measured from the later of the DagRun start and the end of
    # the run's earlier task instances — not from queued_dttm (always ~0).
    assert "GREATEST(dr.start_date, max(t.end_date) OVER (" in sql
    assert "ROWS BETWEEN UNBOUNDED PRECEDING AND 1 PRECEDING" in sql
    assert "queued_dttm" not in sql


def test_scheduler_hand_overs_up_to_sixty_seconds_are_not_waiting(pool_wait):
    assert pool_wait.WAIT_THRESHOLD_S == 60
    assert "FILTER (WHERE gap_s > 60)" in pool_wait.pool_wait_sql(T0, T1)


def test_lane_overlap_sql_counts_history_inside_daily_and_daily_in_refresh_pool(
    pool_wait,
):
    sql = pool_wait.lane_overlap_sql(T0, T1)
    assert "t.task_id = 'run_historical_scope'" in sql
    assert "d.dag_id = 'dag_ingest_sofascore'" in sql
    assert "t.start_date < coalesce(d.end_date, now())" in sql
    assert "t.pool = 'sofascore_players_pool'" in sql


@pytest.mark.parametrize("bad", ["2026-09-24", "2026-09-24T00:00:00Z'; DROP", ""])
def test_sql_rejects_non_literal_bounds(pool_wait, bad):
    with pytest.raises(ValueError):
        pool_wait.pool_wait_sql(bad, T1)
    with pytest.raises(ValueError):
        pool_wait.lane_overlap_sql(T0, bad)


def test_parse_rows_reads_psql_tuples(pool_wait):
    wait = pool_wait.parse_rows("3|1|556|555.6\n", "12|0\n")
    assert wait == pool_wait.PoolWait(3, 1, 556.0, 555.6, 12, 0)


def test_parse_rows_refuses_a_malformed_row(pool_wait):
    with pytest.raises(ValueError):
        pool_wait.parse_rows("", "0|0")
    with pytest.raises(ValueError):
        pool_wait.parse_rows("3|1|556", "0|0")


def test_format_line_green_when_nothing_waited(pool_wait):
    line = pool_wait.format_line(pool_wait.PoolWait(3, 0, 0.0, 24.0, 12, 0))
    assert line == (
        "ожидание пула актуалкой: 0 мин (0/3 прогонов ждали, макс 0,4 мин) ✅ · "
        "история во время дейли: 12 скоупов · дейли в пуле актуалки: 0"
    )


def test_format_line_red_when_a_run_waited(pool_wait):
    # 24.09 live: the 15:30 run waited 9.3 min behind the daily ingest.
    line = pool_wait.format_line(pool_wait.PoolWait(3, 1, 556.0, 555.6, 0, 0))
    assert line.startswith(
        "ожидание пула актуалкой: 9,3 мин (1/3 прогонов ждали, макс 9,3 мин) ⛔"
    )


def test_format_line_red_when_daily_takes_the_refresh_pool(pool_wait):
    line = pool_wait.format_line(pool_wait.PoolWait(3, 0, 0.0, 10.0, 5, 1))
    assert "⛔" in line and line.endswith("дейли в пуле актуалки: 1")


def test_format_line_warns_without_runs(pool_wait):
    line = pool_wait.format_line(pool_wait.PoolWait(0, 0, 0.0, 0.0, 0, 0))
    assert line.startswith("ожидание пула актуалкой: ⚠️ нет прогонов")
