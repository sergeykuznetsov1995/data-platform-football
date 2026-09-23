"""#1356: one red-share threshold for the SofaScore scope lanes.

The module is imported by the host morning report and the stall watch
straight from the release tree, so it must not need Airflow.
"""

from __future__ import annotations

import importlib.util
import subprocess
import sys
from pathlib import Path

import pytest

MODULE_PATH = (
    Path(__file__).resolve().parents[3] / "dags" / "utils" / "sofascore_red_share.py"
)


@pytest.fixture
def red_share():
    spec = importlib.util.spec_from_file_location("sofascore_red_share", MODULE_PATH)
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


def test_threshold_is_twenty_percent(red_share):
    assert red_share.RED_SHARE_THRESHOLD_PCT == 20


def test_exactly_twenty_percent_is_red(red_share):
    share = red_share.RedShare.of(failed=20, total=100)
    assert share.verdict == "red"


def test_just_below_twenty_percent_is_ok(red_share):
    share = red_share.RedShare.of(failed=199, total=1000)
    assert share.pct == pytest.approx(19.9)
    assert share.verdict == "ok"


def test_no_terminal_task_instances_is_not_ok(red_share):
    share = red_share.RedShare.of(failed=0, total=0)
    assert share.verdict == "no_data"


def test_sql_counts_terminal_scope_task_instances_in_the_window(red_share):
    sql = red_share.red_share_sql("2026-09-22T00:00:00Z", "2026-09-23T00:00:00Z")
    assert "FROM task_instance" in sql
    assert "dag_backfill_sofascore_all_mens" in sql
    assert "dag_refresh_sofascore_all_mens" in sql
    assert "task_id LIKE 'run\\_%\\_scope' ESCAPE '\\'" in sql
    assert "map_index >= 0" in sql
    assert "state IN ('success', 'failed')" in sql
    assert "start_date >= '2026-09-22T00:00:00Z'::timestamptz" in sql
    assert "start_date < '2026-09-23T00:00:00Z'::timestamptz" in sql
    assert "dag_run" not in sql


def test_sql_rejects_a_non_utc_literal(red_share):
    with pytest.raises(ValueError):
        red_share.red_share_sql("2026-09-22 00:00", "2026-09-23T00:00:00Z")


def test_pattern_matches_scope_tasks_only(red_share):
    import re

    like = red_share.SCOPE_TASK_PATTERN.replace("\\_", "_").replace("%", ".*")
    regex = re.compile("^" + like + "$")
    assert regex.match("run_historical_scope")
    assert regex.match("run_refresh_scope")
    assert not regex.match("run_sofascore_dq")


def test_parse_psql_rows(red_share):
    per_dag = red_share.parse_rows(
        "dag_backfill_sofascore_all_mens|70|121\n"
        "dag_refresh_sofascore_all_mens|3|40\n"
    )
    assert per_dag == {
        "dag_backfill_sofascore_all_mens": (70, 121),
        "dag_refresh_sofascore_all_mens": (3, 40),
    }


def test_format_line_red(red_share):
    line = red_share.format_line(
        "2026-09-22",
        {
            "dag_backfill_sofascore_all_mens": (70, 121),
            "dag_refresh_sofascore_all_mens": (3, 40),
        },
    )
    assert line == (
        "красных run_*_scope TI за 22.09 (UTC): 73/161 = 45,3 % ⛔ "
        "(порог < 20 %; история 70/121, актуалка 3/40)"
    )


def test_format_line_ok_has_the_check_mark(red_share):
    line = red_share.format_line(
        "2026-09-22", {"dag_backfill_sofascore_all_mens": (1, 10)}
    )
    assert line == (
        "красных run_*_scope TI за 22.09 (UTC): 1/10 = 10,0 % ✅ "
        "(порог < 20 %; история 1/10, актуалка 0/0)"
    )


def test_format_line_no_data_has_no_check_mark(red_share):
    line = red_share.format_line("2026-09-22", {})
    assert "✅" not in line
    assert "⚠️ нет терминальных TI" in line


def test_module_imports_without_airflow():
    code = (
        "import sys, importlib.util\n"
        "sys.modules['airflow'] = None\n"
        f"spec = importlib.util.spec_from_file_location('m', {str(MODULE_PATH)!r})\n"
        "m = importlib.util.module_from_spec(spec)\n"
        "spec.loader.exec_module(m)\n"
        "assert not any(k == 'airflow' or k.startswith('airflow.') "
        "for k, v in sys.modules.items() if v is not None)\n"
        "print(m.RED_SHARE_THRESHOLD_PCT)\n"
    )
    out = subprocess.run(
        [sys.executable, "-c", code], capture_output=True, text=True, check=True
    )
    assert out.stdout.strip() == "20"
