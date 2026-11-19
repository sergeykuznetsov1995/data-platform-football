"""Check the actual morning patch, its CLI row format and its persisted streak."""

import json
import shutil
import subprocess
from datetime import datetime, timezone
from pathlib import Path
from types import ModuleType, SimpleNamespace

import pytest

from scrapers.understat.criterion import DAILY_CRITERION_SQL

ROOT = Path(__file__).resolve().parents[3]


@pytest.fixture
def report(tmp_path):
    target = tmp_path / "morning_report.py"
    shutil.copyfile(ROOT / "tests/fixtures/understat/morning_deadline_legacy.py", target)
    subprocess.run(
        ["patch", "--batch", "--fuzz=0", "-p1", "-i",
         str(ROOT / "deploy/understat/morning-report-2342.patch")],
        cwd=tmp_path, check=True, capture_output=True, text=True,
    )
    module = ModuleType("understat_morning_test")
    exec(compile(target.read_text(), str(target), "exec"), module.__dict__)
    assert module.UNDERSTAT_DAILY_SQL == DAILY_CRITERION_SQL
    module.UNDERSTAT_DAILY_STATE = str(tmp_path / "state.json")
    module.sys = SimpleNamespace(argv=[])
    return module


def run_day(report, day, result_rows):
    class Clock(datetime):
        @classmethod
        def now(cls, tz=None):
            return cls(2026, 10, day + 1, 7, tzinfo=timezone.utc)

    def trino_rows(sql):
        assert f"2026-10-{day:02}" in sql
        return [*result_rows, [report.UNDERSTAT_SENTINEL, "0", "0", "0", "1"]]

    report.datetime = Clock
    report._trino_rows = trino_rows
    return report.understat_daily_criterion()[0]


def test_morning_retries_unmeasured_day_and_caches_corrected_half(report):
    state_file = Path(report.UNDERSTAT_DAILY_STATE)
    healthy = ["M", "1", "1", "0", "1"]
    line = run_day(report, 14, [healthy, ["L", "0", "0", "0", "0"]])
    assert "не измер" in line.lower() or "неизмер" in line.lower()
    assert "%" not in line and "матчей со сроком нет" not in line
    line = run_day(report, 14, [healthy, ["L", "1", "0", "0", "1"]])
    assert "1/2 = 50.0%" in line and "серия 0" in line
    assert json.loads(state_file.read_text())["days"]["2026-10-14"]["due"] == 2
    report._trino_rows = lambda *args: pytest.fail("a measured day should be cached")
    assert report.understat_daily_criterion() == [line]


def test_only_confirmed_neutral_days_carry_the_streak(report):
    healthy = [["M", "1", "1", "0", "1"]]
    assert "серия 1" in run_day(report, 11, healthy)
    assert "матчей со сроком нет" in run_day(report, 12, [])
    assert "серия 2" in run_day(report, 13, healthy)
    line = run_day(report, 14, [["L", "0", "0", "0", "0"]])
    assert "не измер" in line.lower() or "неизмер" in line.lower()
    assert "серия 1" in run_day(report, 15, healthy)


def test_pre_fix_pass_and_neutral_range_are_archived_on_remeasurement(report):
    path = Path(report.UNDERSTAT_DAILY_STATE)
    old = {
        "days": {"2026-10-14": {"due": 1, "ok": 1, "pct": 100.0, "site_late": 0, "streak": 7}},
        "last_day": "2026-10-14", "streak": 7,
        "neutral_since": "2026-10-15", "neutral_through": "2026-10-16",
    }
    path.write_text(json.dumps(old))
    report.sys.argv = ["--dry-run"]
    unknown = [["L", "0", "0", "0", "0"]]
    assert "день не измерен" in run_day(report, 14, unknown)
    assert json.loads(path.read_text()) == old
    report.sys.argv = []
    assert "день не измерен" in run_day(report, 14, unknown)
    state = json.loads(path.read_text())
    assert state["previous_criterion"] == old and "2026-10-14" not in state["days"]
    assert "серия 1" in run_day(report, 15, [["M", "1", "1", "0", "1"]])


def test_failed_query_keeps_old_streak_out_of_active_state(report):
    path = Path(report.UNDERSTAT_DAILY_STATE)
    old = {"days": {}, "last_day": "2026-10-10", "streak": 7}
    path.write_text(json.dumps(old))
    report._trino_rows = lambda sql, **kwargs: None
    assert "день не измерен" in report.understat_daily_criterion()[0]
    state = json.loads(path.read_text())
    assert state["previous_criterion"] == old
    assert "last_day" not in state and not state["days"]
