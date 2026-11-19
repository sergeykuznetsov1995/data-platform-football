"""Exercise the deployed watch query and formatter on synthetic Bronze inputs."""

import json
import shutil
import subprocess
from datetime import datetime, timedelta, timezone
from pathlib import Path
from types import ModuleType, SimpleNamespace

import duckdb
import pytest
import sqlglot

from scrapers.understat.catalog import PRODUCTION_LEAGUES

ROOT = Path(__file__).resolve().parents[3]
NOW = datetime(2026, 11, 19, 15, tzinfo=timezone.utc)
RPL = "RUS-Premier League"
CONTRACT = "understat-bronze-v2"


class Clock(datetime):
    @classmethod
    def now(cls, tz=None):
        return NOW


def plan_result(report, entries):
    def psql(command, **kwargs):
        assert command[:4] == ["docker", "exec", "postgres", "psql"]
        assert "plan_current_scopes" in command[-1]
        return SimpleNamespace(stdout=json.dumps(entries))

    report.subprocess = SimpleNamespace(run=psql, SubprocessError=subprocess.SubprocessError)


@pytest.fixture
def watch(tmp_path):
    path = tmp_path / "morning_report.py"
    shutil.copyfile(ROOT / "tests/fixtures/understat/morning_watch_legacy.py", path)
    subprocess.run(
        ["patch", "--batch", "--fuzz=0", "-p1", "-i", str(ROOT / "deploy/understat/morning-report-2343.patch")],
        cwd=tmp_path, check=True, capture_output=True, text=True,
    )
    report = ModuleType("watch")
    exec(compile(path.read_bytes(), str(path), "exec"), report.__dict__)
    report.datetime = Clock
    plan_result(report, [("current", lg, "2627") for lg in PRODUCTION_LEAGUES])
    with duckdb.connect(":memory:") as db:
        db.execute("ATTACH ':memory:' AS iceberg")
        db.execute("CREATE SCHEMA iceberg.ops")
        db.execute("CREATE SCHEMA iceberg.bronze")
        db.execute("""CREATE TABLE iceberg.ops.understat_ingest_manifest_v1
                  (league VARCHAR, season VARCHAR, status VARCHAR, completed_at VARCHAR, contract_version VARCHAR)""")
        db.execute("""CREATE TABLE iceberg.ops.understat_ingest_failures_v1
                  (league VARCHAR, season VARCHAR, status VARCHAR, completed_at VARCHAR,
                   error_message VARCHAR, contract_version VARCHAR)""")
        db.execute("""CREATE TABLE iceberg.bronze.fotmob_matches_current
                  (competition_id BIGINT, source_season_key VARCHAR, finished BOOLEAN, cancelled BOOLEAN)""")

        def trino_rows(sql):
            return db.execute(sqlglot.transpile(sql, read="trino", write="duckdb")[0]).fetchall()

        report._trino_rows = trino_rows
        yield report, db


def mark(db, league, *, status="complete", age=6, season="2627"):
    stamp = (NOW - timedelta(hours=age)).isoformat()
    db.execute("INSERT INTO iceberg.ops.understat_ingest_manifest_v1 VALUES (?, ?, ?, ?, ?)",
               [league, season, status, stamp, CONTRACT])


def failure(db, league=RPL, *, age=5, season="2627", reason="сбой получения расписания"):
    stamp = (NOW - timedelta(hours=age)).isoformat()
    db.execute("INSERT INTO iceberg.ops.understat_ingest_failures_v1 VALUES (?, ?, 'failure', ?, ?, ?)",
               [league, season, stamp, reason, CONTRACT])


@pytest.mark.parametrize("rus_age", [8 * 24, None, 6], ids=["stale", "missing", "six-fresh"])
def test_six_expected_leagues(watch, rus_age):
    report, db = watch
    for league in PRODUCTION_LEAGUES:
        if league != RPL:
            mark(db, league)
    if rus_age is not None:
        mark(db, RPL, age=rus_age)
    # A recently rechecked closed season must neither cover nor duplicate this pair.
    mark(db, "ENG-Premier League", season="2526")
    failure(db, age=7)
    failure(db, age=1, season="2526", reason="чужой сезон")
    lines = report.understat_watch()
    if rus_age == 6:
        assert lines == ["• итоговые попытки: 6/6 скоупов ✅"]
    else:
        assert "5/6" in lines[0]
        assert all("✅" not in line and "2526" not in line and "чужой сезон" not in line for line in lines)
        assert any(f"{RPL} 2627" in line and "нет итоговой попытки" in line
                   and "сбой получения расписания" in line for line in lines)


def test_empty_manifest_reports_all_six_and_only_known_recent_causes(watch):
    report, db = watch
    failure(db, age=49)
    lines = report.understat_watch()
    assert "0/6" in lines[0]
    assert len(lines) == 7
    for league in PRODUCTION_LEAGUES:
        assert any(league in line and "причина неизвестна" in line for line in lines)
    assert all("скоуп скрыт" not in line and "сбой получения" not in line for line in lines)


def test_plan_counts_next_season_before_first_attempt_and_filters_closed_checks(watch):
    report, db = watch
    plan = [("current", league, "2627") for league in PRODUCTION_LEAGUES]
    plan_result(report, [*plan, *plan, ("current", RPL, "2728"), ("current", RPL, "2526"),
                         ("closed_check", "ENG-Premier League", "2728")])
    for league in PRODUCTION_LEAGUES:
        mark(db, league)
    mark(db, RPL, season="2526")
    failure(db, season="2728", reason="первый сбор")
    lines = report.understat_watch()
    assert "6/7" in lines[0] and len(lines) == 2
    assert f"{RPL} 2728" in lines[1] and "первый сбор" in lines[1]
    assert report._understat_season_closed("2526", datetime(2026, 7, 1).date())
    assert not report._understat_season_closed("2526", datetime(2026, 6, 30).date())


@pytest.mark.parametrize(("status", "age", "expected"), [
    ("complete", 26, "✅"), ("complete", 26 + 1/60, "нет итоговой попытки"),
    ("in_progress", 2, "✅"), ("in_progress", 3, "скоуп скрыт"), ("failure", 1, "скоуп скрыт"),
])
def test_final_window_and_latest_marker(watch, status, age, expected):
    report, db = watch
    for league in PRODUCTION_LEAGUES:
        if league != RPL or status != "complete":
            mark(db, league)
    mark(db, RPL, status=status, age=age)
    lines = report.understat_watch()
    assert any(expected in line for line in lines)
    assert ("6/6" if expected == "✅" else "5/6") in lines[0]


def test_fresh_pending_counts_as_final_and_keeps_its_fotmob_alarm(watch):
    report, db = watch
    for league in PRODUCTION_LEAGUES:
        if league != RPL:
            mark(db, league)
    mark(db, RPL, status="upstream_pending", age=72)
    mark(db, RPL, status="upstream_pending", age=1)
    assert "6/6 скоупов ✅" in report.understat_watch()[0]
    db.execute("INSERT INTO iceberg.bronze.fotmob_matches_current VALUES (63, '2026/2027', TRUE, FALSE)")
    lines = report.understat_watch()
    assert "6/6" in lines[0] and "✅" not in lines[0]
    assert "RUS" in lines[1] and "FotMob видит 1" in lines[1]


@pytest.mark.parametrize("bad_plan", [None, [["current", "unknown", "2627"]], [["current", RPL, "2699"]]])
def test_unavailable_plan_shows_calendar_coverage_with_warning(watch, bad_plan):
    report, db = watch
    plan_result(report, bad_plan)
    for league in PRODUCTION_LEAGUES:
        mark(db, league)
    lines = report.understat_watch()
    assert "6/6" in lines[0] and "✅" not in lines[0]
    assert any("План DAG недоступен" in line for line in lines)


def test_empty_or_closed_only_plan_has_no_daily_denominator(watch):
    report, _ = watch
    plan_result(report, [("closed_check", RPL, "2627")])
    report._trino_rows = lambda sql: pytest.fail("no open scope to check")
    assert report.understat_watch() == ["• итоговые попытки Understat: в плане нет открытых скоупов"]


def test_incomplete_query_response_cannot_shrink_the_plan(watch):
    report, _ = watch
    report._trino_rows = lambda sql: []
    assert "не совпал" in report.understat_watch()[0]


def test_query_error_is_separate_from_missing_manifest(watch):
    report, _ = watch
    report._trino_rows = lambda sql, **kwargs: None
    assert "(trino)" in report.understat_watch()[0]
    report._trino_rows = lambda sql, **kwargs: [] if sql.startswith("SHOW TABLES") else None
    assert "журнал ещё не создан" in report.understat_watch()[0]
