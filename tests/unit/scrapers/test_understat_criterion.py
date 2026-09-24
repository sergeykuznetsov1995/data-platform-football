"""Daily Understat "played -> Bronze within 24 h" meter (#1429)."""

from __future__ import annotations

import json
import re
from decimal import Decimal

import pytest

from scrapers.understat.criterion import (
    COMPLETENESS_SQL,
    DAILY_CRITERION_SQL,
    DayResult,
    pct,
    render_daily_criterion_sql,
    summarize_days,
)

pytestmark = pytest.mark.unit


def test_days_without_deadlines_are_neutral_and_do_not_break_the_streak():
    days = [
        DayResult("2026-10-10", due=0, ok=0),
        DayResult("2026-10-11", due=10, ok=10),
        DayResult("2026-10-12", due=0, ok=0),
        DayResult("2026-10-13", due=8, ok=8),
        DayResult("2026-10-14", due=0, ok=0),
        DayResult("2026-10-15", due=12, ok=12),
        DayResult("2026-10-16", due=0, ok=0),
    ]
    assert summarize_days(days) == 3

    missed = days + [DayResult("2026-10-09", due=10, ok=9)]
    assert summarize_days(missed) == 3
    assert summarize_days(days + [DayResult("2026-10-17", due=10, ok=9)]) == 0


def test_site_delay_is_subtracted_from_due_and_counted_separately():
    # One match the site had not marked played by its deadline: it is not in
    # due, so the day stays 100 % and keeps the streak.
    day = DayResult("2026-09-13", due=28, ok=28, site_late=1)
    assert day.pct == Decimal("100.0")
    assert summarize_days([day]) == 1

    sql = re.sub(r"\s+", " ", DAILY_CRITERION_SQL)
    assert "count_if(NOT site_late) AS due" in sql
    assert "count_if(site_late) AS site_late" in sql
    assert (
        "covered_hits = 0 AND attempts > 0 AND site_hits = 0 AS site_late" in sql
    )


def test_pct_rounds_half_up_like_the_fotmob_line():
    assert pct(397, 400) == Decimal("99.3")
    assert pct(0, 0) is None
    assert DayResult("2026-10-11", due=400, ok=397).pct == Decimal("99.3")


def test_streak_threshold_uses_the_exact_ratio_not_the_rounded_pct():
    near_miss = DayResult("2026-10-11", due=96, ok=95)
    assert near_miss.pct == Decimal("99.0")
    assert summarize_days([near_miss]) == 0
    assert summarize_days([DayResult("2026-10-11", due=100, ok=99)]) == 1


def test_sql_reads_through_the_manifest_fence_with_a_26_hour_deadline():
    daily = render_daily_criterion_sql("2026-09-13")
    for sql in (daily, COMPLETENESS_SQL):
        assert "ORDER BY completed_at DESC, attempt_id DESC" in sql
        assert "m.status = 'complete'" in sql
        assert re.search(r"\w+\._batch_id = m\.batch_id", sql)
        assert "contract_version = 'understat-bronze-v2'" in sql
    assert "kickoff + INTERVAL '26' HOUR" in daily
    assert "TIMESTAMP '2026-09-13 00:00:00'" in daily
    with pytest.raises(ValueError):
        render_daily_criterion_sql("13.09.2026")


# --- the SQL itself on a synthetic manifest (Trino -> DuckDB via sqlglot) ---

MANIFEST_DDL = (
    "CREATE TABLE ops.understat_ingest_manifest_v1 (league varchar, "
    "season varchar, contract_version varchar, batch_id varchar, "
    "attempt_id varchar, status varchar, completed_at varchar, "
    "quality_json varchar)"
)
SCHEDULE_DDL = (
    "CREATE TABLE bronze.understat_schedule (league varchar, season varchar, "
    "game_id bigint, date timestamp, is_result boolean, _batch_id varchar)"
)


def _attempt(attempt_id, batch_id, completed_at, **quality):
    return (
        "L", "2627", "understat-bronze-v2", batch_id, attempt_id, "complete",
        completed_at, json.dumps(quality),
    )


@pytest.fixture()
def synthetic():
    sqlglot = pytest.importorskip("sqlglot")
    duckdb = pytest.importorskip("duckdb")
    con = duckdb.connect(":memory:")
    con.execute("CREATE SCHEMA ops")
    con.execute("CREATE SCHEMA bronze")
    con.execute(MANIFEST_DDL)
    con.execute(SCHEDULE_DDL)
    con.executemany(
        "INSERT INTO ops.understat_ingest_manifest_v1 VALUES (?,?,?,?,?,?,?,?)",
        [
            # Legacy attempt without lists: transitional rule by count.
            _attempt("a1", "b1", "2026-10-11T09:30:00+00:00",
                     completed_game_count=1),
            _attempt("a2", "b2", "2026-10-12T09:30:00+00:00",
                     completed_game_count=5,
                     covered_game_ids=["1", "2", "3"],
                     site_result_game_ids=["1", "2", "3", "4"]),
            _attempt("a3", "b3", "2026-10-13T14:00:00+00:00",
                     completed_game_count=6,
                     covered_game_ids=["1", "2", "3", "4", "5", "6"],
                     site_result_game_ids=["1", "2", "3", "4", "5", "6"]),
        ],
    )
    kickoffs = {
        1: "2026-10-10 15:00",  # deadline 11.10 17:00: legacy a1 covers (cg 1)
        2: "2026-10-10 18:00",  # deadline 11.10 20:00: a1 cg 1 < 2 -> site late
        3: "2026-10-11 15:00",  # deadline 12.10 17:00: a2 covers -> ok
        4: "2026-10-11 16:00",  # site saw it, not covered -> our delay
        5: "2026-10-11 17:00",  # site did not see it, a2 ran -> site late
        6: "2026-10-12 10:00",  # deadline 13.10 12:00: no attempt in window
    }
    rows = [
        ("L", "2627", game_id, kickoff, True, "b3")
        for game_id, kickoff in kickoffs.items()
    ]
    # Outside the manifest fence: an older batch must be invisible.
    rows.append(("L", "2627", 99, "2026-10-10 15:30", True, "b2"))
    con.executemany(
        "INSERT INTO bronze.understat_schedule VALUES (?,?,?,?,?,?)", rows
    )

    def run(day: str) -> DayResult:
        sql = sqlglot.transpile(
            render_daily_criterion_sql(day), read="trino", write="duckdb"
        )[0]
        sql = sql.replace("iceberg.ops.", "ops.").replace(
            "iceberg.bronze.", "bronze."
        )
        rows = con.execute(sql).fetchall()
        assert len(rows) <= 1
        if not rows:
            return DayResult(day, due=0, ok=0)
        _, due, ok, site_late = rows[0]
        return DayResult(day, due=due, ok=ok, site_late=site_late)

    return run


def test_daily_sql_grades_a_synthetic_manifest(synthetic):
    day_11 = synthetic("2026-10-11")
    day_12 = synthetic("2026-10-12")
    day_13 = synthetic("2026-10-13")
    day_14 = synthetic("2026-10-14")

    # g1 on time (legacy count), g2 site late -> subtracted; g99 fenced out.
    assert day_11 == DayResult("2026-10-11", due=1, ok=1, site_late=1)
    # g3 on time (list), g4 ours late, g5 site late.
    assert day_12 == DayResult("2026-10-12", due=2, ok=1, site_late=1)
    # g6 had no attempt between kickoff and deadline: ours, not the site's.
    assert day_13 == DayResult("2026-10-13", due=1, ok=0, site_late=0)
    # No deadlines: neutral.
    assert day_14.due == 0

    assert summarize_days([day_11, day_14]) == 1
    assert summarize_days([day_11, day_12, day_14]) == 0
